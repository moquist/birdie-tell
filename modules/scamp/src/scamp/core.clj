(ns scamp.core
  "\"He was a scamp; he is a hero!\" - Les Mis, p. 587

  Implement Gossip cluster membership protocol defined in \"Peer-to-Peer Membership Management
  for Gossip-Based Protocols\".
  SCAMP = SCAlable Membership Protocol
  https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/ieee_tocs.pdf

  Paper sections TODO:
    * DONE: subscription (§2.2.1, Algorithm 1)
    * DONE: subscription (§2.2.1, Algorithm 2)
    * DONE: unsubscription (§2.2.2)
    * DONE: heartbeats (§2.2.3) (This is the first async feature,
                                 because it depends on the concept of
                                 \"a long time\".)
    * subscription indirection (§3.1)
    * lease mechanism (§3.2)
  "
  (:require [clojure.string :as str]
            [taoensso.timbre :as timbre]
            [schema.core :as s]
            [scamp.util :as util]))

(s/set-fn-validation! true)

(def envelope-id-counter (atom 0))

(defn ^:dynamic *get-envelope-id*
  "TODO: replace with UUIDs"
  []
  (str (swap! envelope-id-counter inc)))

(defn reset-envelope-ids! []
  (reset! envelope-id-counter 0))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Custom randomization (for predictable testing)
(def ^:dynamic *rand* rand)

(defn- ^Integer rand-int* [n]
  (int (*rand* n)))

(defn- rand-nth* [coll]
  (nth coll (rand-int* (count coll))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn tick-clock-millis! [clock-atom & [num-ticks]]
  (swap! clock-atom + (or num-ticks 1)))

(def ^:dynamic *milli-time* deref)

(defn jitter-int
  "Given a 'base-duration in some numerical unit of time, and an
  'agitation-distance the jitter can cover from that 'base-duration,
  compute a jittered duration and return it as an integer."
  [base-duration agitation-distance]
  (let [agitation (- (*rand* agitation-distance) (/ agitation-distance 2))]
    (int (+ base-duration agitation))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Schema definitions
(def NodeContactAddressSchema
  s/Str)

(def NodeCoreSchema
  {:id NodeContactAddressSchema})

(def NodeNeighborsSchema
  ;; TODO: decouple contact address and node ID.
  #{NodeContactAddressSchema})

(def MessageEnvelopeIdSchema s/Str)

(def SelflessNodeSchema
  {:upstream NodeNeighborsSchema
   :downstream NodeNeighborsSchema
   :messages-seen {MessageEnvelopeIdSchema s/Int}
   (s/optional-key :removed?) s/Bool
   :send-next-heartbeats-milli-time (s/maybe s/Int)
   :heartbeat-timeout-milli-time (s/maybe s/Int)
   :clock (s/maybe (s/atom s/Int))
   })

(def SelflessNodeSchemaOptional
  "Just like SelflessNodeSchema, but every key is optional.
  This is used for schema checking of init-value maps."
  (->> SelflessNodeSchema
       (map
        (fn [[k v]]
          (if (instance? schema.core.OptionalKey k)
            [k v]
            [(s/optional-key k) v])))
       (into {})))

(def NetworkedNodeSchema
  (merge SelflessNodeSchema
         {:self NodeCoreSchema}))


(def MessageTypesSchema
  (s/enum
   ;; first [re]contact: [re]subscribe a new node
   :new-subscription

   ;; new node joining cluster, recipient will probabilistically add
   ;; this new node to :downstream, else forward it again
   :forwarded-subscription

   ;; inform newly :downstream node that you are now in its :upstream
   :new-upstream-node

   ;; Not in SCAMP: added to make it possible to instruct a node to
   ;; unsubscribe.
   :node-unsubscription

   ;; instruct an upstream node and a downstream node to connect
   ;; directly, without me in the middle
   :node-replacement

   ;; alternative to :node-replacement, used when upstream/downstream
   ;; don't get a replacement
   :node-removal

   ;; just for saying "Hi" and "please don't assume you've been
   ;; partitioned from the rest of the cluster"
   :heartbeat

   ;; more stuff that isn't here yet...
   ))

(def MessageEnvelopeSchema
  [(s/one (s/eq :message-envelope) "envelope variant type")
   (s/one NodeContactAddressSchema "envelope destination")
   (s/one MessageTypesSchema "envelope message type")
   (s/one s/Any "envelope body")
   (s/one MessageEnvelopeIdSchema "envelope id")])

(def SubscriptionSchema
  NodeContactAddressSchema)

(def ClusterConfigSchema
  {:connection-redundancy s/Int
   :message-dup-drop-after s/Int
   :logging {s/Keyword s/Any}
   :heartbeat-interval-in-millis s/Int
   :heartbeat-timeout-in-millis s/Int
   :node-clock-init-fn s/Any ; TODO: FIXME s/Fn isn't a thing
   })

(def CommUpdateSchema
  [(s/one (s/maybe NetworkedNodeSchema) "networked node (recipient of processed message)")
   ;; zereo or more new messages
   [MessageEnvelopeSchema]])

(def ProbabilitySchema
  (s/pred #(<= 0 % 1)))

"
TODO:
  * distinguish better between node contact info (network address, map entry in (:network world)) and node itself
"

(defn atomic-clock [& initial-time-in-millis]
  (atom (or initial-time-in-millis 0)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Defaults and samples
(def default-config
  (s/validate
   ClusterConfigSchema
   {;; :c :connection-redundancy
    :connection-redundancy 2 ;; Emperically determined by [1]
    :message-dup-drop-after 10 ;; Empirically determined by [1]
    :logging (assoc timbre/*config*
                    :level :debug)
    :heartbeat-interval-in-millis (* 30 1000)
    :heartbeat-timeout-in-millis (* 45 1000)
    :node-clock-init-fn atomic-clock
    })
  )

(def sample-node
  {:self {:id "node-id5"
          ;; :host "127.0.0.1"
          ;; :port 2005
          }

   ;; :partial-view from [1] = :downstream
   ;; :local-view from [1] = :downstream
   :downstream #{"node-id1" ; node-contact-address for node-id1
                 "node-id2" ; node-contact-address for node-id2
                 }

   ;; :in-view :upstream
   :upstream #{"node-id3" ; node-contact-address for node-id3
               "node-id4" ; node-contact-address for node-id4
               }
   :messages-seen {}})

(def sample-subscription-request
  {:new-node-contact-address "node-id6"
   :contact-address "node-id1" ; clustered node-contact-address
   })

(s/defn node->node-contact-address :- NodeContactAddressSchema
  "Take a 'node, and return the contact address for the node.

  E.g., a node contact address in a simulation 'world might just be
  the node's name, whereas a node contact address in a TCP/IP gossip
  cluster might be a map with an IP address and a port number."
  [node :- NodeCoreSchema]
  {:pre [(map? node)]
   ;; For now, node-contact-addresses are strings that can be used to
   ;; get nodes from 'world:
   ;; (get-in world [:network node-contact-address)
   :post [(string? %)]}
  (:id node))

(s/defn networked-node->node-contact-address :- NodeContactAddressSchema
  [node :- NetworkedNodeSchema]
  (-> node
      :self
      node->node-contact-address))

(s/defn node-contact-address->node :- NetworkedNodeSchema
  "Take a node-contact-address and optional 'init-vals;
  return a networked-node structure."
  [node-contact-address :- NodeContactAddressSchema
   & [init-vals :- SelflessNodeSchemaOptional]]
  (merge {:self {:id node-contact-address}
          :upstream #{}
          :downstream #{}
          :messages-seen {}
          :send-next-heartbeats-milli-time nil
          :heartbeat-timeout-milli-time nil
          :clock nil}
         init-vals))

(s/defn node->removed :- NetworkedNodeSchema
  [node :- NetworkedNodeSchema]
  (-> node
      networked-node->node-contact-address
      node-contact-address->node
      (assoc :removed? true)))

(s/defn init-node-async :- NetworkedNodeSchema
  "Take a node, initialize async-related settings"
  [{:keys [heartbeat-interval-in-millis
           heartbeat-timeout-in-millis
           node-clock-init-fn] :as cluster-config} :- ClusterConfigSchema
   {:keys [clock] :as node} :- NetworkedNodeSchema]
  (let [clock (if clock clock (node-clock-init-fn))
        milli-time (*milli-time* clock)]
    (assoc node
           :clock clock
           :send-next-heartbeats-milli-time (+ milli-time heartbeat-interval-in-millis)
           :heartbeat-timeout-milli-time (+ milli-time heartbeat-timeout-in-millis))))

(s/defn init-new-subscriber :- NetworkedNodeSchema
  "Take a 'subscriber-address for a new subscriber, and the
  'contact-node handling the subscription, and return an initialized
  networked-node for the new subscriber."
  [cluster-config :- ClusterConfigSchema
   subscriber-address :- NodeContactAddressSchema
   contact-node :- NodeContactAddressSchema]
  (let [init-fn (partial init-node-async cluster-config)]
    (-> subscriber-address
        node-contact-address->node
        init-fn
        (assoc :downstream #{contact-node}))))

(s/defn subscription-acceptance-probability :- ProbabilitySchema
  "Determine the probability that a node will accept a subscription
  request (for a node not already present in 'downstream), given the
  count of 'downstream."
  [downstream :- NodeNeighborsSchema]
  (/ 1 (+ 1 (count downstream))))

(s/defn msg->envelope :- MessageEnvelopeSchema
  [destination-node-contact-address :- NodeContactAddressSchema
   msg-type :- MessageTypesSchema
   msg-body]
  [:message-envelope
   destination-node-contact-address
   msg-type
   msg-body
   (*get-envelope-id*)])

(s/defn maybe-conj
  "'conj 'new-node-contact-address into 'coll as long as
  'this-node-contact-address and 'new-node-contact-address are
  different."
  [coll :- #{NodeContactAddressSchema}
   this-node-contact-address :- NodeContactAddressSchema
   new-node-contact-address :- NodeContactAddressSchema]
  (if (= this-node-contact-address new-node-contact-address)
    coll
    (conj coll new-node-contact-address)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; receive-msg
(defmulti receive-msg
  (fn [msg-type & _] msg-type))

(defmethod receive-msg :default
  [& args]
  (let [[_message-type {:keys [logging]} & _rest] args]
    (when logging
      ;; TODO: If an unknown message type is received, gracefully shut
      ;; down to allow restart with an upgrade?
      (timbre/log* logging :error
                   :receive-msg-unknown-message-type
                   :args args))
    (throw (ex-info "Error: unknown message type received by 'receive-msg." {:args args}))))

(s/defmethod receive-msg :new-subscription :- CommUpdateSchema
  #_"Add 'subscription to :upstream.
     Then Forward the 'subscription to every :downstream node,
     duplicating the forwarded 'subscription to :connection-redundancy
     :downstream nodes. If :downstream is empty, do no forwarding.

  2.2.1 Subscription: Algorithm 1"
  [_message-type :- MessageTypesSchema
   {:keys [logging connection-redundancy] :as cluster-config}
   {:keys [downstream] :as node} :- NetworkedNodeSchema
   subscription :- SubscriptionSchema
   _envelope-id :- MessageEnvelopeIdSchema]
  (timbre/log* logging :trace
               :receive-msg-new-subscription
               :node node
               :subscription subscription
               :connection-redundancy connection-redundancy)

  (let [self-id (networked-node->node-contact-address node)]
    (when (= self-id subscription)
      (throw (ex-info (str "Got own new subscription: " subscription) {:node node})))

    (let [node (update node :upstream maybe-conj self-id subscription)
          new-messages (if (empty? downstream)
                         ;; There is no 'downstream. No messages are needed, because the
                         ;; 'subscription node will already have 'node in its
                         ;; :downstream.
                         []

                         ;; Forward subscription to :downstream.
                         (let [downstream (sort downstream)
                               downstream+ (reduce (fn [x _] (conj x (rand-nth* downstream)))
                                                   downstream
                                                   (range connection-redundancy))]
                           (mapv
                            (fn [node-id]
                              (msg->envelope node-id
                                             :forwarded-subscription
                                             subscription))
                            downstream+)))]
      [node new-messages])))

(s/defn do-probability :- s/Bool
  "'rand is inclusive of 0 and exclusive of 1, so our test should not have '=."
  [cutoff :- ProbabilitySchema]
  (< (*rand*) cutoff))

(s/defn forward-subscription :- [MessageEnvelopeSchema]
  "Take a node's 'downstream map, a new 'subscription, and the
  'envelope-id.

  Return a seq of message-envelopes that communicate the forwarded
  subscription."
  [downstream :- NodeNeighborsSchema
   subscription :- SubscriptionSchema
   envelope-id :- MessageEnvelopeIdSchema]
  {:post [(vector? %)]}
  (if (empty? downstream)
    []
    [[:message-envelope
      (rand-nth* (sort downstream))
      :forwarded-subscription
      subscription
      envelope-id]]))

(s/defn node-do-async-processing :- CommUpdateSchema
  "Do all node-specific async processing, such as sending heartbeats."
  [{:keys [heartbeat-interval-in-millis heartbeat-timeout-in-millis] :as config} :- ClusterConfigSchema
   {:keys [send-next-heartbeats-milli-time
           heartbeat-timeout-milli-time
           downstream clock]
    :as node} :- NetworkedNodeSchema]
  (let [milli-time (*milli-time* clock)
        this-node-contact-address (networked-node->node-contact-address node)
        heartbeat-timeout-fn (fn [[node msgs]]
                               [(assoc node
                                       :heartbeat-timeout-milli-time nil
                                       ;; TODO: ditch :upstream, keep :downstream?
                                       :upstream #{})
                                (conj msgs (msg->envelope (rand-nth* (sort downstream))
                                                          :new-subscription
                                                          this-node-contact-address))])
        send-next-heartbeats-fn (fn [[node msgs]]
                                  [(assoc node
                                          :send-next-heartbeats-milli-time
                                          (+ milli-time heartbeat-interval-in-millis))
                                   (util/concatv msgs
                                           (mapv #(msg->envelope %
                                                                 :heartbeat
                                                                 this-node-contact-address)
                                                 downstream))])]
    (cond-> [node []]
      (and heartbeat-timeout-milli-time (<= heartbeat-timeout-milli-time milli-time))
      heartbeat-timeout-fn

      (and send-next-heartbeats-milli-time (<= send-next-heartbeats-milli-time milli-time))
      send-next-heartbeats-fn)))

(s/defmethod receive-msg :new-upstream-node :- CommUpdateSchema
  #_"Take a node and a new 'upstream-node-contact-address.
   Add the 'upstream-node-contact-address to :upstream.

   Return this updated 'node and an empty messages vector, to match
   CommUpdateSchema."
  [_message-type :- MessageTypesSchema
   {:keys [logging]}
   node :- NetworkedNodeSchema
   upstream-node-contact-address :- NodeContactAddressSchema
   _envelope-id :- MessageEnvelopeIdSchema]
  (timbre/log* logging :trace
               :receive-msg-new-upstream-node
               :node node
               :upstream-node-contact-address upstream-node-contact-address)
  [(update node :upstream
           maybe-conj
           (networked-node->node-contact-address node)
           upstream-node-contact-address)
   []])

(s/defmethod receive-msg :forwarded-subscription :- CommUpdateSchema
  #_"Take 'config, a node, a new subscription, and an
  'envelope-id. Either accept the subscription into :downstream, or
  forward it to a :downstream node.

  Return a vector matching 'CommUpdateSchema.

  [1] 2.2.1 Subscription: Algorithm 2"
  [_message-type :- MessageTypesSchema
   {:keys [logging]}
   {:keys [downstream] :as node} :- NetworkedNodeSchema
   subscriber-contact-address :- NodeContactAddressSchema
   envelope-id :- MessageEnvelopeIdSchema]
  (timbre/log* logging :trace
               :receive-msg-forwarded-subscription
               :node node
               :subscriber-contact-address subscriber-contact-address)
  (let [self-id (networked-node->node-contact-address node)]
    (if (and (not= self-id subscriber-contact-address)
             (not (downstream subscriber-contact-address))
             ;; The subscription id is not for this node itself, and
             ;; the subscription id is not already in node's downstream,
             ;; so check the probability that we add it to this node.
             (do-probability (subscription-acceptance-probability downstream)))
      [(update-in node [:downstream] maybe-conj self-id subscriber-contact-address)
       [(msg->envelope subscriber-contact-address
                       :new-upstream-node
                       (networked-node->node-contact-address node))]]

      (let [forwarded-subscription-messages (forward-subscription downstream
                                                                  subscriber-contact-address
                                                                  envelope-id)]
        (when (empty? forwarded-subscription-messages)
          (throw (ex-info "Failed either to accept or forward subscription"
                          {:node node :subscriber-contact-address subscriber-contact-address})))
        [node forwarded-subscription-messages]))))

(s/defmethod receive-msg :node-unsubscription :- CommUpdateSchema
  #_"Take 'config, a node, and the ID of a node that is unsubscribing.

   If the unsubscribing node is not this one throw an exception.

   If the unsubscribing node is this one, notify :upstream and
   :downstream nodes accordingly. [1] 2.2.2 Unsubscriptions
   1. If an upstream link to this node is being replaced with a link
      to one of this node's downstream nodes (removing this node as a
      link in that chain), then both the upstream and downstream nodes
      must be informed of each other in order to remove this node from
      between them.
   2. If an upstream link to this node is being dropped, then the
      upstream node must be informed.
   3. Any downstream nodes that do not receive new upstream nodes must
      be informed that this node is being dropped.

   FYI: This function returns new messages, some of which are pairs of
   messages to inform upstream and downstream nodes of changes. It's
   possible that one of a pair of messages may fail to be delivered,
   or be mishandled on the receiving node. These messages are not
   handled transactionally, and there are no delivery or handling
   guarantees or verifications. SCAMP includes a subscription leasing
   mechanism that will correct any dangling upstream or downstream
   pointers.

   Prose Restatement of 2.2.2 Unsubscriptions:
   Remove self from the middle of connections between upstream nodes
   and downstream nodes, by informing *most* upstream nodes of
   downstream nodes directly. Leave out (inc :connection-redundancy)
   upstream nodes when making these replacement connections; these
   upstream nodes do not receive replacement downstream
   connections. If there are still more upstream nodes than downstream
   nodes, just wrap around the downstream list and keep making
   replacement connections.
   "
  [_message-type :- MessageTypesSchema
   {:keys [logging connection-redundancy]}
   node :- NetworkedNodeSchema
   unsubscribing-node-id :- NodeContactAddressSchema
   _envelope-id :- MessageEnvelopeIdSchema]

  (timbre/log* logging :trace
               :receive-msg-node-unsubscription
               :node node
               :unsubscribing-node-id unsubscribing-node-id
               :connection-redundancy connection-redundancy)

  (let [node-id (networked-node->node-contact-address node)]

    (when (not= node-id unsubscribing-node-id)
      ;; TODO: Be more resiliant.
      (throw (ex-info "Received :node-unsubscription for other node."
                      {:unsubscribing-node-id unsubscribing-node-id})))

    (let [rand-comparator (fn [& x] (*rand*))
          upstream-nodes (sort-by identity rand-comparator (:upstream node))
          downstream-nodes (sort-by identity rand-comparator (:downstream node))
          num-upstream-to-drop (inc connection-redundancy)

          upstream-replacements (drop num-upstream-to-drop upstream-nodes)
          ;; upstream nodes we're intentionally not reconnecting
          upstream-droppers (take num-upstream-to-drop upstream-nodes)

          downstream-droppers (if (> (count upstream-replacements)
                                     (count downstream-nodes))
                                ;; If there are more upstream nodes to
                                ;; receive replacements than there are
                                ;; downstream nodes to be reconnected,
                                ;; then we'll use all the downstream
                                ;; nodes.
                                []
                                ;; Else, there are more downstream nodes
                                ;; than upstream nodes, so some of the
                                ;; downstream nodes will be unused and
                                ;; references to this node should just
                                ;; be removed without replacement.
                                (drop (count upstream-replacements) downstream-nodes))

          droppers (into (set upstream-droppers) downstream-droppers)

          new-connections (remove #(apply = %) ; Filter out self-connections.
                                  (zipmap upstream-replacements (cycle downstream-nodes)))

          replacement-messages
          (doall
           (mapcat (fn [[upstream-node-contact-address
                         new-downstream-node-contact-address]]

                     [(msg->envelope upstream-node-contact-address
                                     :node-replacement
                                     {:old node-id :new new-downstream-node-contact-address})
                      (msg->envelope new-downstream-node-contact-address
                                     :node-replacement
                                     {:old node-id :new upstream-node-contact-address})])
            new-connections))

          drop-me-msgs (doall (map #(msg->envelope % :node-removal node-id) droppers))]

      (timbre/log* logging :trace
               :receive-msg-node-unsubscription
               :replacement-messages replacement-messages
               :drop-me-msgs drop-me-msgs)
      [(node->removed node) (util/concatv replacement-messages drop-me-msgs)])))

(s/defmethod receive-msg :node-removal :- CommUpdateSchema
  #_"Take 'config, a 'node, and the contact address of a
  'node-to-remove. Remove 'node-to-remove from :upstream and
  :downstream.

  Return a vector matching 'CommUpdateSchema."
  [_message-type :- MessageTypesSchema
   {:keys [logging]}
   {:keys [upstream downstream] :as node} :- NetworkedNodeSchema
   node-to-remove :- NodeContactAddressSchema
   _envelope-id :- MessageEnvelopeIdSchema]
  (timbre/log* logging :trace
               :receive-msg-node-removal
               :node node
               :node-to-remove node-to-remove)
  [(-> node
       (update-in [:upstream] util/set-disclude node-to-remove)
       (update-in [:downstream] util/set-disclude node-to-remove))
   []])

(s/defmethod receive-msg :node-replacement :- CommUpdateSchema
  #_ "Take 'config, a 'node, and a message body consisting of an :old
   node-contact-address and a :new node-contact-address.
   Replace all 'node's :upstream and :downstream references to
   :old with :new.

   It's OK to replace both :upstream and :downstream indiscriminately,
   because the same replacement needs to be done in both.

   Return updated 'node."
  [_message-type :- MessageTypesSchema
   {:keys [logging] :as _cluster-config}
   {:keys [upstream downstream] :as node} :- NetworkedNodeSchema
   {:keys [old new] :as _body} :- s/Any
   _envelope-id :- MessageEnvelopeIdSchema]
  (timbre/log* logging :trace
               :receive-msg-node-replacement
               :body _body
               :node node)
  (let [node (assoc node
                    :upstream (util/set-swap upstream old new)
                    :downstream (util/set-swap downstream old new))]
    [node []]))

(s/defmethod receive-msg :heartbeat :- CommUpdateSchema
  #_"Take 'config and a 'node, and update :heartbeat-timeout-milli-time.

  Return a vector matching 'CommUpdateSchema."
  [_message-type :- MessageTypesSchema
   {:keys [logging heartbeat-timeout-in-millis]}
   {:keys [clock] :as node} :- NetworkedNodeSchema
   _body :- s/Any
   _envelope-id :- MessageEnvelopeIdSchema]
  (timbre/log* logging :trace
               :receive-msg-heartbeat
               :node node)
  (let [milli-time (*milli-time* clock)]
    [(assoc node :heartbeat-timeout-milli-time (+ milli-time heartbeat-timeout-in-millis))
     []]))

(s/defn read-mail :- CommUpdateSchema
  "Take a 'destination-node and a message.

  Process the message for 'destination-node and return a vector
  matching 'CommUpdateSchema.

  If this envelope has been seen too many times by 'destination-node
  (as configured by :message-dup-drop-after in 'config), then ignore
  the message."
  [{:keys [logging message-dup-drop-after connection-redundancy] :as config}
   destination-node :- NetworkedNodeSchema
   [message-type message-body envelope-id]]
  {:post [(vector? %) (-> % first map?) (-> % second vector?)]}
  (timbre/log* logging :trace
               :read-mail
               :destination-node destination-node
               :message-type message-type
               :message-body message-body
               :envelope-id envelope-id)
  (let [destination-node (update-in destination-node
                                    [:messages-seen envelope-id]
                                    #(if % (inc %) 1))]
    (if (< (get-in destination-node [:messages-seen envelope-id])
           message-dup-drop-after)
      (receive-msg message-type config destination-node message-body envelope-id)
      (do
        (timbre/log* logging :trace
                     :read-mail-dropping-dup
                     :destination-node destination-node
                     :envelope-id envelope-id)
        [destination-node []]))))

(comment
  [1] "Peer-to-Peer Membership Management for Gossip-Based Protocols"

  (spit "/tmp/wemp.dot" (world->dot world))
  ;; dot -Tpng -O /tmp/wemp.dot
  ;; circo -Tpng -O /tmp/wemp.dot

  )
