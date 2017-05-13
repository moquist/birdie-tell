(ns scamp.core
  (:require [clojure.string :as str]
            [taoensso.timbre :as timbre]
            [schema.core :as s]))

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
(defn- concatv [coll & colls]
  (vec (apply concat coll colls)))

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

(def NetworkedNodeSchema
  {:self NodeCoreSchema
   :upstream NodeNeighborsSchema
   :downstream NodeNeighborsSchema
   :messages-seen {MessageEnvelopeIdSchema s/Int}
   (s/optional-key :removed?) s/Bool})

(def MessageTypesSchema
  (s/enum :forwarded-subscription
          :new-upstream-node
          :node-unsubscription ;; currently only for self-unsubscription
          :node-replacement
          :node-removal
          :new-subscription
          ))

(def MessageEnvelopeSchema
  [(s/one (s/eq :message-envelope) "envelope variant type")
   (s/one NodeContactAddressSchema "envelope destination")
   (s/one MessageTypesSchema "envelope message type")
   (s/one s/Any "envelope body")
   (s/one MessageEnvelopeIdSchema "envelope id")])

(def SubscriptionSchema
  NodeContactAddressSchema)

(def WorldConfigSchema
  {:connection-redundancy s/Int
   :message-dup-drop-after s/Int
   :logging {s/Keyword s/Any}})

(def WorldSchema
  {:message-envelopes [MessageEnvelopeSchema]
   :config WorldConfigSchema
   :network {NodeContactAddressSchema NetworkedNodeSchema}})

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Defaults and samples
(def default-config
  (s/validate
   WorldConfigSchema
   {;; :c :connection-redundancy
    :connection-redundancy 2 ;; Emperically determined by [1]
    :message-dup-drop-after 10 ;; Empirically determined by [1]
    :logging (assoc timbre/*config*
                    :level :debug)
    })
  )

(def sample-node
  {:self {:id "node-id5"
          ;; :host "127.0.0.1"
          ;; :port 2005
          }

   ;; :partial-view :downstream
   ;; :local-view :downstream
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

(def sample-world
  {:message-envelopes []
   :config default-config
   :network {"node-id0" {:self {:id "node-id0"}
                         :downstream #{}
                         :upstream #{}
                         :messages-seen {}}
             "node-id1" sample-node}})

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
  "Take a node-contact-address, return a networked-node structure."
  [node-contact-address :- NodeContactAddressSchema]
  {:self {:id node-contact-address}
   :upstream #{}
   :downstream #{}
   :messages-seen {}})

(s/defn node->removed :- NetworkedNodeSchema
  [node :- NetworkedNodeSchema]
  (-> (get-in node [:self :id])
      node-contact-address->node
      (assoc :removed? true)))

(s/defn init-new-subscriber :- NetworkedNodeSchema
  "Take a 'subscriber-address for a new subscriber, and the
  'contact-node handling the subscription, and return an initialized
  networked-node for the new subscriber."
  [subscriber-address :- NodeContactAddressSchema
   contact-node :- NodeContactAddressSchema]
  (-> subscriber-address
      node-contact-address->node
      (assoc-in [:downstream] #{contact-node})))

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

(s/defn receive-msg-new-subscription :- CommUpdateSchema
  "Forward the 'subscription to every :downstream node, duplicating
  the forwarded 'subscription to :connection-redundancy :downstream
  nodes. If :downstream is empty, do nothing."
  [logging-config
   {:keys [downstream] :as node} :- NetworkedNodeSchema
   subscription :- SubscriptionSchema
   connection-redundancy :- s/Int]
  (timbre/log* logging-config :trace
               :receive-msg-new-subscription
               :node node
               :subscription subscription
               :connection-redundancy connection-redundancy)

  (let [self-id (get-in node [:self :id])]
    (when (= self-id subscription)
      (throw (ex-info (str "Got own new subscription: " subscription) {:node node})))

    (let [node (update node :upstream maybe-conj self-id subscription)
          new-messages (if (empty? downstream)
                         ;; There is no 'downstream. No messages are needed, because the
                         ;; 'subscription node will already have 'node in its
                         ;; :downstream.
                         []

                         ;; Forward subscription to :downstream.
                         (let [downstream (seq downstream)
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
      (rand-nth* (seq downstream))
      :forwarded-subscription
      subscription
      envelope-id]]))

(s/defn receive-msg-new-upstream-node :- CommUpdateSchema
  "Take a node and a new 'upstream-node-contact-address.
   Add the 'upstream-node-contact-address to :upstream.

   Return this updated 'node and an empty messages vector, to match
   CommUpdateSchema."
  [logging-config
   node :- NetworkedNodeSchema
   upstream-node-contact-address :- NodeContactAddressSchema]
  (timbre/log* logging-config :trace
               :receive-msg-new-upstream-node
               :node node
               :upstream-node-contact-address upstream-node-contact-address)
  [(update node :upstream maybe-conj (get-in node [:self :id]) upstream-node-contact-address)
   []])

(s/defn send-msg-new-upstream-node :- MessageEnvelopeSchema
  "Take a new 'upstream-node-contact-address and a 'node, and return
  an :new-upstream-node message envelope for
  'upstream-node-contact-address."
  [upstream-node-contact-address :- NodeContactAddressSchema
   node :- NetworkedNodeSchema]
  [:message-envelope
   upstream-node-contact-address
   :new-upstream-node
   (get-in node [:self :id])
   (*get-envelope-id*)])

(s/defn receive-msg-forwarded-subscription :- CommUpdateSchema
  "Take 'logging-config, a node, a new subscription, and an
  'envelope-id. Either accept the subscription into :downstream, or
  forward it to a :downstream node.

  Return a vector matching 'CommUpdateSchema.

  [1] 2.2.1 Subscription: Algorithm 2"
  [logging-config
   {:keys [downstream] :as node} :- NetworkedNodeSchema
   subscriber-contact-address :- NodeContactAddressSchema
   envelope-id :- MessageEnvelopeIdSchema]
  (timbre/log* logging-config :trace
               :receive-msg-forwarded-subscription
               :node node
               :subscriber-contact-address subscriber-contact-address)
  (let [self-id (get-in node [:self :id])]
    (if (and (not= self-id subscriber-contact-address)
             (not (downstream subscriber-contact-address))
             ;; The subscription id is not for this node itself, and
             ;; the subscription id is not already in node's downstream,
             ;; so check the probability that we add it to this node.
             (do-probability (subscription-acceptance-probability downstream)))
      [(update-in node [:downstream] maybe-conj self-id subscriber-contact-address)
       [(send-msg-new-upstream-node subscriber-contact-address node)]]

      (let [forwarded-subscription-messages (forward-subscription downstream
                                                                  subscriber-contact-address
                                                                  envelope-id)]
        (when (empty? forwarded-subscription-messages)
          (throw (ex-info "Failed either to accept or forward subscription"
                          {:node node :subscriber-contact-address subscriber-contact-address})))
        [node forwarded-subscription-messages]))))

(def new-world
  "Return a new, pristine world."
  {:message-envelopes []
   :config default-config
   :network {}})

(s/defn add-new-node :- WorldSchema
  "Update world to 'turn on' node."
  [world :- WorldSchema
   networked-node :- NetworkedNodeSchema]
  (assoc-in world [:network (node->node-contact-address (:self networked-node))]
            networked-node))

(s/defn add-messages :- WorldSchema
  "Given 'world and new messages to add, return 'world with the new
  messages added."
  [world :- WorldSchema
   new-messages :- [MessageEnvelopeSchema]]
  (update-in world [:message-envelopes] concatv new-messages))

(s/defn update-self :- WorldSchema
  "Given world and 'self, update self with 'f and 'args."
  [world :- WorldSchema
   self :- NetworkedNodeSchema
   f & args]
  (let [self-id (get-in self [:self :id])]
    (update-in world
               [:network self-id]
               #(apply f % args))))

(s/defn get-node-from-world :- NetworkedNodeSchema
  [world :- WorldSchema
   node-contact-address :- NodeContactAddressSchema]
  (get-in world [:network node-contact-address]))

(s/defn subscribe-new-node :- WorldSchema
  "Given 'world, a 'new-node-contact-address for the node that is
  subscribing, and a 'node, add 'new-node-contact-address to 'world
  and inject a :new-subscription message into 'world.

  Return 'world with the new node and a :new-subscription message.

  [1] 2.2.1 Subscription: Algorithm 1"
  [world :- WorldSchema
   new-node-contact-address :- NodeContactAddressSchema
   contact-node-address :- NodeContactAddressSchema]
  (let [new-node (init-new-subscriber new-node-contact-address
                                      contact-node-address)
        new-subscription-message (msg->envelope contact-node-address
                                                :new-subscription
                                                new-node-contact-address)]
    (-> world
        (add-new-node new-node)
        (add-messages [new-subscription-message]))))

(s/defn instruct-node-to-unsubscribe :- WorldSchema
  "This is a mechanism to make unsubscription happen in this toy
  implementation by inserting a message into 'world from outside the
  'world.

  Given 'world and the id of a node that is unsubscribing, add a
  :node-unsubscription message to and return 'world."
  [world :- WorldSchema
   node-to-unsubscribe :- NodeContactAddressSchema]
  (add-messages world [(msg->envelope node-to-unsubscribe
                                      :node-unsubscription
                                      node-to-unsubscribe)]))

(s/defn send-msg-node-replacement :- MessageEnvelopeSchema
  [recipient-node-contact-address :- NodeContactAddressSchema
   old-node-contact-address :- NodeContactAddressSchema
   new-node-contact-address :- NodeContactAddressSchema]
  (msg->envelope recipient-node-contact-address
                 :node-replacement
                 {:old old-node-contact-address
                  :new new-node-contact-address}))

(s/defn send-msg-node-removal :- MessageEnvelopeSchema
  [recipient-node-contact-address :- NodeContactAddressSchema
   removal-node-contact-address :- NodeContactAddressSchema]
  [:message-envelope
   recipient-node-contact-address
   :node-removal
   removal-node-contact-address
   (*get-envelope-id*)])

(s/defn receive-msg-node-unsubscription :- CommUpdateSchema
  "Take 'logging-config, a node, and the ID of a node that is unsubscribing.

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
  [logging-config
   node :- NetworkedNodeSchema
   unsubscribing-node-id :- NodeContactAddressSchema
   connection-redundancy :- s/Int]

  (timbre/log* logging-config :trace
               :receive-msg-node-unsubscription
               :node node
               :unsubscribing-node-id unsubscribing-node-id
               :connection-redundancy connection-redundancy)

  (let [get-node-id #(get-in % [:self :id])
        node-id (get-node-id node)]

    (when (not= node-id unsubscribing-node-id)
      ;; TODO: Be more resiliant.
      (throw (ex-info "Received :node-unsubscription for other node."
                      {:unsubscribing-node-id unsubscribing-node-id})))

    (let [upstream-nodes (-> node :upstream sort)
          downstream-nodes (->> node :downstream sort)
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
                     [(send-msg-node-replacement upstream-node-contact-address
                                                 node-id
                                                 new-downstream-node-contact-address)
                      (send-msg-node-replacement new-downstream-node-contact-address
                                                 node-id
                                                 upstream-node-contact-address)])
            new-connections))

          ;; TODO: filter out dups
          drop-me-msgs (doall (map #(send-msg-node-removal % node-id) droppers))]

      (timbre/log* logging-config :trace
               :receive-msg-node-unsubscription
               :replacement-messages replacement-messages
               :drop-me-msgs drop-me-msgs)
      [(node->removed node) (concatv replacement-messages drop-me-msgs)])))


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
      (condp = message-type
        ;; TODO: refactor into 'receive-msg multimethod
        :new-upstream-node (receive-msg-new-upstream-node logging
                                                          destination-node
                                                          message-body)
        :forwarded-subscription (receive-msg-forwarded-subscription logging
                                                                    destination-node
                                                                    message-body
                                                                    envelope-id)
        :node-unsubscription (receive-msg-node-unsubscription logging
                                                              destination-node
                                                              message-body
                                                              connection-redundancy)
        :new-subscription (receive-msg-new-subscription logging
                                                        destination-node
                                                        message-body
                                                        connection-redundancy)
        (timbre/log* logging :error
                     :read-mail-unknown-message-type
                     :destination-node destination-node
                     :message-type message-type
                     :message-body message-body))
      (do
        (timbre/log* logging :trace
                     :read-mail-dropping-dup
                     :destination-node destination-node
                     :envelope-id envelope-id)
        [destination-node []]))))

(s/defn do-comm :- WorldSchema
  "Take 'world. Process one message. Return new 'world."
  [{:keys [config] :as world} :- WorldSchema]
  (if (-> world :message-envelopes empty?)
    world
    (let [[[_ destination-node-id & message] & message-envelopes] (:message-envelopes world)
          destination-node (get-in world [:network destination-node-id])
          [new-destination-node new-message-envelopes] (read-mail config
                                                                  destination-node
                                                                  message)]
      (-> world
          (assoc :message-envelopes (concatv message-envelopes new-message-envelopes))
          (assoc-in [:network destination-node-id] new-destination-node)))))

(s/defn do-all-comms :- WorldSchema
  "Take 'world. Process communication until no unread messages remain.
   Return world."
  [world :- WorldSchema]
  (if (-> world :message-envelopes empty?)
    world
    (recur (do-comm world))))

(s/defn world->dot
  [world :- WorldSchema]
  (let [node-id->int #(str/replace % #"node-id" "")]
    (str "digraph {"
         (->> (:network world)
              (reduce (fn [r [self {:keys [downstream]}]]
                        (apply str r (map #(str (node-id->int self)
                                                " -> "
                                                (node-id->int %)
                                                ";")
                                          downstream)))
                      ""))
         "}")))

(comment
  [1] "Peer-to-Peer Membership Management for Gossip-Based Protocols"

  (spit "/tmp/wemp.dot" (world->dot world))
  ;; dot -Tpng -O /tmp/wemp.dot
  ;; circo -Tpng -O /tmp/wemp.dot

  (binding [scamp.core/*rand* testing-rand*]
    (reset-rand-state!)
    (let [world (-> (world-with-subs 243)
                    core/do-all-comms
                    (core/instruct-node-to-unsubscribe "node-id9")
                    core/do-all-comms)]
      (clojure.pprint/pprint world)))

  )
