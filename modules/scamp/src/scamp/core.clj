(ns scamp.core
  (:require [taoensso.timbre :as timbre]
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
   :messages-seen {MessageEnvelopeIdSchema s/Int}})

(def MessageTypesSchema
  (s/enum :forwarded-subscription
          :new-upstream-node
          :node-unsubscription ;; currently only for self-unsubscription
          :node-replacement
          :node-removal
          ))

(def SubscriptionSchema
  NodeContactAddressSchema)

(def MessageEnvelopeSchema
  [(s/one (s/eq :message-envelope) "envelope variant type")
   (s/one NodeContactAddressSchema "envelope destination")
   (s/one MessageTypesSchema "envelope message type")
   (s/one s/Any "envelope body")
   (s/one MessageEnvelopeIdSchema "envelope id")])

(def WorldConfigSchema
  {:connection-redundancy s/Int
   :message-dup-drop-after s/Int
   :logging {s/Keyword s/Any}})

(def WorldSchema
  {:message-envelopes [MessageEnvelopeSchema]
   :config WorldConfigSchema
   :network {NodeContactAddressSchema NetworkedNodeSchema}})

(def CommUpdateSchema
  [(s/one NetworkedNodeSchema "networked node (recipient of processed message)")
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
  {;; :c :connection-redundancy
   :connection-redundancy 2 ;; Emperically determined by [1]
   :message-dup-drop-after 10 ;; Empirically determined by [1]
   :logging (assoc timbre/*config*
                   :level :debug)
   }
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

(s/defn receive-msg-new-subscription :- [MessageEnvelopeSchema]
  "Forward the 'subscription to every :downstream node, duplicating
  the forwarded 'subscription to :connection-redundancy :downstream
  nodes. If :downstream is empty, do nothing."
  [config :- WorldConfigSchema
   {:keys [downstream] :as node} :- NetworkedNodeSchema
   subscription :- SubscriptionSchema]
  (if (empty? downstream)
    ;; There is no 'downstream. Do nothing, because the 'subscription
    ;; node will already have 'node in its :downstream.
    []

    ;; Forward subscription to :downstream.
    (let [downstream (seq downstream)
          downstream+ (reduce (fn [x _] (conj x (rand-nth* downstream)))
                              downstream
                              (range (:connection-redundancy config)))]
      (map
       (fn [node-id]
         [:message-envelope
          node-id
          :forwarded-subscription
          subscription
          (*get-envelope-id*)])
       downstream+))))

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
  [(update node :upstream conj upstream-node-contact-address)
   []])

(s/defn send-msg-new-upstream-node :- MessageEnvelopeSchema
  "Take a new 'upstream-node-contact-address and a 'node, and return
  an :new-upstream-node message envelope for
  'upstream-node-contact-address."
  [node :- NetworkedNodeSchema
   upstream-node-contact-address :- NodeContactAddressSchema]
  [:message-envelope
   upstream-node-contact-address
   :new-upstream-node
   (get-in node [:self :id])
   (*get-envelope-id*)])

(s/defn receive-msg-forwarded-subscription :- CommUpdateSchema
  "Take 'logging-config, a node, a new subscription, and an
  'envelope-id. Either accept the subscription into :downstream, or
  forward it to a :downstream node.

  Return a vector matching 'CommUpdateSchema."
  [logging-config
   {:keys [downstream] :as node} :- NetworkedNodeSchema
   subscriber-contact-address :- NodeContactAddressSchema
   envelope-id :- MessageEnvelopeIdSchema]
  (timbre/log* logging-config :trace
               :receive-msg-forwarded-subscription
               :node node
               :subscriber-contact-address subscriber-contact-address)
  (if (and (not= (get-in node [:self :id]) subscriber-contact-address)
           (not (downstream subscriber-contact-address))
           ;; The subscription id is not for this node itself, and
           ;; the subscription id is not already in node's downstream,
           ;; so check the probability that we add it to this node.
           (do-probability (subscription-acceptance-probability downstream)))
    [(update-in node [:downstream] conj subscriber-contact-address)
     [(send-msg-new-upstream-node node subscriber-contact-address)]]

    (let [forwarded-subscription-messages (forward-subscription downstream
                                                                subscriber-contact-address
                                                                envelope-id)]
      (when (empty? forwarded-subscription-messages)
        (throw (ex-info "Failed either to accept or forward subscription"
                        {:node node :subscriber-contact-address subscriber-contact-address})))
      [node forwarded-subscription-messages])))

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
  subscribing, and a 'node, forward subscription requests
  from 'node.

  Return 'world with new messages."
  [world :- WorldSchema
   new-node-contact-address :- NodeContactAddressSchema
   contact-node-address :- NodeContactAddressSchema]
  (let [contact-node (get-node-from-world world contact-node-address)
        new-node (init-new-subscriber new-node-contact-address
                                      contact-node-address)
        new-messages (receive-msg-new-subscription
                      (:config world)
                      contact-node
                      new-node-contact-address)]
    (-> world
        (update-self contact-node #(update % :upstream conj new-node-contact-address))
        (add-new-node new-node)
        (add-messages new-messages))))

(s/defn read-mail :- CommUpdateSchema
  "Take a 'destination-node and a message.

  Process the message for 'destination-node and return a vector
  matching 'CommUpdateSchema.

  If this envelope has been seen too many times by 'destination-node
  (as configured by :message-dup-drop-after in 'config), then ignore
  the message."
  [{:keys [logging message-dup-drop-after] :as config}
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
        :new-upstream-node (receive-msg-new-upstream-node logging destination-node message-body)
        :forwarded-subscription (receive-msg-forwarded-subscription logging
                                                               destination-node
                                                               message-body
                                                               envelope-id)
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

(comment
  [1] "Peer-to-Peer Membership Management for Gossip-Based Protocols"

  )
