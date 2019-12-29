(ns world.main
  (:require [clojure.string :as str]
            [schema.core :as s]
            [scamp.core :as scamp]
            [scamp.util :as util]))

(def WorldSchema
  {:message-envelopes [scamp/MessageEnvelopeSchema]
   :config scamp/ClusterConfigSchema
   :network {scamp/NodeContactAddressSchema scamp/NetworkedNodeSchema}})

(def world-base
  "Return a new, pristine world."
  {:message-envelopes []
   :config scamp/default-config
   :network {}})

(s/defn world-add-new-node :- WorldSchema
  "Update world to 'turn on' node."
  [world :- WorldSchema
   networked-node :- scamp/NetworkedNodeSchema]
  (assoc-in world [:network (scamp/node->node-contact-address (:self networked-node))]
            networked-node))

(s/defn world-add-messages :- WorldSchema
  "Given 'world and new messages to add, return 'world with the new
  messages added."
  [world :- WorldSchema
   new-messages :- [scamp/MessageEnvelopeSchema]]
  (update-in world [:message-envelopes] util/concatv new-messages))

(s/defn world-subscribe-new-node :- WorldSchema
  "Given 'world, a 'new-node-contact-address for the node that is
  subscribing, and a 'node, add 'new-node-contact-address to 'world
  and inject a :new-subscription message into 'world.

  Return 'world with the new node and a :new-subscription message.

  [1] 2.2.1 Subscription: Algorithm 1"
  [world :- WorldSchema
   new-node-contact-address :- scamp/NodeContactAddressSchema
   contact-node-address :- scamp/NodeContactAddressSchema]
  (let [new-node (scamp/init-new-subscriber (:config world)
                                            new-node-contact-address
                                            contact-node-address)
        new-subscription-message (scamp/msg->envelope contact-node-address
                                                      :new-subscription
                                                      new-node-contact-address)]
    (-> world
        (world-add-new-node new-node)
        (world-add-messages [new-subscription-message]))))

(s/defn world-instruct-node-to-unsubscribe :- WorldSchema
  "This is a mechanism to make unsubscription happen in this toy
  implementation by inserting a message into 'world from outside the
  'world.

  Given 'world and the id of a node that is unsubscribing, add a
  :node-unsubscription message to and return 'world."
  [world :- WorldSchema
   node-to-unsubscribe :- scamp/NodeContactAddressSchema]
  (world-add-messages world [(scamp/msg->envelope node-to-unsubscribe
                                                 :node-unsubscription
                                                 node-to-unsubscribe)]))

(s/defn world-do-async-processing :- WorldSchema
  [world :- WorldSchema]
  (let [comm-updates (mapcat (partial scamp/node-do-async-processing (:config world)) (-> world :network vals))]
    #_(println :t (*milli-time*) :comm-updates comm-updates)
    world))

(s/defn world-do-comm :- WorldSchema
  "Take 'world. Process one message. Return new 'world."
  [{:keys [config] :as world} :- WorldSchema
   & [options :- {s/Keyword s/Any}]]
  (if (-> world :message-envelopes empty?)
    world
    (let [[[_ destination-node-id & message] & message-envelopes] (:message-envelopes world)
          destination-node (get-in world [:network destination-node-id])
          [new-destination-node new-message-envelopes] (scamp/read-mail config
                                                                        destination-node
                                                                        message)]
      (when (:verbose? options)
        (println "message to " destination-node-id ": " message))
      (doseq [[_ {:keys [clock] :as node}] (:network world)]
        ;; This is a dirty hack to make time move forward, somehow.
        (when clock (scamp/tick-clock-millis! clock))
        )
      (world-do-async-processing world)
      (-> world
          (assoc :message-envelopes (util/concatv message-envelopes new-message-envelopes))
          (assoc-in [:network destination-node-id] new-destination-node)))))

(s/defn world-do-all-comms :- WorldSchema
  "Take 'world. Process communication until no unread messages remain.
   Return world."
  ([world :- WorldSchema] (world-do-all-comms world {}))
  ([world :- WorldSchema
    options :- {s/Keyword s/Any}]
   (if (-> world :message-envelopes empty?)
     world
     (recur (world-do-comm world options) options))))

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

