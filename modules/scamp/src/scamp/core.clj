(ns scamp.core)

(def default-config
  {:c :connection-redundancy
   :connection-redundancy 2 ;; Emperically determined by [1]
   }
  )

(def sample-node
  {:self {:id "node-id5"
          :host "127.0.0.1"
          :port 2005}

   :partial-view :downstream
   :local-view :downstream
   :downstream {"node-id1" {:id "node-id1"
                            :host "127.0.0.1"
                            :port 2001}
                "node-id2" {:id "node-id2"
                            :host "127.0.0.1"
                            :port 2002}}

   :in-view :upstream
   :upstream {"node-id3" {:id "node-id3"
                          :host "127.0.0.1"
                          :port 2003}
              "node-id4" {:id "node-id4"
                          :host "127.0.0.1"
                          :port 2004}}})

(def sample-subscription-request
  {:id "node-id6"
   :host "127.0.0.1"
   :port 2006})

(def sample-network
  {"node-id0" {:self {:id "node-id0"}
               :downstream {"node-id0" {:id "node-id0"}}
               :upstream {}}
   "node-id1" sample-node})

(def sample-world
  {:messages []
   :network sample-network})

(defn subscription-acceptance-probability
  "Determine the probability that a node will accept a subscription
  request (for a node not already present in 'downstream), given the
  count of 'downstream."
  [downstream]
  0.9)

(defn handle-new-subscription
  "Forward the 'subscription to every :downstream node, duplicating
  the forwarded 'subscription to :connection-redundancy :downstream
  nodes."
  [config state subscription]
  (let [downstream (-> state :downstream keys)
        downstream+ (reduce (fn [x _] (conj x (rand-nth downstream)))
                            downstream
                            (range (:connection-redundancy config)))]
    (map
     (fn [node-id]
       [:message node-id :forwarded-subscription subscription])
     downstream+)))

(def new-world
  "Return a new, pristine world."
  {:messages []
   :config default-config
   :network {}})

(defn init-cluster [world node-core]
  (-> world
      (add-new-node node-core)
      (assoc-in [:network (:id node-core) :downstream (:id node-core)]
                node-core)
      (assoc-in [:network (:id node-core) :upstream (:id node-core)]
                node-core)))

(defn add-new-node
  "Update world to 'turn on' node."
  [world node-core]
  (assoc-in world [:network (:id node-core)]
            {:self node-core
             :upstream {}
             :downstream {}}))

(defn subscribe
  "Given 'world and a 'node-core (the :self part of a member node),
  which represents a subscription-request, and a 'contact-id, forward
  subscription requests from 'contact-id."
  [world node-core contact-id]
  (let [messages (handle-new-subscription
                         (:config world)
                         (get-in world [:network contact-id])
                         node-core)]
    (-> world
        (add-new-node node-core)
        (update-in [:messages] concat messages))))


(comment
  [1] "Peer-to-Peer Membership Management for Gossip-Based Protocols")
