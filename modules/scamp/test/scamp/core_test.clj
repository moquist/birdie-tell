(ns scamp.core-test
  (:require [clojure.test :refer :all]
            [scamp.core :as core]
            [schema.core :as s]))

(def random-instance (atom (java.util.Random.)))

(defn reset-rand-state! [& seed]
  (let [seed (or seed 43)]
    (.setSeed @random-instance seed)))

(defn- testing-rand*
  ([] (.nextFloat @random-instance))
  ([n] (* n (testing-rand*))))

(defn- purge-envelope-id [envelope]
  (vec (butlast envelope)))

(defn- purge-world-envelope-ids [world]
  (update world :message-envelopes #(mapv purge-envelope-id %)))

(defn scamp-test [f]
  (core/reset-envelope-ids!)
  (reset-rand-state!)
  (f))

(deftest subscribe-new-node-test
  (scamp-test
   #(is (= (-> core/new-world
               (core/add-new-node (core/node-contact-address->node "node-id0"))
               (core/subscribe-new-node "node-id1" "node-id0")
               (dissoc :config)
               purge-world-envelope-ids)
           {:message-envelopes [],
            :network
            {"node-id0"
             {:self {:id "node-id0"},
              :upstream #{"node-id1"},
              :downstream #{},
              :messages-seen {}},
             "node-id1"
             {:self {:id "node-id1"},
              :upstream #{},
              :downstream #{"node-id0"},
              :messages-seen {}}}}))))

(deftest forward-subscription-test
  (scamp-test
   #(binding [scamp.core/*rand* testing-rand*]
      (let [result (->> (core/forward-subscription #{"stuffy-node" "stuffier-node"}
                                                   "allergen-free-node"
                                                   "43")
                        (map purge-envelope-id))]
        (is (= result
               [[:message-envelope "stuffier-node" :forwarded-subscription "allergen-free-node"]]))))))

(defn world-with-subs [subscriptions-count]
  (let [node-name #(str "node-id" %)
        world (core/add-new-node core/new-world
                                 (core/node-contact-address->node (node-name 0)))]
    (loop [world world
           n 0]
      (if (>= n subscriptions-count)
        world
        (let [new-node-id-num (inc n)]
          (recur (core/subscribe-new-node (core/do-all-comms world)
                                          (node-name new-node-id-num)
                                          (node-name n))
                 new-node-id-num))))))

(deftest do-comm-test
  (scamp-test
   #(binding [scamp.core/*rand* testing-rand*]
      (let [world (world-with-subs 3)
            end-world (-> (reduce
                           (fn [world _n] (core/do-comm world))
                           world
                           (range 9))
                          (dissoc :config)
                          purge-world-envelope-ids)]
        (is (= end-world
               {:message-envelopes
                [[:message-envelope "node-id3" :forwarded-subscription "node-id3"]],
                :network
                {"node-id0"
                 {:self {:id "node-id0"},
                  :upstream #{"node-id1"},
                  :downstream #{"node-id2"},
                  :messages-seen {"1" 1, "2" 1, "3" 7, "8" 1}},
                 "node-id1"
                 {:self {:id "node-id1"},
                  :upstream #{"node-id2"},
                  :downstream #{"node-id2" "node-id3" "node-id0"},
                  :messages-seen {"2" 1, "3" 9, "6" 1, "7" 1, "8" 1}},
                 "node-id2"
                 {:self {:id "node-id2"},
                  :upstream #{"node-id3" "node-id0" "node-id1"},
                  :downstream #{"node-id3" "node-id1"},
                  :messages-seen {"4" 1, "2" 1, "3" 10, "5" 1, "7" 1, "8" 1}},
                 "node-id3"
                 {:self {:id "node-id3"},
                  :upstream #{"node-id2" "node-id1"},
                  :downstream #{"node-id2"},
                  :messages-seen {"9" 1, "7" 1, "10" 1}}}}
               ))))))

(deftest notify-add-upstream-test
  (scamp-test
   #(is (= (-> {:self {:id "canticle"} :upstream #{} :downstream #{} :messages-seen {}}
               (core/notify-add-upstream "liebowitz")
               purge-envelope-id)
           [:message-envelope "liebowitz" :add-upstream "canticle"]))))

(deftest handle-add-upstream-test
  (scamp-test
   #(is (= (core/handle-add-upstream (:logging core/default-config)
                                     {:self {:id "canticle"} :upstream #{} :downstream #{} :messages-seen {}}
                                     "liebowitz")
           [{:self {:id "canticle"}, :upstream #{"liebowitz"}, :downstream #{} :messages-seen {}} []]))))

(deftest update-self-test
  (scamp-test
   (fn []
     (let [new-node (core/node-contact-address->node "node-id0")
           non-networked-node (core/node-contact-address->node "non-networked-node")]
       (is (= (-> core/new-world
                  (core/add-new-node new-node)
                  (core/update-self new-node #(update % :upstream conj "flimflam"))
                  (dissoc :config)
                  purge-world-envelope-ids)
              {:message-envelopes []
               :network
               {"node-id0"
                {:self {:id "node-id0"}, :upstream #{"flimflam"}, :downstream #{} :messages-seen {}}}}))))))

(s/defn do-comms :- core/WorldSchema
  "Take 'world and 'n. Process up to 'n messages. Return new 'world.
   If 'world has no :message-envelopes before 'n messages have been
   processed, 'world is returned immediately."
  [world :- core/WorldSchema
   n :- s/Int]
  (if (or (zero? n)
          (empty? (:message-envelopes world)))
    world
    (recur (core/do-comm world) (dec n))))


(comment
(binding [scamp.core/*rand* testing-rand*]
                                                                  (for [comm (range 2 252)]
                                                                       (-> comm-test-world (core/do-comms comm) (dissoc :config) purge-world-envelope-ids)))
  )
