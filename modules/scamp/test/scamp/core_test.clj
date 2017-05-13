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
           {:message-envelopes
            [[:message-envelope "node-id0" :new-subscription "node-id1"]],
            :network
            {"node-id0"
             {:self {:id "node-id0"},
              :upstream #{},
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
               [[:message-envelope "stuffy-node" :forwarded-subscription "allergen-free-node"]]))))))

(defn world-with-subs [subscriptions-count]
  (let [node-name #(str "node-id" %)
        world (core/add-new-node core/new-world
                                 (core/node-contact-address->node (node-name 0)))]
    (loop [world world
           n 0]
      (if (>= n subscriptions-count)
        world
        (let [new-node-id-num (inc n)]
          (recur (core/do-all-comms
                  (core/subscribe-new-node (core/do-all-comms world)
                                           (node-name new-node-id-num)
                                           (node-name n)))
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
               {:message-envelopes [],
                :network
                {"node-id0"
                 {:self {:id "node-id0"},
                  :upstream #{"node-id1"},
                  :downstream #{"node-id2" "node-id3"},
                  :messages-seen {"1" 1, "3" 1, "4" 1, "5" 5, "9" 1}},
                 "node-id1"
                 {:self {:id "node-id1"},
                  :upstream #{"node-id2"},
                  :downstream #{"node-id2" "node-id3" "node-id0"},
                  :messages-seen {"2" 1, "4" 1, "5" 9, "9" 2, "10" 1, "11" 5}},
                 "node-id2"
                 {:self {:id "node-id2"},
                  :upstream #{"node-id3" "node-id0" "node-id1"},
                  :downstream #{"node-id3" "node-id1"},
                  :messages-seen
                  {"6" 1, "4" 1, "5" 10, "7" 1, "8" 1, "9" 1, "11" 5}},
                 "node-id3"
                 {:self {:id "node-id3"},
                  :upstream #{"node-id2" "node-id0" "node-id1"},
                  :downstream #{"node-id2"},
                  :messages-seen {"12" 1, "13" 1, "11" 3, "14" 1}}}}
               ))))))

(deftest send-msg-new-upstream-node-test
  (scamp-test
   #(is (= (->> {:self {:id "canticle"} :upstream #{} :downstream #{} :messages-seen {}}
                (core/send-msg-new-upstream-node "liebowitz")
                purge-envelope-id)
           [:message-envelope "liebowitz" :new-upstream-node "canticle"]))))

(deftest receive-msg-new-subscription-test
  (scamp-test
   #(binding [scamp.core/*rand* testing-rand*]
      (is (= (core/receive-msg :new-subscription
                               core/default-config
                               {:self {:id "Auri"}
                                :upstream #{"Kvothe" "Mandrag"}
                                :downstream #{"Fulcrum" "Scaperling" "Foxen" "Underthing" "Mantle" "Withy" "Crumbledon" "Cricklet" "The Twelve"}
                                :messages-seen {}}
                               "Ninewise"
                               "43")
             [{:self {:id "Auri"},
               :upstream #{"Mandrag" "Kvothe" "Ninewise"},
               :downstream
               #{"Fulcrum" "Crumbledon" "Mantle" "Cricklet" "Foxen" "The Twelve"
                 "Underthing" "Withy" "Scaperling"},
               :messages-seen {}}
              [[:message-envelope "Foxen" :forwarded-subscription "Ninewise" "1"]
               [:message-envelope
                "The Twelve"
                :forwarded-subscription
                "Ninewise"
                "2"]
               [:message-envelope
                "Cricklet"
                :forwarded-subscription
                "Ninewise"
                "3"]
               [:message-envelope
                "Crumbledon"
                :forwarded-subscription
                "Ninewise"
                "4"]
               [:message-envelope "Foxen" :forwarded-subscription "Ninewise" "5"]
               [:message-envelope "Fulcrum" :forwarded-subscription "Ninewise" "6"]
               [:message-envelope "Mantle" :forwarded-subscription "Ninewise" "7"]
               [:message-envelope
                "Scaperling"
                :forwarded-subscription
                "Ninewise"
                "8"]
               [:message-envelope
                "The Twelve"
                :forwarded-subscription
                "Ninewise"
                "9"]
               [:message-envelope
                "Underthing"
                :forwarded-subscription
                "Ninewise"
                "10"]
               [:message-envelope
                "Withy"
                :forwarded-subscription
                "Ninewise"
                "11"]]]
             )))))

(deftest receive-msg-new-upstream-node-test
  (scamp-test
   #(is (= (core/receive-msg :new-upstream-node
                             core/default-config
                             {:self {:id "canticle"}
                              :upstream #{}
                              :downstream #{}
                              :messages-seen {}}
                             "liebowitz"
                             "43")
           [{:self {:id "canticle"},
             :upstream #{"liebowitz"},
             :downstream #{}
             :messages-seen {}} []]))))

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

(deftest receive-msg-node-unsubscription-test
  (scamp-test
   #(binding [scamp.core/*rand* testing-rand*]
      (let [world (-> (world-with-subs 43)
                      core/do-all-comms
                      (core/instruct-node-to-unsubscribe "node-id19")
                      core/do-comm)]
        (is (= (:message-envelopes world)
               [[:message-envelope
                 "node-id20"
                 :node-replacement
                 {:old "node-id19", :new "node-id18"}
                 "286"]
                [:message-envelope
                 "node-id18"
                 :node-replacement
                 {:old "node-id19", :new "node-id20"}
                 "287"]
                [:message-envelope "node-id17" :node-removal "node-id19" "288"]
                [:message-envelope "node-id21" :node-removal "node-id19" "289"]
                [:message-envelope "node-id20" :node-removal "node-id19" "290"]
                [:message-envelope "node-id18" :node-removal "node-id19" "291"]
                [:message-envelope "node-id16" :node-removal "node-id19" "292"]
                [:message-envelope "node-id23" :node-removal "node-id19" "293"]]
               ))))))



(comment
  (binding [scamp.core/*rand* testing-rand*]
    (for [comm (range 2 252)]
      (-> comm-test-world (core/do-comms comm) (dissoc :config) purge-world-envelope-ids)))

  (binding [scamp.core/*rand* testing-rand*]
    (let [world comm-test-world
          result (loop [world world messages-count 0]
                   (if (-> world :message-envelopes empty?)
                     {:world world :messages-count messages-count}
                     (recur (core/do-comm world) (inc messages-count))))
          end-world (-> result
                        :world
                        (dissoc :config)
                        purge-world-envelope-ids)]
      (clojure.pprint/pprint {:end-world end-world
                              :messages-count (:messages-count result)})
      ))

  )
