(ns scamp.core-test
  (:require [clojure.test :refer :all]
            [scamp.core :as core]
            [schema.core :as s]))

(defn- testing-rand*
  ([] 0.4)
  ([n] (* n (testing-rand*))))

(defn- purge-envelope-id [envelope]
  (vec (butlast envelope)))

(defn- purge-world-envelope-ids [world]
  (update world :message-envelopes #(mapv purge-envelope-id %)))

(deftest subscribe-new-node-test
  (is (= (-> core/new-world
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
            :messages-seen {}}}})))

(deftest forward-subscription-test
  (binding [scamp.core/*rand* testing-rand*]
    (let [result (->> (core/forward-subscription #{"stuffy-node" "stuffier-node"}
                                                 "allergen-free-node"
                                                 "43")
                      (map purge-envelope-id))]
      (is (= result
           [[:message-envelope "stuffy-node" :forwarded-subscription "allergen-free-node"]])))))

(def comm-test-world
  (-> core/new-world
      (core/add-new-node (core/node-contact-address->node "node-id0"))
      (core/subscribe-new-node "node-id1" "node-id0")
      (core/subscribe-new-node "node-id2" "node-id1")
      (core/subscribe-new-node "node-id3" "node-id2")))

(deftest do-comm-test
  (println "TODO: improve do-comm-test (randomization, elegance)")
  (binding [scamp.core/*rand* testing-rand*]
    (let [world comm-test-world
          world0 (-> world
                     (dissoc :config)
                     purge-world-envelope-ids)
          world1 (-> world
                     core/do-comm
                     (dissoc :config)
                     purge-world-envelope-ids)
          world2 (-> world
                     (core/do-comms 2)
                     (dissoc :config)
                     purge-world-envelope-ids)

          world3 (-> world
                     (core/do-comms 3)
                     (dissoc :config)
                     purge-world-envelope-ids)
          world4 (-> world
                     (core/do-comms 4)
                     (dissoc :config)
                     purge-world-envelope-ids)
          world5 (-> world
                     (core/do-comms 5)
                     (dissoc :config)
                     purge-world-envelope-ids)
          world6 (-> world
                     (core/do-comms 6)
                     (dissoc :config)
                     purge-world-envelope-ids)
          world7 (-> world
                     (core/do-comms 7)
                     (dissoc :config)
                     purge-world-envelope-ids)
          world8 (-> world
                     (core/do-comms 8)
                     (dissoc :config)
                     purge-world-envelope-ids)
          ]
      (is (= world0 {:message-envelopes
                     [[:message-envelope "node-id0" :forwarded-subscription "node-id2"]
                      [:message-envelope "node-id0" :forwarded-subscription "node-id2"]
                      [:message-envelope "node-id0" :forwarded-subscription "node-id2"]
                      [:message-envelope "node-id1" :forwarded-subscription "node-id3"]
                      [:message-envelope "node-id1" :forwarded-subscription "node-id3"]
                      [:message-envelope "node-id1" :forwarded-subscription "node-id3"]],
                     :network
                     {"node-id0"
                      {:self {:id "node-id0"}, :upstream #{"node-id1"}, :downstream #{} :messages-seen {}},
                      "node-id1"
                      {:self {:id "node-id1"},
                       :upstream #{"node-id2"},
                       :downstream #{"node-id0"}
                       :messages-seen {}},
                      "node-id2"
                      {:self {:id "node-id2"},
                       :upstream #{"node-id3"},
                       :downstream #{"node-id1"}
                       :messages-seen {}},
                      "node-id3"
                      {:self {:id "node-id3"}, :upstream #{}, :downstream #{"node-id2"} :messages-seen {}}}}))

      (is (= world1
             {:message-envelopes
              [[:message-envelope "node-id0" :forwarded-subscription "node-id2"]
               [:message-envelope "node-id0" :forwarded-subscription "node-id2"]
               [:message-envelope "node-id1" :forwarded-subscription "node-id3"]
               [:message-envelope "node-id1" :forwarded-subscription "node-id3"]
               [:message-envelope "node-id1" :forwarded-subscription "node-id3"]
               [:message-envelope "node-id2" :add-upstream "node-id0"]],
              :network
              {"node-id0"
               {:self {:id "node-id0"},
                :upstream #{"node-id1"},
                :downstream #{"node-id2"}
                :messages-seen {"1" 1}},
               "node-id1"
               {:self {:id "node-id1"},
                :upstream #{"node-id2"},
                :downstream #{"node-id0"}
                :messages-seen {}},
               "node-id2"
               {:self {:id "node-id2"},
                :upstream #{"node-id3"},
                :downstream #{"node-id1"}
                :messages-seen {}},
               "node-id3"
               {:self {:id "node-id3"}, :upstream #{}, :downstream #{"node-id2"}
                :messages-seen {}}}}))

      (is (= world2
             {:message-envelopes
              [[:message-envelope "node-id0" :forwarded-subscription "node-id2"]
               [:message-envelope "node-id1" :forwarded-subscription "node-id3"]
               [:message-envelope "node-id1" :forwarded-subscription "node-id3"]
               [:message-envelope "node-id1" :forwarded-subscription "node-id3"]
               [:message-envelope "node-id2" :add-upstream "node-id0"]
               [:message-envelope "node-id2" :forwarded-subscription "node-id2"]],
              :network
              {"node-id0"
               {:self {:id "node-id0"},
                :upstream #{"node-id1"},
                :downstream #{"node-id2"}
                :messages-seen {"1" 1 "2" 1}},
               "node-id1"
               {:self {:id "node-id1"},
                :upstream #{"node-id2"},
                :downstream #{"node-id0"}
                :messages-seen {}},
               "node-id2"
               {:self {:id "node-id2"},
                :upstream #{"node-id3"},
                :downstream #{"node-id1"}
                :messages-seen {}},
               "node-id3"
               {:self {:id "node-id3"}, :upstream #{}, :downstream #{"node-id2"}
                :messages-seen {}}}}))

      (is (= world3
             {:message-envelopes
              [[:message-envelope "node-id1" :forwarded-subscription "node-id3"]
               [:message-envelope "node-id1" :forwarded-subscription "node-id3"]
               [:message-envelope "node-id1" :forwarded-subscription "node-id3"]
               [:message-envelope "node-id2" :add-upstream "node-id0"]
               [:message-envelope "node-id2" :forwarded-subscription "node-id2"]
               [:message-envelope "node-id2" :forwarded-subscription "node-id2"]],
              :network
              {"node-id0"
               {:self {:id "node-id0"},
                :upstream #{"node-id1"},
                :downstream #{"node-id2"}
                :messages-seen {"1" 1 "2" 1 "3" 1}},
               "node-id1"
               {:self {:id "node-id1"},
                :upstream #{"node-id2"},
                :downstream #{"node-id0"}
                :messages-seen {}},
               "node-id2"
               {:self {:id "node-id2"},
                :upstream #{"node-id3"},
                :downstream #{"node-id1"}
                :messages-seen {}},
               "node-id3"
               {:self {:id "node-id3"}, :upstream #{}, :downstream #{"node-id2"}
                :messages-seen {}}}}))

      (is (= world4
             {:message-envelopes
              [[:message-envelope "node-id1" :forwarded-subscription "node-id3"]
               [:message-envelope "node-id1" :forwarded-subscription "node-id3"]
               [:message-envelope "node-id2" :add-upstream "node-id0"]
               [:message-envelope "node-id2" :forwarded-subscription "node-id2"]
               [:message-envelope "node-id2" :forwarded-subscription "node-id2"]
               [:message-envelope "node-id3" :add-upstream "node-id1"]],
              :network
              {"node-id0"
               {:self {:id "node-id0"},
                :upstream #{"node-id1"},
                :downstream #{"node-id2"}
                :messages-seen {"1" 1 "2" 1 "3" 1}},
               "node-id1"
               {:self {:id "node-id1"},
                :upstream #{"node-id2"},
                :downstream #{"node-id3" "node-id0"}
                :messages-seen {"4" 1}},
               "node-id2"
               {:self {:id "node-id2"},
                :upstream #{"node-id3"},
                :downstream #{"node-id1"}
                :messages-seen {}},
               "node-id3"
               {:self {:id "node-id3"}, :upstream #{}, :downstream #{"node-id2"}
                :messages-seen {}}}}))

      (is (= world5
             {:message-envelopes
              [[:message-envelope "node-id1" :forwarded-subscription "node-id3"]
               [:message-envelope "node-id2" :add-upstream "node-id0"]
               [:message-envelope "node-id2" :forwarded-subscription "node-id2"]
               [:message-envelope "node-id2" :forwarded-subscription "node-id2"]
               [:message-envelope "node-id3" :add-upstream "node-id1"]
               [:message-envelope "node-id3" :forwarded-subscription "node-id3"]],
              :network
              {"node-id0"
               {:self {:id "node-id0"},
                :upstream #{"node-id1"},
                :downstream #{"node-id2"}
                :messages-seen {"1" 1 "2" 1 "3" 1}},
               "node-id1"
               {:self {:id "node-id1"},
                :upstream #{"node-id2"},
                :downstream #{"node-id3" "node-id0"}
                :messages-seen {"4" 1 "5" 1}},
               "node-id2"
               {:self {:id "node-id2"},
                :upstream #{"node-id3"},
                :downstream #{"node-id1"}
                :messages-seen {}},
               "node-id3"
               {:self {:id "node-id3"}, :upstream #{}, :downstream #{"node-id2"}
                :messages-seen {}}}}))

      (is (= world6
             {:message-envelopes
              [[:message-envelope "node-id2" :add-upstream "node-id0"]
               [:message-envelope "node-id2" :forwarded-subscription "node-id2"]
               [:message-envelope "node-id2" :forwarded-subscription "node-id2"]
               [:message-envelope "node-id3" :add-upstream "node-id1"]
               [:message-envelope "node-id3" :forwarded-subscription "node-id3"]
               [:message-envelope "node-id3" :forwarded-subscription "node-id3"]],
              :network
              {"node-id0"
               {:self {:id "node-id0"},
                :upstream #{"node-id1"},
                :downstream #{"node-id2"}
                :messages-seen {"1" 1 "2" 1 "3" 1}},
               "node-id1"
               {:self {:id "node-id1"},
                :upstream #{"node-id2"},
                :downstream #{"node-id3" "node-id0"}
                :messages-seen {"4" 1 "5" 1 "6" 1}},
               "node-id2"
               {:self {:id "node-id2"},
                :upstream #{"node-id3"},
                :downstream #{"node-id1"}
                :messages-seen {}},
               "node-id3"
               {:self {:id "node-id3"}, :upstream #{}, :downstream #{"node-id2"}
                :messages-seen {}}}}))


      (clojure.pprint/pprint world7)

      )))

(deftest notify-add-upstream-test
  (is (= (-> {:self {:id "canticle"} :upstream #{} :downstream #{} :messages-seen {}}
             (core/notify-add-upstream "liebowitz")
             purge-envelope-id)
         [:message-envelope "liebowitz" :add-upstream "canticle"])))

(deftest handle-add-upstream-test
  (is (= (core/handle-add-upstream (:logging core/default-config)
                                   {:self {:id "canticle"} :upstream #{} :downstream #{} :messages-seen {}}
                                   "liebowitz")
         [{:self {:id "canticle"}, :upstream #{"liebowitz"}, :downstream #{} :messages-seen {}} []])))

(deftest update-self-test
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
             {:self {:id "node-id0"}, :upstream #{"flimflam"}, :downstream #{} :messages-seen {}}}}))))

(comment
(binding [scamp.core/*rand* testing-rand*]
                                                                  (for [comm (range 2 252)]
                                                                       (-> comm-test-world (core/do-comms comm) (dissoc :config) purge-world-envelope-ids)))
  )
