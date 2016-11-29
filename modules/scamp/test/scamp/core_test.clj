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

(deftest do-comm-test
  (is (= (-> core/new-world
             (core/add-new-node (core/node-contact-address->node "node-id0"))
             (core/subscribe "node-id1" "node-id0")
             core/do-comm
             (dissoc :config))
         {:message-envelopes (),
          :network
          {"node-id1"
           {:self {:id "node-id1"}, :upstream #{}, :downstream #{"node-id0"}},
           "node-id0"
           {:self {:id "node-id0"}, :upstream #{}, :downstream #{"node-id1"}}}})))
