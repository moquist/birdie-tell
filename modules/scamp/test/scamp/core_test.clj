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

(deftest test-subscribe
  (is (= (-> core/new-world
             (core/add-new-node (core/node-contact-address->node "node-id0"))
             (core/subscribe "node-id1" "node-id0")
             (dissoc :config)
             purge-world-envelope-ids)
         {:message-envelopes
          '([:message-envelope
             "node-id0"
             :forwarded-subscription
             "node-id1"]),
          :network
          {"node-id1"
           {:self {:id "node-id1"}, :upstream #{}, :downstream #{"node-id0"}},
           "node-id0" {:self {:id "node-id0"}, :upstream #{}, :downstream #{}}}})))

(deftest forward-subscription-test
  (let [result (->> (core/forward-subscription #{"stuffy-node" "stuffier-node"}
                                               "allergen-free-node")
                    (map purge-envelope-id))]
    (is (#{[[:message-envelope "stuffy-node" :forwarded-subscription "allergen-free-node"]]
           [[:message-envelope "stuffier-node" :forwarded-subscription "allergen-free-node"]]}
         result))))

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
