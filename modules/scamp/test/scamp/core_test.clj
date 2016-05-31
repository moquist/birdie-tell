(ns scamp.core-test
  (:require [clojure.test :refer :all]
            [scamp.core :as core]
            [schema.core :as s]))

(deftest test-subscribe
  (is (= (-> core/new-world
             (core/add-new-node (core/node-contact-address->node "node-id0"))
             (core/subscribe "node-id1" "node-id0")
             (dissoc :config))
         {:message-envelopes
          '([:message-envelope "node-id0" :forwarded-subscription "node-id1"]),
          :network
          {"node-id1"
           {:self {:id "node-id1"}, :upstream #{}, :downstream #{"node-id0"}},
           "node-id0" {:self {:id "node-id0"}, :upstream #{}, :downstream #{}}}})))

(deftest forward-subscription-test
  (let [result (core/forward-subscription #{"stuffy-node" "stuffier-node"}
                                          "allergen-free-node")]
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
