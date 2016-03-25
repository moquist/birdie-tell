(ns scamp.core-test
  (:require [clojure.test :refer :all]
            [scamp.core :as core]))

(deftest test-subscribe
  (is (= (-> core/new-world
             (core/init-cluster {:id "node-id0"})
             (core/subscribe {:id "node-id1"} "node-id0"))
         {:messages
          '([:message "node-id0" :forwarded-subscription {:id "node-id1"}]
            [:message "node-id0" :forwarded-subscription {:id "node-id1"}]
            [:message "node-id0" :forwarded-subscription {:id "node-id1"}]),
          :config {:connection-redundancy 2, :c :connection-redundancy},
          :network
          {"node-id1" {:self {:id "node-id1"}, :upstream {}, :downstream {}},
           "node-id0"
           {:self {:id "node-id0"},
            :upstream {"node-id0" {:id "node-id0"}},
            :downstream {"node-id0" {:id "node-id0"}}}}})))
