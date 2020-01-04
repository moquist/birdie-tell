(ns scamp.util-test
  (:require [schema.core :as s]
            [clojure.test :refer [deftest is]]
            [scamp.core :as scamp]
            [scamp.util :as util]))

(s/set-fn-validation! true)

(deftest key-swap-test
  (let [old-node-contact-address "friend-01"
        new-node-contact-address "friend-02"
        node-neighbors {"this-node" {:weight 43}
                        old-node-contact-address {:weight 8}}
        old-node-val (node-neighbors old-node-contact-address)]
    (is (= ((util/key-swap node-neighbors old-node-contact-address new-node-contact-address)
            new-node-contact-address)
           old-node-val))))
