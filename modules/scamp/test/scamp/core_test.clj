(ns scamp.core-test
  (:require [clojure.math.numeric-tower :as math]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [scamp.core :as core]
            [scamp.util :as util]
            [schema.core :as s]))

(s/set-fn-validation! true)

(def random-instance (atom (java.util.Random.)))
(defn test-clock [] (atom 0))

(defn reset-rand-state! [& seed]
  (let [seed (or seed 43)]
    (.setSeed @random-instance seed)))

(defn- testing-rand*
  ([] (.nextFloat @random-instance))
  ([n] (* n (testing-rand*))))

(s/defn node-contact-addresses->node-neighbors :- core/NodeNeighborsSchema
  [node-contact-addresses]
  (->> node-contact-addresses
       (map #(vector % core/default-node-neighbor))
       (into {})))

(defn- purge-envelope-id [envelope]
  (vec (butlast envelope)))

(defn- purge-world-envelope-ids [world]
  (update world :message-envelopes #(mapv purge-envelope-id %)))

(defn- purge-world-async-state [world]
  (reduce (fn [world [node-contact-address networked-node]]
            (assoc-in world [:network node-contact-address]
                      (assoc networked-node
                             :send-next-heartbeats-milli-time nil
                             :heartbeat-timeout-milli-time nil)))
          world
          (:network world)))

(defn scamp-test [f]
  (binding [scamp.core/*rand* testing-rand*]
    (core/reset-envelope-ids!)
    (reset-rand-state!)
    (f)))

(deftest maybe-neighbor-test
  (is (= (node-contact-addresses->node-neighbors ["susan" "howatch" "louis" "sachar"])
         (core/maybe-neighbor (node-contact-addresses->node-neighbors ["susan" "howatch" "louis"])
                          "vinge"
                          "sachar")))
  (is (= (node-contact-addresses->node-neighbors ["susan" "howatch" "louis" "sachar"])
         (core/maybe-neighbor (node-contact-addresses->node-neighbors ["susan" "howatch" "louis" "sachar"])
                          "vinge"
                          "vinge"))))

(deftest forward-subscription-test
  (scamp-test
   #(let [result (->> (core/forward-subscription (node-contact-addresses->node-neighbors
                                                  ["stuffy-node" "stuffier-node"])
                                                 "allergen-free-node"
                                                 "43")
                      (map purge-envelope-id))]
      (is (= result
             [[:message-envelope "stuffy-node" :forwarded-subscription "allergen-free-node"]])))))

(deftest receive-msg-new-subscription-test
  (scamp-test
   #(let [node-base (assoc (core/node-contact-address->node "Auri")
                           :upstream (node-contact-addresses->node-neighbors ["Kvothe" "Mandrag"])
                           :downstream (node-contact-addresses->node-neighbors
                                        ["Fulcrum" "Scaperling"
                                         "Foxen" "Underthing" "Mantle"
                                         "Withy" "Crumbledon"
                                         "Cricklet" "The Twelve"])
                           :messages-seen {})]
      (is (= (core/receive-msg :new-subscription
                               core/default-config
                               node-base
                               "Ninewise"
                               "43")
             [(update node-base :upstream assoc "Ninewise" core/default-node-neighbor)
              [[:message-envelope "Fulcrum" :forwarded-subscription "Ninewise" "1"]
               [:message-envelope "Fulcrum" :forwarded-subscription "Ninewise" "2"]
               [:message-envelope "Fulcrum" :forwarded-subscription "Ninewise" "3"]
               [:message-envelope
                "Crumbledon"
                :forwarded-subscription
                "Ninewise"
                "4"]
               [:message-envelope "Mantle" :forwarded-subscription "Ninewise" "5"]
               [:message-envelope
                "Cricklet"
                :forwarded-subscription
                "Ninewise"
                "6"]
               [:message-envelope "Foxen" :forwarded-subscription "Ninewise" "7"]
               [:message-envelope
                "The Twelve"
                :forwarded-subscription
                "Ninewise"
                "8"]
               [:message-envelope
                "Underthing"
                :forwarded-subscription
                "Ninewise"
                "9"]
               [:message-envelope "Withy" :forwarded-subscription "Ninewise" "10"]
               [:message-envelope
                "Scaperling"
                :forwarded-subscription
                "Ninewise"
                "11"]]])))))

(deftest receive-msg-new-upstream-node-test
  (scamp-test
   #(let [node-base (assoc (core/node-contact-address->node "Braedee")
                           :upstream (node-contact-addresses->node-neighbors ["Charon"])
                           :downstream (node-contact-addresses->node-neighbors ["Elnear" "Luzali" "Cat"])
                           :messages-seen {})
          result (core/receive-msg :new-upstream-node
                                   core/default-config
                                   node-base
                                   "Elnear"
                                   "43")]
      (is (= result
             [(update node-base :upstream assoc "Elnear" core/default-node-neighbor)
              []])))))

(deftest receive-msg-forwarded-subscription
  (scamp-test
   #(let [node-base (assoc (core/node-contact-address->node "Braedee")
                           :upstream (node-contact-addresses->node-neighbors ["Charon"])
                           :downstream (node-contact-addresses->node-neighbors ["Elnear" "Luzali" "Cat"])
                           :messages-seen {})]
      (let [dup (core/receive-msg :forwarded-subscription
                                  core/default-config
                                  node-base
                                  "Elnear"
                                  "43")]
        (is (= dup
               [node-base
                [[:message-envelope "Cat" :forwarded-subscription "Elnear" "43"]]])))

      (let [keep (core/receive-msg :forwarded-subscription
                                   core/default-config
                                   (assoc node-base :downstream {})
                                   "Mikah"
                                   "43")]
        (is (= keep
               [(assoc node-base :downstream (node-contact-addresses->node-neighbors ["Mikah"]))
                [[:message-envelope "Mikah" :new-upstream-node "Braedee" "1"]]])))

      (let [new-node-base (assoc node-base
                                 :downstream
                                 (node-contact-addresses->node-neighbors
                                  ["Daric" "Lazuli" "Argentyne" "Jiro" "Talitha"]))
            fwd (core/receive-msg :forwarded-subscription
                                  core/default-config
                                  new-node-base
                                  "Mikah"
                                  "43")]
        (is (= fwd
               [new-node-base
                [[:message-envelope "Daric" :forwarded-subscription "Mikah" "43"]]]))))))

(deftest msg->envelope-test
  ;; Probably a worthless test, but does ensure that 'scamp-test
  ;; resets envelope-id-counter as expected.
  (scamp-test
   #(is (= (core/msg->envelope "Esther" :new-subscription "Jonathan")
           [:message-envelope "Esther" :new-subscription "Jonathan" "1"]))))

(deftest receive-msg-node-removal
  (scamp-test
   #(let [node-base (assoc (core/node-contact-address->node "Braedee")
                           :upstream (node-contact-addresses->node-neighbors ["Charon" "Centauri"])
                           :downstream (node-contact-addresses->node-neighbors ["Daric" "Centauri"])
                           :messages-seen {})
          rem (core/receive-msg :node-removal
                                core/default-config
                                node-base
                                "Centauri"
                                "43")]
      (is (= rem
             [(assoc node-base
                     :upstream (node-contact-addresses->node-neighbors ["Charon"])
                     :downstream (node-contact-addresses->node-neighbors ["Daric"]))
              []])))))

(deftest node-do-async-processing-test
  (scamp-test
   #(let [node-base (assoc (core/node-contact-address->node "lewis")
                           :upstream (node-contact-addresses->node-neighbors ["macdonald"])
                           :downstream (node-contact-addresses->node-neighbors
                                        ["studdock" "feverstone" "hingest" "merlinus" "ironwood"])
                           :messages-seen {}
                           :send-next-heartbeats-milli-time 10
                           :heartbeat-timeout-milli-time 15
                           :clock (test-clock))
          clock (:clock node-base)]
      (testing "heartbeat sending"
        (core/tick-clock-millis! clock 14)
        (is (= (core/node-do-async-processing core/default-config node-base)
               [(assoc node-base :send-next-heartbeats-milli-time 30014)
                [[:message-envelope "studdock" :heartbeat "lewis" "1"]
                 [:message-envelope "feverstone" :heartbeat "lewis" "2"]
                 [:message-envelope "hingest" :heartbeat "lewis" "3"]
                 [:message-envelope "merlinus" :heartbeat "lewis" "4"]
                 [:message-envelope "ironwood" :heartbeat "lewis" "5"]]])))

      (testing "both heartbeat sending and heartbeat timeout"
        (core/tick-clock-millis! clock 5)
        (is (= (core/node-do-async-processing core/default-config node-base)
               [(assoc node-base
                       :upstream {}
                       :heartbeat-timeout-milli-time nil
                       :send-next-heartbeats-milli-time 30019)
                [[:message-envelope "studdock" :new-subscription "lewis" "6"]
                 [:message-envelope "studdock" :heartbeat "lewis" "7"]
                 [:message-envelope "feverstone" :heartbeat "lewis" "8"]
                 [:message-envelope "hingest" :heartbeat "lewis" "9"]
                 [:message-envelope "merlinus" :heartbeat "lewis" "10"]
                 [:message-envelope "ironwood" :heartbeat "lewis" "11"]]
                ]))))))

(deftest receive-msg-heartbeat-test
  (scamp-test
   #(let [node-base (assoc (core/node-contact-address->node "valjean")
                           :heartbeat-timeout-milli-time 43
                           :clock (test-clock))
          clock (:clock node-base)]
      (testing "heartbeat receiving"
        (core/tick-clock-millis! clock 40)
        (is (= (core/receive-msg :heartbeat core/default-config node-base nil "envelope-7")
               [(assoc node-base
                       :heartbeat-timeout-milli-time 45040)
                []]))))))

(deftest heartbeats-test
  (scamp-test
   #(let [node-base (assoc (core/node-contact-address->node "valjean")
                           :heartbeat-timeout-milli-time 43
                           :clock (test-clock))
          clock (:clock node-base)]
      (testing "heartbeat receiving"
        (core/tick-clock-millis! clock 40)
        (is (= (core/receive-msg :heartbeat core/default-config node-base nil "envelope-7")
               [(assoc node-base
                       :heartbeat-timeout-milli-time 45040)
                []]))))))

(deftest receive-msg-node-replacement-test
  (let [node (assoc (core/node-contact-address->node "Frodo")
                    :upstream (node-contact-addresses->node-neighbors
                               ["Gandalf" "Gaffer" "Gollum"])
                    :downstream (node-contact-addresses->node-neighbors
                                 ["Sam" "Gollum" "Meriadoc" "Peregrin"]))]
    (let [[node _] (core/receive-msg :node-replacement core/default-config
                                      node {:old "Gollum" :new "Bilbo"}
                                      "envelope-43")]
      (is (= (-> node :upstream keys set)
             #{"Gandalf" "Gaffer" "Bilbo"}))
      (is (= (-> node :downstream keys set)
             #{"Sam" "Bilbo" "Meriadoc" "Peregrin"})))))

(deftest rebalance-to-1-test
  (let [sum-to-1 (prop/for-all (v (gen/vector (gen/such-that pos?
                                                             (gen/fmap math/abs
                                                                       (gen/one-of [gen/ratio gen/small-integer gen/nat gen/large-integer])))))
                               (or (empty? v)
                                   (= 1 (apply + (core/rebalance-to-1 v)))))
        {:as qc-report
         :keys [result]} (tc/quick-check 100000 sum-to-1)]
    (when-not result
      (prn qc-report))
    (is result)))

(defn round-weight
  [weight]
  (-> weight
      (* 1000.0)
      int
      (/ 1000.0)))

(defn weights-equalish?
  "If 'weights are equal to within a hundredth, they're close enough."
  [& weights]
  (->> weights
       (map round-weight)
       (apply =)))

(defn round-neighbors-weights
  [neighbors-collection]
  (util/map-map neighbors-collection
                :val-fn
                (fn rnw* [n] (update n :weight round-weight))))

(defn nodes-equal?
  [& nodes]
  (->> nodes
       (map (fn rw* [node]
              (-> node
                  (update :upstream round-neighbors-weights)
                  (update :downstream round-neighbors-weights))))))

(deftest rebalance-arc-weights-test
  (let [node-neighbors {"node1" {:weight 1} "node2" {:weight 2} "node3" {:weight 3}}
        expected {"node1" {:weight 1/6} "node2" {:weight 1/3} "node3" {:weight 1/2}}]
    (is (map
         (fn [[nca n]]
           (weights-equalish? (:weight n)
                              (get-in expected [nca :weight])))
         (core/rebalance-arc-weights node-neighbors)))))

(deftest node-rebalance-arc-weights-test
  (let [org-upstream (node-contact-addresses->node-neighbors
                      ["Gandalf" "Gaffer" "Gollum"])
        org-downstream (node-contact-addresses->node-neighbors
                        ["Sam" "Gollum" "Meriadoc" "Peregrin"])
        expected-upstream-weight (/ 1.0 (count org-upstream))
        expected-downstream-weight (/ 1.0 (count org-downstream))
        this-node "Frodo"
        starting-node (assoc (core/node-contact-address->node this-node)
                             :upstream org-upstream
                             :downstream org-downstream)

        [{:as node :keys [upstream downstream]} messages]
        (core/node-rebalance-arc-weights core/default-config starting-node)]
    (is (every? (fn [[_k {:keys [weight]}]]
                  (weights-equalish? weight expected-upstream-weight))
                upstream))
    (is (every? (fn [[_k {:keys [weight]}]]
                  (weights-equalish? weight expected-downstream-weight))
                downstream))
    (is (every? (fn [[_ _ _ body _]]
                  (let [{:keys [upstream-node downstream-node weight]} body]
                    (condp = this-node
                      downstream-node (weights-equalish? weight expected-upstream-weight)
                      upstream-node (weights-equalish? weight expected-downstream-weight)
                      false)))
                messages))))

(deftest receive-msg-new-arc-weight-test
  (let [org-upstream (node-contact-addresses->node-neighbors
                      ["Gandalf" "Gaffer" "Gollum"])
        org-downstream (node-contact-addresses->node-neighbors
                        ["Sam" "Gollum" "Meriadoc" "Peregrin"])
        this-node "Frodo"
        starting-node (assoc (core/node-contact-address->node this-node)
                             :upstream org-upstream
                             :downstream org-downstream)
        [node1] (core/receive-msg :new-arc-weight
                                  core/default-config
                                  starting-node
                                  {:upstream-node this-node :downstream-node "Meriadoc" :weight 7} "43")
        [node2] (core/receive-msg :new-arc-weight
                                  core/default-config
                                  starting-node
                                  {:upstream-node "Gollum" :downstream-node this-node :weight 17} "43")
        [node3] (core/receive-msg :new-arc-weight
                                  (assoc-in core/default-config [:logging :level] :fatal)
                                  starting-node
                                  {:upstream-node "Wiggum" :downstream-node this-node :weight 97} "43")
        ]
    (is (nodes-equal? node1 (assoc-in starting-node [:downstream "Meriadoc" :weight] 7)))
    (is (nodes-equal? node2 (assoc-in starting-node [:upstream "Gollum" :weight] 17)))
    (is (nodes-equal? node3 starting-node))))

(deftest neighbors->average-weight-test
  (is (= 2.0 (core/neighbors->average-weight {"Elnear" {:weight 1},
                                              "Luzali" {:weight 2},
                                              "Cat" {:weight 3}}))))


;; TODO: set up a world with enough nodes to have some interesting upstream/downstream, maybe manaully set a couple to have shorter heartbeat-timeouts than the send-next-heartbeats-milli-time of everything else, so they unsubscribe and re-subscribe

;; TODO: test unsubscription (and everything else) with :connection-redundancy 0

;; TODO: test :message-dup-drop-after
