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

(defn- purge-world-async-state [world]
  (reduce (fn [world [node-contact-address networked-node]]
            (assoc-in world [:network node-contact-address]
                      (assoc networked-node
                             :send-next-heartbeats-milli-time nil
                             :heartbeat-timeout-milli-time nil)))
          world
          (:network world)))

(defn scamp-test [f]
  (core/reset-envelope-ids!)
  (reset-rand-state!)
  (core/reset-clock!)
  (f))

(deftest maybe-conj-test
  (is (= #{"susan" "howatch" "louis" "sachar"}
         (core/maybe-conj #{"susan" "howatch" "louis"} "vinge" "sachar")))
  (is (= #{"susan" "howatch" "louis" "sachar"}
         (core/maybe-conj #{"susan" "howatch" "louis" "sachar"} "vinge" "vinge"))))

(deftest forward-subscription-test
  (scamp-test
   #(binding [scamp.core/*rand* testing-rand*]
      (let [result (->> (core/forward-subscription #{"stuffy-node" "stuffier-node"}
                                                   "allergen-free-node"
                                                   "43")
                        (map purge-envelope-id))]
        (is (= result
               [[:message-envelope "stuffy-node" :forwarded-subscription "allergen-free-node"]]))))))

(deftest receive-msg-new-subscription-test
  (scamp-test
   #(binding [scamp.core/*rand* testing-rand*]
      (let [node-base (assoc (core/node-contact-address->node "Auri")
                             :upstream #{"Kvothe" "Mandrag"}
                             :downstream #{"Fulcrum" "Scaperling" "Foxen" "Underthing" "Mantle" "Withy" "Crumbledon" "Cricklet" "The Twelve"}
                             :messages-seen {})]
        (is (= (core/receive-msg :new-subscription
                                 core/default-config
                                 node-base
                                 "Ninewise"
                                 "43")
               [(update node-base :upstream conj "Ninewise")
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
               ))))))

(deftest receive-msg-new-upstream-node-test
  (scamp-test
   #(binding [scamp.core/*rand* testing-rand*]
      (let [node-base (assoc (core/node-contact-address->node "Braedee")
                             :upstream #{"Charon"}
                             :downstream #{"Elnear" "Luzali" "Cat"}
                             :messages-seen {})
            result (core/receive-msg :new-upstream-node
                                     core/default-config
                                     node-base
                                     "Elnear"
                                     "43")]
        (is (= result
               [(update node-base :upstream conj "Elnear")
                []]))))))

(deftest receive-msg-forwarded-subscription
  (scamp-test
   #(binding [scamp.core/*rand* testing-rand*]
      (let [node-base (assoc (core/node-contact-address->node "Braedee")
                             :upstream #{"Charon"}
                             :downstream #{"Elnear" "Luzali" "Cat"}
                             :messages-seen {})]
        (let [dup (core/receive-msg :forwarded-subscription
                                    core/default-config
                                    node-base
                                    "Elnear"
                                    "43")]
          (is (= dup
                 [node-base
                  [[:message-envelope "Luzali" :forwarded-subscription "Elnear" "43"]]])))

        (let [keep (core/receive-msg :forwarded-subscription
                                     core/default-config
                                     (assoc node-base :downstream #{})
                                     "Mikah"
                                     "43")]
          (is (= keep
                 [(assoc node-base :downstream #{"Mikah"})
                  [[:message-envelope "Mikah" :new-upstream-node "Braedee" "1"]]])))

        (let [new-node-base (assoc node-base
                                   :downstream
                                   #{"Daric" "Lazuli" "Argentyne" "Jiro" "Talitha"})
              fwd (core/receive-msg :forwarded-subscription
                                    core/default-config
                                    new-node-base
                                    "Mikah"
                                    "43")]
          (is (= fwd
                 [new-node-base
                  [[:message-envelope "Talitha" :forwarded-subscription "Mikah" "43"]]])))))))

(deftest msg->envelope-test
  ;; Probably a worthless test, but does ensure that 'scamp-test
  ;; resets envelope-id-counter as expected.
  (scamp-test
   #(is (= (core/msg->envelope "Esther" :new-subscription "Jonathan")
           [:message-envelope "Esther" :new-subscription "Jonathan" "1"]))))

(deftest receive-msg-node-removal
  (scamp-test
   #(binding [scamp.core/*rand* testing-rand*]
      (let [node-base (assoc (core/node-contact-address->node "Braedee")
                             :upstream #{"Charon" "Centauri"}
                             :downstream #{"Daric" "Centauri"}
                             :messages-seen {})
            rem (core/receive-msg :node-removal
                                  core/default-config
                                  node-base
                                  "Centauri"
                                  "43")]
        (is (= rem
               [(assoc node-base
                       :upstream #{"Charon"}
                       :downstream #{"Daric"})
                []]))))))

(deftest node-do-async-processing-test
  (scamp-test
   #(binding [scamp.core/*rand* testing-rand*]
      (let [node-base (assoc (core/node-contact-address->node "lewis")
                             :upstream #{"macdonald"}
                             :downstream #{"studdock" "feverstone" "hingest" "merlinus" "ironwood"}
                             :messages-seen {}
                             :send-next-heartbeats-milli-time 10
                             :heartbeat-timeout-milli-time 15)]
        (testing "heartbeat sending"
          (core/tick-clock-millis! 14)
          (is (= (core/node-do-async-processing core/default-config node-base)
                 [(assoc node-base :send-next-heartbeats-milli-time 30014)
                  [[:message-envelope "merlinus" :heartbeat "lewis" "1"]
                   [:message-envelope "ironwood" :heartbeat "lewis" "2"]
                   [:message-envelope "studdock" :heartbeat "lewis" "3"]
                   [:message-envelope "feverstone" :heartbeat "lewis" "4"]
                   [:message-envelope "hingest" :heartbeat "lewis" "5"]]])))

        (testing "both heartbeat sending and heartbeat timeout"
          (core/tick-clock-millis! 5)
          (is (= (core/node-do-async-processing core/default-config node-base)
                 [(assoc node-base
                         :upstream #{}
                         :heartbeat-timeout-milli-time nil
                         :send-next-heartbeats-milli-time 30019)
                  [[:message-envelope "merlinus" :new-subscription "lewis" "6"]
                   [:message-envelope "merlinus" :heartbeat "lewis" "7"]
                   [:message-envelope "ironwood" :heartbeat "lewis" "8"]
                   [:message-envelope "studdock" :heartbeat "lewis" "9"]
                   [:message-envelope "feverstone" :heartbeat "lewis" "10"]
                   [:message-envelope "hingest" :heartbeat "lewis" "11"]]])))))))

(deftest receive-msg-heartbeat-test
  (scamp-test
   #(binding [scamp.core/*rand* testing-rand*]
      (let [node-base (assoc (core/node-contact-address->node "valjean")
                             :heartbeat-timeout-milli-time 43)]
        (testing "heartbeat receiving"
          (core/tick-clock-millis! 40)
          (is (= (core/receive-msg :heartbeat core/default-config node-base nil "envelope-7")
                 [(assoc node-base
                         :heartbeat-timeout-milli-time 45040)
                  []])))))))

(deftest heartbeats-test
  (scamp-test
   #(binding [scamp.core/*rand* testing-rand*]
      (let [node-base (assoc (core/node-contact-address->node "valjean")
                             :heartbeat-timeout-milli-time 43)]
        (testing "heartbeat receiving"
          (core/tick-clock-millis! 40)
          (is (= (core/receive-msg :heartbeat core/default-config node-base nil "envelope-7")
                 [(assoc node-base
                         :heartbeat-timeout-milli-time 45040)
                  []])))))))

;; TODO: set up a world with enough nodes to have some interesting upstream/downstream, maybe manaully set a couple to have shorter heartbeat-timeouts than the send-next-heartbeats-milli-time of everything else, so they unsubscribe and re-subscribe




(comment
  (binding [scamp.core/*rand* testing-rand*]
    (for [comm (range 2 252)]
      (-> comm-test-world (core/do-comms comm) (dissoc :config) purge-world-envelope-ids)))

  (binding [scamp.core/*rand* testing-rand*]
    (let [world comm-test-world
          result (loop [world world messages-count 0]
                   (if (-> world :message-envelopes empty?)
                     {:world world :messages-count messages-count}
                     (recur (core/world-do-comm world) (inc messages-count))))
          end-world (-> result
                        :world
                        (dissoc :config)
                        purge-world-envelope-ids)]
      (clojure.pprint/pprint {:end-world end-world
                              :messages-count (:messages-count result)})
      ))

  )
