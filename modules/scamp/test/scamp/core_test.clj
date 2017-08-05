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
  (f)
  (println :clock-value (core/*milli-time*)))

(deftest maybe-conj-test
  (is (= #{"susan" "howatch" "louis" "sachar"}
         (core/maybe-conj #{"susan" "howatch" "louis"} "vinge" "sachar")))
  (is (= #{"susan" "howatch" "louis" "sachar"}
         (core/maybe-conj #{"susan" "howatch" "louis" "sachar"} "vinge" "vinge"))))

(deftest world-subscribe-new-node-test
  (scamp-test
   #(is (= (-> core/new-world
               (core/world-add-new-node (core/node-contact-address->node "node-id0"))
               (core/world-subscribe-new-node "node-id1" "node-id0")
               (dissoc :config)
               purge-world-envelope-ids
               purge-world-async-state)
           {:message-envelopes
            [[:message-envelope "node-id0" :new-subscription "node-id1"]],
            :network
            {"node-id0"
             (assoc (core/node-contact-address->node "node-id0")
                    :upstream #{},
                    :downstream #{},
                    :messages-seen {}),
             "node-id1"
             (assoc (core/node-contact-address->node "node-id1")
                    :upstream #{},
                    :downstream #{"node-id0"},
                    :messages-seen {})}}))))

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
        world (core/world-add-new-node core/new-world
                                       (core/node-contact-address->node (node-name 0)))]
    (loop [world world
           n 0]
      (if (>= n subscriptions-count)
        world
        (let [new-node-id-num (inc n)]
          (recur (core/world-do-all-comms
                  (core/world-subscribe-new-node world
                                                 (node-name new-node-id-num)
                                                 (node-name n)))
                 new-node-id-num))))))

(deftest do-comm-test
  (scamp-test
   #(binding [scamp.core/*rand* testing-rand*]
      (let [world (world-with-subs 3)
            end-world (-> (reduce
                           (fn [world _n] (core/world-do-comm world))
                           world
                           (range 9))
                          (dissoc :config)
                          purge-world-envelope-ids)]
        (is (= (purge-world-async-state end-world)
               {:message-envelopes [],
                :network
                {"node-id0"
                 (assoc (core/node-contact-address->node "node-id0")
                        :upstream #{"node-id1"},
                        :downstream #{"node-id2" "node-id3"},
                        :messages-seen {"1" 1, "3" 1, "4" 1, "5" 5, "9" 1}),
                 "node-id1"
                 (assoc (core/node-contact-address->node "node-id1")
                        :upstream #{"node-id2"},
                        :downstream #{"node-id2" "node-id3" "node-id0"},
                        :messages-seen {"2" 1, "4" 1, "5" 9, "9" 2, "10" 1, "11" 5}),
                 "node-id2"
                 (assoc (core/node-contact-address->node "node-id2")
                        :upstream #{"node-id3" "node-id0" "node-id1"},
                        :downstream #{"node-id3" "node-id1"},
                        :messages-seen
                        {"6" 1, "4" 1, "5" 10, "7" 1, "8" 1, "9" 1, "11" 5}),
                 "node-id3"
                 (assoc (core/node-contact-address->node "node-id3")
                        :upstream #{"node-id2" "node-id0" "node-id1"},
                        :downstream #{"node-id2"},
                        :messages-seen {"12" 1, "13" 1, "11" 3, "14" 1})}}
               ))))))

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

(s/defn do-comms :- core/WorldSchema
  "Take 'world and 'n. Process up to 'n messages. Return new 'world.
   If 'world has no :message-envelopes before 'n messages have been
   processed, 'world is returned immediately."
  [world :- core/WorldSchema
   n :- s/Int]
  (if (or (zero? n)
          (empty? (:message-envelopes world)))
    world
    (recur (core/world-do-comm world) (dec n))))

(deftest receive-msg-node-unsubscription-test
  (scamp-test
   #(binding [scamp.core/*rand* testing-rand*]
      (let [world (-> (world-with-subs 43)
                      core/world-do-all-comms
                      (core/world-instruct-node-to-unsubscribe "node-id19")
                      core/world-do-comm)]
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
