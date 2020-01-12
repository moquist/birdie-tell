(ns world.main-test
  (:require [clojure.data :as cd]
            [clojure.test :refer :all]
            [schema.core :as s]
            [world.main :as main]
            [scamp.core :as scamp]
            [scamp.core-test :as scamp-test]))

(s/set-fn-validation! true)

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
                             :heartbeat-timeout-milli-time nil
                             :clock nil)))
          world
          (:network world)))

(deftest world-subscribe-new-node-test
  (scamp-test/scamp-test
   #(is (= (-> main/world-base
               (main/world-add-new-node (scamp/node-contact-address->node "node-id0"))
               (main/world-subscribe-new-node "node-id1" "node-id0")
               (dissoc :config)
               purge-world-envelope-ids
               purge-world-async-state)
           {:message-envelopes
            [[:message-envelope "node-id0" :new-subscription "node-id1"]],
            :network
            {"node-id0"
             {:self {:id "node-id0"},
              :upstream {},
              :downstream {},
              :messages-seen {},
              :send-next-heartbeats-milli-time nil,
              :heartbeat-timeout-milli-time nil,
              :clock nil},
             "node-id1"
             {:self {:id "node-id1"},
              :upstream {},
              :downstream {"node-id0" {:weight 1.0}},
              :messages-seen {},
              :send-next-heartbeats-milli-time nil,
              :heartbeat-timeout-milli-time nil,
              :clock nil}}}))))


(defn world-with-subs [subscriptions-count]
  (let [node-name #(str "node-id" %)
        world (main/world-add-new-node main/world-base
                                       (scamp/node-contact-address->node (node-name 0)
                                                                         {:clock (scamp-test/test-clock)}))]
    (loop [world world
           n 0]
      (if (>= n subscriptions-count)
        world
        (let [new-node-id-num (inc n)]
          (recur (main/world-do-all-comms
                  (main/world-subscribe-new-node world
                                                 (node-name new-node-id-num)
                                                 (node-name n)))
                 new-node-id-num))))))

(defn purge-world [world]
  (-> world
      (dissoc :config)
      purge-world-async-state
      purge-world-envelope-ids))

(deftest do-comm-test
  (scamp-test/scamp-test
   #(let [world (world-with-subs 3)
          result (-> (reduce
                      (fn [world _n] (main/world-do-comm world))
                      world
                      (range 9)))
          [a b both] (cd/diff (purge-world world) (purge-world result))]
      ;; What, exactly, is being tested here?
      (is (every? nil? [a b])))))

(s/defn do-comms :- main/WorldSchema
  "Take 'world and 'n. Process up to 'n messages. Return new 'world.
   If 'world has no :message-envelopes before 'n messages have been
   processed, 'world is returned immediately."
  [world :- main/WorldSchema
   n :- s/Int]
  (if (or (zero? n)
          (empty? (:message-envelopes world)))
    world
    (recur (main/world-do-comm world) (dec n))))

(deftest receive-msg-cmd-unsubscribe-test
  (scamp-test/scamp-test
   #(let [world (-> (world-with-subs 43)
                    main/world-do-all-comms
                    (main/world-instruct-node-to-unsubscribe "node-id19")
                    main/world-do-comm)]
      (is (= (:message-envelopes world)
             [[:message-envelope
               "node-id20"
               :node-replacement
               {:old "node-id19", :new "node-id18"}
               "287"]
              [:message-envelope
               "node-id18"
               :node-replacement
               {:old "node-id19", :new "node-id20"}
               "288"]
              [:message-envelope "node-id22" :node-removal "node-id19" "289"]
              [:message-envelope "node-id17" :node-removal "node-id19" "290"]
              [:message-envelope "node-id21" :node-removal "node-id19" "291"]
              [:message-envelope "node-id20" :node-removal "node-id19" "292"]
              [:message-envelope "node-id18" :node-removal "node-id19" "293"]
              [:message-envelope "node-id16" :node-removal "node-id19" "294"]])))))

(defn anticipated-scamp-arcs
  "Given 'connection-redundancy and 'n nodes, how many arcs is the
  graph anticipated to have?"
  [connection-redundancy n]
  (* (inc connection-redundancy)
     n
     ;; TODO: which log is this supposed to be?
     (Math/log n)))

(defn demo-unsubscription []
  (scamp-test/scamp-test
   #(let [world (-> main/world-base
                    (assoc-in [:config :connection-redundancy] -1)
                    (main/world-add-new-node (assoc (scamp/node-contact-address->node "node-id0")
                                                    :upstream {"node-id2" scamp/default-node-neighbor}
                                                    :downstream {"node-id1" scamp/default-node-neighbor}
                                                    :clock (scamp-test/test-clock)))
                    (main/world-add-new-node (assoc (scamp/node-contact-address->node "node-id1")
                                                    :upstream {"node-id0" scamp/default-node-neighbor}
                                                    :downstream {"node-id3" scamp/default-node-neighbor
                                                                 "node-id4" scamp/default-node-neighbor}
                                                    :clock (scamp-test/test-clock)))
                    (main/world-add-new-node (assoc (scamp/node-contact-address->node "node-id2")
                                                    :upstream {"node-id4" scamp/default-node-neighbor}
                                                    :downstream {"node-id0" scamp/default-node-neighbor}
                                                    :clock (scamp-test/test-clock)))
                    (main/world-add-new-node (assoc (scamp/node-contact-address->node "node-id3")
                                                    :upstream {"node-id1" scamp/default-node-neighbor}
                                                    :downstream {"node-id4" scamp/default-node-neighbor}
                                                    :clock (scamp-test/test-clock)))
                    (main/world-add-new-node (assoc (scamp/node-contact-address->node "node-id4")
                                                    :upstream {"node-id1" scamp/default-node-neighbor
                                                               "node-id3" scamp/default-node-neighbor}
                                                    :downstream {"node-id2" scamp/default-node-neighbor}
                                                    :clock (scamp-test/test-clock))))
          world-2 (-> world
                      (main/world-instruct-node-to-unsubscribe "node-id4")
                      (main/world-do-all-comms {:verbose? true}))]
      {:world world
       :world-2 world-2})))

(deftest test-demo-unsubscription
  (let [{:keys [world world-2]} (demo-unsubscription)
        [_in-a in-b _in-both] (cd/diff world world-2)]
    (is (get-in in-b [:network "node-id4" :removed?]) "node-id4 has been removed")))

(defn demo-unsubscription->dot [output-filename-base]
  (let [{:keys [world world-2]} (demo-unsubscription)]
    (spit (str output-filename-base "-01.dot") (main/world->dot world))
    (spit (str output-filename-base "-02.dot") (main/world->dot world-2))))

(defn demo-unsubscription2 []
  (scamp-test/scamp-test
   #(let [world (world-with-subs 6)
          world-2 (-> world
                      (main/world-instruct-node-to-unsubscribe "node-id4")
                      (main/world-do-all-comms {:verbose? true}))
          ]
      {:world world
       :world-2 world-2})))

(deftest test-demo-unsubscription2
  (let [{:keys [world world-2]} (demo-unsubscription2)
        [_in-a in-b _in-both] (cd/diff world world-2)]
    (is (get-in in-b [:network "node-id4" :removed?]) "node-id4 has been removed")))

(defn demo-unsubscription2->dot [& [output-filename-base]]
  (let [{:keys [world world-2]} (demo-unsubscription2)]
    (spit (str output-filename-base "-01.dot") (main/world->dot world))
    (spit (str output-filename-base "-02.dot") (main/world->dot world-2))))

(defn pprint-cluster [world]
  (->> world
       :network
       (map (fn [[k v]] [k (select-keys v [:upstream :downstream])]))
       (into {})
       clojure.pprint/pprint))

(comment
  (def mesh-size 100)
  (let [mesh-size 100]
    (println "dot -Tpng -O /tmp/mesh.dot")
    (spit "/tmp/mesh.dot" (str "digraph {"
                               (apply str
                                      (apply concat
                                             (filter (complement nil?)
                                                     (for [s (range mesh-size)]
                                                       (for [d (range mesh-size)]
                                                         (when (not= s d)
                                                           (str s "->" d ";"))))))) "}")))
  )
