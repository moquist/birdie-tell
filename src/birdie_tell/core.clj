(ns birdie-tell.core
  "This ns implements a simple Gossip protocol, where peers tell each
  other about each other, and whether each other peer appears to be
  :alive or :dead right now.

  That's it; this doesn't do any actual work at this point."
  (:require [server.socket :as socket]
            [cheshire.core :as cheshire]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [clojurewerkz.vclock.core :as vclock]
            [clojure.pprint :refer [pprint]])
  (:import (java.net Socket SocketException)
           (java.io PrintWriter InputStreamReader BufferedReader)))

(defn vclock-play []
  (let [a0 (vclock/fresh :127.0.0.1:1234 :alive)
        b0 (vclock/fresh :127.0.0.1:1235 :alive)
        a1 (vclock/increment a0 [:dabba :yeep])
        ]
    {:a0 a0
     :b0 b0
     :a1 a1}
    ))

;; To read:
;; https://github.com/michaelklishin/vclock
;; http://courses.csail.mit.edu/6.895/fall02/papers/Ladin/acmtocs.pdf
;; https://code.google.com/p/cassandra-shawn/wiki/GossipProtocol
;; http://doc.akka.io/docs/akka/snapshot/common/cluster.html
;; https://github.com/edwardcapriolo/gossip
;; http://courses.washington.edu/css434/students/Gossip.pdf

;; Basically, I need a kind of shared clock (vector clock, likely) and
;; each piece of juicy gossip needs to show up with a timestamp
;; according to the shared clock. This allows us to reject old news,
;; and mitigate the hotness of news that's pretty old.

;; * what are the states? what does the state machine look like?
;; * what is the protocol?
;; * what are the transitions between states?
;; * what transport options are there? sockets, core.async, other?
;; * separate ns for user interaction: init the state, read the state, update the state
;; * separate ns for CLI interface

;; right now, we only notice a peer is dead if we try to talk to it and can't.
;; what if nobody(?) hears from it for a while?

(def peers (atom {:states {}
                  ;; :states is a map from [host port] to :state
                  ;; :hotness is a map from [host port] to a hotness
                  ;;          integer that approaches zero as news
                  ;;          ages
                  :hotness {}}))

(defn- split-hostport
  "Split a host:port pair into a vector with a string host and an integer port.

  Example:
      (split-hostport \"hostname:1234\")
      ;=> [\"hostname\" 1234]"
  [host-port]
  ;; TODO: add error checking
  (let [[ip-addr port] (str/split host-port #":")
        port (Integer/parseInt port)]
    [ip-addr port]))

(defn- join-hostport
  "Join a host, port pair into a host:port string."
  [host port]
  (str host ":" port))

(defn- update-peer!
  "Update a peer's state in the specified 'peers atom.

  Take a 'peers atom, a 'max-hotness integer, and a vector with this shape:
      [[host port] state]

  Return nil."
  [peers max-hotness [host-port state]]
  {:pre [(vector? host-port) (keyword? state)]}
  (swap! peers #(merge-with merge
                            %
                            {:states {host-port state}
                             :hotness {host-port max-hotness}}))
  nil)

(defn- import-chirps
  "Take a map of input 'chirps. Split host:port pairs and ensure states
  are keywords.

  Example:
      (import-chirps {\"127.0.0.1:2234\" \"alive\", \"127.0.0.1:2237\" \"alive\"})
      ;==> {[\"127.0.0.1\" 2234] :alive, [\"127.0.0.1\" 2237] :alive}
  "
  [chirps]
  (into {}
        (map (fn [[k v]] [(split-hostport k) (keyword v)])
             chirps)))

;; TODO: think about core.async
(defn- receive-gossip
  "Take an 'input-stream and a (unused) '_output-stream.

  Parse 'input-stream JSON into a 'news map (see 'choose-news), and
  map 'update-peer! across each entry in that map.

  Return nil."
  [max-hotness input-stream _output-stream]
  (let [chirps (-> input-stream
                   (InputStreamReader. "UTF-8")
                   cheshire/parse-stream)
        chirps (import-chirps chirps)]
    ;; TODO: pull side-effects out, pass fn in (to allow for multiple implementations and general decomplection)
    (dorun (map (partial update-peer! #'peers max-hotness) chirps))
    nil))

(defn- listen
  "Start listening on 'port, calling 'receive-gossip with 'max-hotness
  on connect."
  [max-hotness port]
  (socket/create-server port (partial receive-gossip max-hotness)))

;; TODO: think about core.async
(defn- send-gossip
  [max-hotness news [ip-addr port]]
  (try
    (with-open [socket (Socket. ip-addr port)
                reader (BufferedReader. (InputStreamReader. (.getInputStream socket)))
                writer (PrintWriter. (.getOutputStream socket))]
      (doto writer
        (.println (cheshire/generate-string news))
        (.flush)))
    (catch Exception e
      (println :exception (.getMessage e))
      (println "Seems dead: " [ip-addr port])
      ;; TODO: pull side-effects out, pass fn in (to allow for multiple implementations and general decomplection)
      (swap! #'peers #(merge-with merge
                                %
                                {:states {[ip-addr port] :dead}
                                 :hotness {[ip-addr port] max-hotness}})))))

(defn choose-news
  "Choose all the latest hot news that I have in my little bird-brain.

  Returns a news-map with this shape:

  {host-port :status
   host-port2 :status}

  For example: {\"127.0.0.1:1234\": \"alive\"
                \"127.0.0.1:1235\": \"dead\"}
  "
  [peers]
  (->> peers
       :hotness
       (map first)
       (select-keys (:states peers))
       (map (fn [[[host port] v]] [(join-hostport host port) v]))
       (into {})))

(defn- get-peer
  "Select a random :alive peer if possible, else a :dead one.

  If there are no peers, return nil.

  Returns a peer vector in this shape:
  [host port]"
  [live-percentage peers]
  (let [peers (reduce (fn [r [k v]] (merge-with concat r {v [k]}))
                      {}
                      (:states peers))
        ;; sometimes contact a dead one on purpose
        [primary-state secondary-state] (if (< (rand-int 100) live-percentage)
                                          [:alive :dead]
                                          [:dead :alive])
        peer (if (seq (primary-state peers))
               (rand-nth (primary-state peers))
               (rand-nth (secondary-state peers)))]
    peer))

(defn- distract-self
  "Decrease the hotness of all current news, eliminating entirely any
  news that reaches a hotness level of zero."
  [peers]
  (swap! peers assoc :hotness
         (->> @peers
              :hotness
              (filter (fn [[host-port hotness]] (< 1 hotness)))
              (map (fn [[host-port hotness]] [host-port (dec hotness)]))
              (into {}))))

(defn mingle [live-percentage max-hotness min-rest-time max-rest-time my-ip-addr my-port]
  (let [wait-range (- max-rest-time min-rest-time)]
    (while true
      (pprint [:peers @peers])
      (if-let [peer (get-peer live-percentage @peers)]
        (send-gossip
         max-hotness
         (assoc (choose-news @peers)
                (join-hostport my-ip-addr my-port) :alive)
         peer))
      ;; forget stuff even if there's nobody to tell; it'll be old news later
      (distract-self peers)
      (Thread/sleep (+ min-rest-time (rand-int wait-range))))))

(defn main
  "'max-hotness: how many times do I tell another peer about this peer?
  'live-percentage: what percentage of the time do I bias toward talking to peers I think are :alive, instead of :dead?"
  [max-hotness live-percentage listen-port my-host-port & initial-peers]
  (swap! #'peers assoc
         :states
         (reduce (fn [r x] (assoc r (split-hostport x) :alive))
                 {}
                 initial-peers))
  (println :main-peers @#'peers)
  (let [[my-ip-addr my-port] (split-hostport my-host-port)
        listen-port (Integer/parseInt listen-port)
        max-hotness (Integer/parseInt max-hotness)
        live-percentage (Integer/parseInt live-percentage)]
    (listen max-hotness listen-port)
    (mingle live-percentage max-hotness 1000 5000 my-ip-addr my-port)))


(comment
  (require 'birdie-tell.core :reload-all) (in-ns 'birdie-tell.core) (use 'clojure.repl)

  ;; peers example
  (def peers (atom
              {:states {["127.0.0.1" 1997] :alive
                        ["127.0.0.1" 1996] :alive
                        ["127.0.0.1" 1995] :dead
                        ["127.0.0.1" 1994] :mostly-dead}
               :hotness {["127.0.0.1" 1997] 2
                         ["127.0.0.1" 1996] 1
                         ["127.0.0.1" 1995] 5
                         ["127.0.0.1" 1994] 3}
               }))

  (->> 'cheshire.core ns-map keys (map name) (filter #(re-find #"ns-" %)))
  (->> 'cheshire.core ns-publics keys (map name))
  )

