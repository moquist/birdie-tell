(ns birdie-tell.util
  (require [clojure.pprint :refer [pprint]]
           [clojure.tools.cli :as cli]))

(def debug? (atom false))

(defn debug [& msgs]
  (when @debug? (pprint [:debug msgs])))

(defn uuid [] (str (java.util.UUID/randomUUID)))

(defn validate-host-port [host-port]
  (re-find #"^[^:]+:[0-9]+$" host-port))

(def cli-options
  [
   ["-d" "--debug" "Enable debug printing" :default false]
   ["-i" "--input-file <file>"
    "This EDN file will be repeatedly read to update the data owned by this peer"]
   ["-h" "--help"]
   [nil "--host-port <host-port>" "Specify this peer's <host>:<port> pair"
    :validate [validate-host-port "host cannot have a colon, and port must be numeric"]]
   ["-l" "--live-percentage <%>"
    "Specifies how often gossip will be directed toward a live random
    peer,rather than a dead peer or a potential peer that hasn't yet
    been reached"
    :default 80
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 100) "Must be a number between 0 and 100"]]
   ["-n" "--name <name>" "This peer's human-readable name"]
   ["-p" "--peer <host-port>" "Specify a <host>:<port> pair to discover a peer."
    :validate [validate-host-port "host cannot have a colon, and port must be numeric"]]
   ["-u" "--uuid <UUID>" "This peer's UUID (optional)"]])

(defn parse-opts [args]
  (let [{:keys [options arguments summary errors]} (cli/parse-opts args cli-options)]
    (cond
      (seq errors) (do (dorun (map println errors)) (System/exit 1))
      (seq arguments) (do (println (str "Error: unexpected argument(s): " arguments "\n")
                                   summary)
                          (System/exit 1))
      (:help options) (do (println summary) (System/exit 0))
      :else options)))
