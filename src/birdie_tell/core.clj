(ns birdie-tell.core
  (:require [liberator.core :as liberator :refer [defresource]]
            [cheshire.core :as cheshire]
            [clojure.edn :as edn]))

(comment
  (def peeps {"192.168.1.1:3245" :live
              "192.168.1.1:3244" :dead
              "192.168.1.1:3246" :mostly-dead})

  (->> 'cheshire.core ns-map keys (map name) (filter #(re-find #"ns-" %)))
  (->> 'cheshire.core ns-publics keys (map name))
  )

(def peeps (atom {}))

(defn add-peep [peeps [ip-addr state]]
  (swap! peeps assoc ip-addr state))

(defresource cheep
  :available-media-types ["application/json" "application/edn"]
  :allowed-methods [:put]
  :handle-ok "hiya"
  :put! (fn [ctx]
          (dorun (map (partial add-peep peeps) (-> ctx :request :edn-body)))
          (println :peeps @peeps)
          #_
          (println :ctx ctx)
          #_
          (println :edn-body (-> ctx :request :edn-body))
          #_
          (cheshire/generate-string {:a 1})))

(defn read-inputstream-edn [input]
  (println :reading-inputstream-edn)
  (let [d (edn/read
           {:eof nil}
           (java.io.PushbackReader.
            (java.io.InputStreamReader. input "UTF-8")))]
    (println :read d)
    d))

(defn parse-edn-body [handler]
  (fn [ctx]
    (print :parsing-yeah ctx)
    (handler (if-let [body (:body ctx)]
               (assoc ctx :edn-body (read-inputstream-edn body))
               ctx))))

(def handler
  (-> cheep
      parse-edn-body))
