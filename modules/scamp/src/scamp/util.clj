(ns scamp.util
  (:require [schema.core :as s]))

(defn concatv [coll & colls]
  (vec (apply concat coll colls)))

(s/defn set-disclude :- #{s/Any}
  [s :- #{s/Any}
   to-remove :- s/Any]
  (into #{} (remove #(= % to-remove) s)))

(s/defn set-swap :- #{s/Any}
  [s :- #{s/Any}
   old new]
  (into #{} (map #(if (= % old) new %) s)))

(defn key-swap
  [node-neighbors old new]
  {:pre [(map? node-neighbors)]
   :post [(map? %)]}
  (if-let [old-neighbor (node-neighbors old)]
    (-> node-neighbors
        (dissoc old)
        (assoc new old-neighbor))
    node-neighbors))

(defn map-map
  "Apply 'key-fn to each of the keys in 'm, and the 'val-fn to each of
  the vals in 'm."
  [m & {:keys [key-fn val-fn]
        :or {key-fn identity val-fn identity}}]
  (->> m
       (map (fn mm* [[k v]] [(key-fn k) (val-fn v)]))
       (into {})))
