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


