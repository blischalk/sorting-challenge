(ns sorting-challenge.data-search
  (:require [iota :as iota]
            [clojure.core.reducers :as red])
  (:import (java.io BufferedReader FileReader)))

(defn revive [line]
  (-> line
      (clojure.string/replace #"\n" "")
      (clojure.string/split #" ")))


(defn sort-func [coll]
  (sort-by (juxt last first) coll))


(defn map-func
  ([top a b]
     (take top (reverse (sort-func (conj a b))))))


(defn merge-func
  ([top] [])
  ([top a b]
     (take top (reverse (sort-func (concat a b))))))

;(red/fold merge-func map-func (take 1000000 (repeatedly #(identity [1 2]))))

(defn find-in-parallel [filename top]
  (->> (iota/seq filename)
       (red/map revive)
       (red/fold (partial merge-func top) (partial map-func top))))


(defn serial-line-func
  [top coll line]
  (let [[id val] (-> line
                     (clojure.string/replace #"\n" "")
                     (clojure.string/split #" "))
        appended (conj coll [id val])]
    (take top (reverse (sort-func appended)))))


(defn find-in-serial [filename top]
  (with-open [rdr (BufferedReader. (FileReader. filename))]
    (reduce (partial serial-line-func top) [] (line-seq rdr))))


(defn find-top-rows
  ([filename top in-parallel]
     (if in-parallel
       (find-in-parallel filename top)
       (find-in-serial filename top))))
