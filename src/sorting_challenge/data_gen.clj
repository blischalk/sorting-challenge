(ns sorting-challenge.data-gen
  (:require [clojure.java.io :as io]))

(def default-filename
  "generative-data.txt")


(defn data-seq
  ([] (data-seq 20))
  ([max-rand]
     "Creates columns of data with an auto incrementing key
      and a random number"
     (partition 4 (interleave (iterate inc 1)
                              (repeatedly #(identity \space))
                              (repeatedly #(rand-int max-rand))
                              (repeatedly #(identity \newline))))))


(defn parse-data-to-rows [count data]
  "Convert an infinate sequence of data rows to
   finite sequence of strings"
  (map #(apply str %) (take count data)))


(defn write-data!
  ([data] write-data! data default-filename)
  ([data filename] (do (io/delete-file filename true)
                       (doseq [line data]
                         (spit filename line :append true)))))
