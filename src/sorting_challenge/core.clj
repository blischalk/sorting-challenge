(ns sorting-challenge.core
  (:require [clojure.java.io :as io]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.core.reducers :as red]
            [iota :as iota])
  (:import (java.io BufferedReader FileReader))
  (:gen-class))

; Create a file with 35000 records
; Each record with an incrementing id
; And a random value between 1 35000
;
; ex.
; 1 5
; 2 15
; 3 2
; 4 19
; etc.

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

; Constantly (constantly \space)  == (repeatedly #(identity \space))


(defn parse-data-to-rows [count data]
  "Convert an infinate sequence of data rows to
   finite sequence of strings"
  (map #(apply str %) (take count data)))


(defn write-data!
  ([data] write-data! data default-filename)
  ([data filename] (do (io/delete-file filename true)
                       (doseq [line data]
                         (spit filename line :append true)))))


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


(def cli-options
  ;; Generate a data file
  [["-g" "--generate" "Generate a data file"]
   ;; Set the desired results to search in file
   ["-t" "--top TOP" "Top n results"
    :default 5]
   ;; Select how many rows of data to generate
   ["-r" "--rows ROWS" "Number of rows of data to generate"
    :default 1000
    :parse-fn #(Integer/parseInt %)]
   ;; Search the data file in parallel
   ["-p" "--parallel" "Search data in parallel"]
   ;; Help
   ["-h" "--help"]
   ])


(defn -main [& args]
  (let [opts (parse-opts args cli-options)
        options (:options opts)
        help (:help options)
        rows (:rows options)
        top  (:top options)
        parallel (:parallel options)
        filename (first (:arguments opts))]

    (if help (println (:summary opts))
        (do (if (:generate options)
              (write-data! (parse-data-to-rows rows (data-seq rows)) filename)
              (time (let [result (find-top-rows filename top parallel)]
                      (println "Your result is:")
                      (println result))))))))

; Normal Reduce Timed
; "Elapsed time: 93600.558 msecs"

; Reducer Fold Timed
; Elapsed time: 10792.479 msecs"
