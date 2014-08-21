(ns sorting-challenge.core)

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
  (apply str (map #(apply str %) (take count data))))


(defn write-data!
  ([data] write-data! data "generative-data.txt")
  ([data filename] (spit filename data)))


(defn create-sample-data!
  [row-count filename]
  "Create a file of randomly generated rows of data"
  (parse-data-to-rows row-count (data-seq row-count)))


(defn parse-sample-data [data]
  "Parse sample data into a sequence of tuples"
  (for [col (map #(clojure.string/split % #" ")
                 (clojure.string/split data #"\n"))]
    (map #(Integer. %) col)))


(defn read-data [filename] (slurp filename))


(defn find-top-rows [n data]
  "Sort sequence of tuples and return n items"
  (take n (sort-by last > data)))


(defn run [total top filename]
  (let [data (parse-data-to-rows total (data-seq))]
    (write-data! data filename)
    (find-top-rows top (parse-sample-data (read-data filename)))))


(run 100 10 "test-data.txt")
