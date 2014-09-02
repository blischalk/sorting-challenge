(ns sorting-challenge.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [sorting-challenge.data-gen :as data-gen]
            [sorting-challenge.data-search :as data-search])
  (:gen-class))


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
              (data-gen/write-data! (data-gen/parse-data-to-rows
                            rows
                            (data-gen/data-seq rows)) filename)
              (time (let [result (data-search/find-top-rows
                                  filename
                                  top
                                  parallel)]
                      (println "Your result is:")
                      (println result))))))))

; Normal Reduce Timed
; "Elapsed time: 93600.558 msecs"

; Reducer Fold Timed
; Elapsed time: 10792.479 msecs"
