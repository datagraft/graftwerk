(ns graftwerk.wrapper.transformation.common

  (:require [taoensso.timbre :as log])
  (:import [net.datagraft.sparker.core InitSpark ]
           [net.datagraft.sparker.tabular TabularTransformer]
           [org.apache.spark.sql DataFrame]
           [scala.collection JavaConversions]
           [java.util HashMap]
           )
  )

(defonce transformer
         (->
           (InitSpark.)
           (.init)
           (TabularTransformer.)))

(defn to-scala-seq [coll]
  (-> coll JavaConversions/asScalaBuffer .toList))


(defn make-data-set
  ([data-path ]
   (.makeDataSet transformer data-path ))
  ([data-path sample-limit]
    (.makeDataSet transformer data-path true sample-limit)
    )
    )

(defn make-first-row-as-column [data-set]
  (.makeDataSetWithColumn transformer data-set)
  )
(defn make-data-set-with-columns
  ([data-set column-names]
   (.makeDataSet transformer data-set (to-scala-seq column-names)))
  ([data-set from to]
   (.makeDataSet transformer data-set to from))
  )

(defn fill-na-values [data-set na-value]
  (.fillNullValues transformer data-set na-value)
  )

(defn group-and-aggregate
  [data-set columns agg-map ]
  (.groupAndAggregate transformer data-set (to-scala-seq columns) (to-scala-seq agg-map))
  )

(defn remove-duplicates
  ([data-set]
   (let [df (.removeDuplicates transformer data-set)]
     df))
  ([data-set column-names]
   (let [df (.removeDuplicates transformer data-set (to-scala-seq column-names))]
     df))
  )

(defn pivot-data [data-set col-to-group pivot-col value-cols]
  ((.pivotDataSet transformer data-set (to-scala-seq col-to-group) pivot-col (to-scala-seq value-cols)))
  )

(defn sort-data
  [data-set cols-to-sort sorting-expr]
  (.sortDataSetWithColumnExpr transformer data-set (to-scala-seq cols-to-sort) (to-scala-seq sorting-expr))
  )

(defn add-column-with-functions
  [data-set col-name utility]
  (.addColumnWithFunctions transformer data-set col-name utility)
  )

(defn add-column-with-value
  [data-set col-name custom-value]
  (.addColumnWithValue transformer data-set col-name custom-value)
  )

(defn drop-columns
  ([data-set from to]
    (.dropColumn transformer data-set from to)
    )
  ([data-set col-names]
   (.dropColumn transformer data-set (to-scala-seq col-names))
    )
  )

(defn apply-on-column
  [data-set col-name func-exp]
  (.applyToColumn transformer data-set col-name (to-scala-seq func-exp))
  )

(defn derive-from-column
  [data-set col-name from func-exp]
  (.deriveColumn transformer data-set col-name (to-scala-seq from) (to-scala-seq func-exp))
  )

(defn split-column
  [data-set col-to-split separator ]
  (.splitColumn transformer data-set col-to-split separator)
  )

(defn rename-all-columns
  [data-set function]
  (.renameAllColumns transformer data-set function)
  )

(defn rename-column
  [data-set old-name new-name]
  (.renameColumn transformer data-set old-name new-name)
  )

(defn merge-column
  [data-set new-col-name cols-to-merge separator]
  (.mergeColumn transformer data-set new-col-name (to-scala-seq cols-to-merge) separator)
  )

(defn add-row
  [data-set row-values]
  (.addRow transformer data-set (to-scala-seq row-values))
  )

(defn take-rows [data-set from to]
  (.takeRows transformer data-set from to)
  )

(defn drop-rows [data-set from to]
  (.dropRows transformer data-set from to)
  )

(defn filter-rows
  [data-set cols-to-filter func-str expr-to-filter]
  (.filterRows transformer data-set (to-scala-seq cols-to-filter) func-str (to-scala-seq expr-to-filter))
  )

(def default-page-size "50")

(defn paginate-seq [results page-size page-number]
  (if (and page-number (not (empty? page-number)))
    (let [page-number (Integer/parseInt page-number)
          page-size (Integer/parseInt (or page-size default-page-size))]
      (log/info "Paging results " page-size " per page.  Page #" page-number)
      (->>  results
            (drop (* page-number page-size))
            (take page-size)))
    results))
(defn paginate-dataset [data-set page-size pg]
   (paginate-seq data-set page-size pg))

;(defn output-dataset
;  [data-set, output-path]
;  (.outputData transformer data-set output-path)
;  )
(defn print-test [data-set]
  (.show data-set)
  data-set
  )

(defn get-columns [data-set]
  (clojure.string/split (.getColumns transformer data-set) #",")
  )

(defn save-as-csv [ data-set data-path]
  (.saveDataAsCsv transformer data-set data-path)
  )

(defn save-as-json [data-set data-path]
  (.saveDataAsJson transformer data-set data-path)
  )