(ns graftwerk.shape.shape-support
  (:require [ grafter.tabular.common :refer [read-dataset ]]
            )
  (:import [net.datagraft.shape GeoShapeFromZip ]
          )
  )
(defn read-shape-csv [data-path]
  (read-dataset data-path :format :csv :separator \;)
  )

(defn convert-shape [shape-path]
  (let [zipFile (GeoShapeFromZip. shape-path)]
     (.writeCSV zipFile )))
