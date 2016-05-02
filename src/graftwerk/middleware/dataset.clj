(ns graftwerk.middleware.dataset
  (:require
    [clojure.pprint :refer [pprint]]
    [ring.middleware.format-response :refer [parse-accept-header]]
    [clojure.java.io :as io]
    [ring.util.mime-type :refer [default-mime-types]]
    [taoensso.timbre :as log]
    [ring.util.io :refer [piped-input-stream]]
    [clojure.data.csv :refer [write-csv read-csv]]
    )
  (:import [java.io OutputStream]
           )
  )

; the directory where the processed output will be created
(def result-dir "Result")
;(def result-file (str result-dir "/part-00000"))


;(defn dataset? [body]
;  (instance? org.apache.spark.sql.DataFrame body)
;  )

;(defn dataset->seq-of-seqs [[dataset column-str]]
;  (let [data (with-open [in-file (io/reader dataset)]
;               (doall
;                 (read-csv in-file)))
;        columns (clojure.string/split column-str #",")
;        rows  (map (fn [row]
;                        (zipmap columns row ))  data)
;        output-data {:column-names columns :rows rows}
;        ;stringified-rows (map (partial map str) output-data)
;        ]
;    output-data
;    )
;  )

(defmacro with-out-stream [ostream & body]
  `(binding [*out* ~ostream]
     ~@body))

(defn stream-edn [output dataset]
  ;(let [rows (dataset->seq-of-seqs dataset)]
    (with-out-stream output
                     (prn dataset))
  ;)
  )

;; It would be great if we could find a way so we don't have to do
;; this.  Essentially I'd like to use ring.middleware.format-response
;; or another middleware to negotiate the response format/type for us.
;; However the above middleware seems to assume that responses are
;; strings, where-as really we want to stream a response to the
;; client.
;;
;; I submitted a bug-report/feature-request here:
;;
;; See: https://github.com/ngrunwald/ring-middleware-format/issues/41
;;
;; NOTE: We also did something similar in drafter, so it would be good
;; to find a way to do this once and for all.
;;
;; But in the mean time I've just hacked this in, in the hope it'll be
;; simple enough and good enough; even if technically incorrect with
;; regards to content-neg.

(defn- select-content-type [types]
  (let [accepts (->> types
                     parse-accept-header
                     (sort-by :q)
                     reverse)
        {:keys [sub-type type]} (first accepts)
        ;; rebuild the object into a string... stupid I know...
        chosen (str type "/" sub-type)]
    (log/info "Selected format: " chosen)
    chosen))

(def ^:private mime-type->streamer {"application/edn" stream-edn
                                    ;"application/edn" stream-csv
                                    })

(defn- ->stream
  "Takes a dataset and a supported tabular format and returns a
  piped-input-stream to stream the dataset to the client."
  [dataset streamer]
  (piped-input-stream
    (fn [ostream]
      (try
        (with-open [writer (clojure.java.io/writer ostream)]
          (streamer writer  dataset))
        (log/info "Dataset streamed")
        (catch Exception ex
          (log/warn ex "Unexpected Exception whilst streaming dataset"))))))



(defn wrap-write-dataset [app]
  (fn write-dataset-middleware
    [req]
    (log/info "Received request" req)
    (try
      (let [response (app req)
            body (:body response)
            headers (:headers req)
            accepts (or (:accepts headers) "application/edn")
            selected-format (select-content-type accepts)]
        ;(println (str "testing " accepts))
        (log/info "Client sent accept headers:" accepts "selected " selected-format)
        ;(cond
          ;true
          (let [
                                dataset  body
                                ;dataset-path (.saveDataAsCsv transformer body result-dir)
                                selected-streamer (get mime-type->streamer selected-format)
                                ]
                            (-> response
                                (assoc :body (->stream dataset  selected-streamer))
                                (assoc-in [:headers "Content-Type"] selected-format)
                                (assoc-in [:headers "Content-Disposition"] "attachment; filename=\"results\"")))

          ;:else response)
        )
      (catch Exception ex
        (log/warn ex "Unknown error caught.  Returning 500 with stack trace")
        {:status  500
         :headers {"Content-Type" "application/edn"}
         :body    (prn-str {:type    :error
                            :message (.getMessage ex)
                            :class   (str (.getName (class ex)))})}))))









;
;(defn format-dataset-to-edn [body]
;  (let [columns (.getColumns transformer body)
;        data-string (.getRowColumnMap transformer body)
;        data-as-set {:column-names [columns] :rows data-string}
;        ] data-as-set
;
;          )
;  )