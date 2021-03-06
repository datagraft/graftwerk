(ns graftwerk.middleware.dataset
  (:require [grafter.tabular :refer [make-dataset dataset? write-dataset]]
            [grafter.tabular.common :refer [write-dataset* dataset->seq-of-seqs]]
            [grafter.rdf :refer [s]]
            [clojure.pprint :refer [pprint]]
            [grafter.rdf.formats :refer :all]
            [grafter.rdf.io :refer [rdf-serializer]]
            [grafter.rdf.preview :refer [->printable-form]]
            [ring.middleware.format-response :refer [parse-accept-header]]
            [clojure.java.io :refer [output-stream]]
            [ring.util.mime-type :refer [default-mime-types]]
            [taoensso.timbre :as log]
            [ring.util.io :refer [piped-input-stream]]
            [clojure.data.csv :refer [write-csv]])
  (:import [java.io OutputStream]))

(defn stream-csv [output dataset]
  (let [rows (dataset->seq-of-seqs dataset)
        stringified-rows (map (partial map str) rows)]
    (write-csv output stringified-rows)))

(defmacro with-out-stream [ostream & body]
  `(binding [*out* ~ostream]
    ~@body))

(defn stream-edn [output dataset]
  (let [dataset (->printable-form dataset)
        rows (:rows dataset)
        cols (:column-names dataset)]
    (with-out-stream output
      (prn {:column-names cols :rows rows}))))

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

(def ^:private mime-type->streamer {"application/csv" stream-csv
                                    "application/edn" stream-edn})
(def ^:private mime-type->serialization-format {"application/n-triples" rdf-ntriples
                                                "application/rdf+xml" rdf-xml
                                                "text/turtle" rdf-turtle
                                                "application/n-quads"  rdf-nquads
                                                "text/n3" rdf-n3
                                                "application/trix" rdf-trix
                                                "application/trig" rdf-trig
                                                "application/ld+json" rdf-jsonld})
(defn map-values [f m]
  (let [kvs (map (fn [[k v]] [k (f v)]) m)
        keys (map first kvs)
        vals (map second kvs)]
    (zipmap keys vals)))

(defn serialise-grafter-s
  "The grafter s function returns reified Objects with anonymous types
  and needs special attention when serialised out again.

  This is a hack, which we should remove when we fix grafter.rdf/s to
  return something more friendly and serialisable."
  [ds]
  (let [reified-s-classes #{(class (s "foo"))
                            (class (s "foo" :blah))}]

    (make-dataset (map (partial map-values (fn [st]
                                             (if (reified-s-classes (class st))
                                               (str st)
                                               st)))
                       (:rows ds))
                  (:column-names ds))))

(defn- ->stream
  "Takes a dataset and a supported tabular format and returns a
  piped-input-stream to stream the dataset to the client."
  [dataset streamer]
  (piped-input-stream
   (fn [ostream]
     (try
       (with-open [writer (clojure.java.io/writer ostream)]
         (streamer writer (serialise-grafter-s dataset)))
       (log/info "Dataset streamed")
       (catch Exception ex
         (log/warn ex "Unexpected Exception whilst streaming dataset"))))))

(defn wrap-write-dataset [app]
  (fn write-dataset-middleware
    [req]
    (log/info "Received request" req)
      (log/info "***************app" app)
    (try
      (let [response (app req)
            body (:body response)
            accepts (get-in req [:headers "accept"] "application/edn")
            selected-format (select-content-type accepts)]

        (log/info "Client sent accept headers:" accepts "selected " selected-format)
        (cond
          (dataset? body) (let [dataset body
                                selected-streamer (get mime-type->streamer selected-format stream-edn)]

                            (-> response
                                (assoc :body (->stream dataset selected-streamer))
                                (assoc-in [:headers "Content-Type"] selected-format)
                                (assoc-in [:headers "Content-Disposition"] "attachment; filename=\"results\"")))

          (map? body) (do
                        (log/info "Responding with map body: " body)
                        (let [accepts (get-in req [:headers "accept"] "application/edn")
                              selected-format (select-content-type accepts)
                              printed-edn (java.io.StringWriter.)]

                          ;; Watch out coz this baby is mutable...
                          (pprint body printed-edn)
                          (-> response
                              (assoc :body (str printed-edn))
                              (assoc-in [:headers "Content-Type"] "application/edn"))))

          ;; If we're sequential assume we're streaming RDF
          (sequential? body) (do
                               (log/info "About to stream RDF")
                               (-> response
                                   (assoc-in [:headers "Content-Type"] selected-format)
                                   (assoc :body (piped-input-stream (fn [ostream]
                                                                      (try
                                                                        (with-open [writer (clojure.java.io/writer ostream)]
                                                                          (log/info "Serialising RDF")
                                                                          (grafter.rdf/add (rdf-serializer writer :format (mime-type->serialization-format  selected-format))
                                                                                           body))
                                                                        (catch Exception ex
                                                                          (log/warn ex "Unexpected exception whilst streaming RDF"))))))))

          :else response))
      (catch Exception ex
        (log/warn ex "Unknown error caught.  Returning 500 with stack trace")
        {:status 500
         :headers {"Content-Type" "application/edn"}
         :body (prn-str {:type :error
                         :message (.getMessage ex)
                         :class (str (.getName (class ex)))})}))))
