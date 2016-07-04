(ns graftwerk.routes.evaluate
  (:require [compojure.core :refer [defroutes POST]]
            [graftwerk.validations :refer [if-invalid validate-pipe-run-request]]
            [clojure.string :refer [trim blank?]]
            [clojail.core :refer [safe-read]]
            [clojure.data.json :refer [read-json read-str]]
            [clojure.data.csv :refer [read-csv]]
            [graftwerk.wrapper.transformation.common :refer [get-columns save-as-json save-as-csv take-rows]]
            [clojure.edn :as edn]
            [taoensso.timbre :as log]
            [clojure.java.io :as io]
            [cheshire.core :as jsonches])
  (:import [java.io FilePermission]
           [java.util PropertyPermission]
           )
  )

(def result-dir "Result")

(def default-page-size "50")

(def default-namespace-declaration
  '(ns graftwerk.pipeline

     (:require [graftwerk.wrapper.transformation.common :refer :all]
               [clojure.string :refer [capitalize lower-case upper-case trim trim-newline triml trimr]]
               )
     )
  )

(defn- user-defs
  "Find get a set of all the symbols of vars defined in a namespace."
  [nspace] (set (keys (ns-interns nspace))))

(defn- bulk-unmap
  "Unmap a bunch of vars."
  [nspace vars]
  (doseq [n vars]
    (binding [*ns* nspace]
      (eval `(ns-unmap *ns* '~n)))))

(defn read-pipeline
  "Takes a ring style multi-part form map that contains a file reference to
  a :tempfile, reads the s-expressions out of the file and returns it wrapped in
  a '(do ...) for evaluation."
  [pipeline]

  (let [code (-> pipeline :tempfile slurp)]

    ;; FUGLY hack beware!!!
    ;; TODO clean this up! try with edn/read-string
    (safe-read (str "(do "
                    code
                    ")"))))

(defn namespace-symbol
  "Return the namespace name for the supplied namespace form"
  [ns-form]
  (second ns-form))

(defn namespace-declaration []
  (let [requires (try (edn/read-string (slurp "namespace.edn"))
                      (catch java.io.FileNotFoundException ex
                        default-namespace-declaration))]
    requires))

(defn execute-pipeline [data command pipeline & opt]
  "Takes the data to operate on (a ring file map) a command (a
  function name for a pipe or graft) and a pipeline clojure file and
  returns a Grafter dataset."
  (let [forms (read-pipeline pipeline)
        command (symbol command)
        data-file (-> data :tempfile .getPath)
        namespace-form (namespace-declaration)
        ;sandbox (build-sandbox forms data-file)
        ]
    ; by-passing sandbox due to hard context restrictions.
    ; TODO Can try to validate user code against tester before execution.
    ; but this can only be an emphirical security.
    (let [nspace (create-ns (namespace-symbol namespace-form))]
      (binding [*ns* nspace]
        (when true (clojure.core/refer-clojure))
        (eval namespace-form)
        (try (eval forms)
            (eval (list command data-file))
        (finally (let [ old-defs (user-defs nspace)
                       ]
                   (bulk-unmap nspace old-defs )
                   )))
        ))
    ;(evaluate-command sandbox command data-file (:filename data) opt)
    )
  )

(defn paginate
  "Paginate the supplied dataset."
  [ page page-size]
    (if (and page (not (empty? page)))
      (let [page-number (Integer/parseInt page)
            page-size (Integer/parseInt (or page-size default-page-size))]
        (log/info "Paging results " page-size " per page.  Page #" page-number)
        [(* page-number page-size) (* (inc page-number) page-size )]
        )
      ))

(defn dataset->key-map-format [[dataset columns] page page-size ]
  (let [lazy-data (with-open [in-file (io/reader dataset)]
               (doall
                 (read-csv in-file)))
        rows  (map (fn [row]
                     (zipmap columns row ))  lazy-data)     ;converts output to zipmap
        output-data {:column-names columns :rows rows}
        ]
    output-data
    )
  )

(defn dataset? [body]
  (instance? org.apache.spark.sql.DataFrame body)
  )

(defn transform-to-key-mapped-dataset [data-set page page-size]
  (cond (dataset? data-set)
    (let [columns (get-columns data-set)
          [from to] (paginate page page-size)
          dataset-path (save-as-csv  (take-rows data-set from to) result-dir)
          data-to-return (dataset->key-map-format [dataset-path columns] page page-size)]
      data-to-return
      ))
  )

(defroutes pipe-route
           (POST "/evaluate/pipe" {{:keys [pipeline data page-size page command ] :as params} :params}
             (if-invalid [errors (validate-pipe-run-request params)]
                         {:status 422 :body errors}
                         {:status 200 :body
                                  (-> data
                                      (execute-pipeline command pipeline)
                                      (transform-to-key-mapped-dataset page page-size)
                                      ;(paginate page-size page)
                                      )

                          })))


;(defroutes graft-route
;           (POST "/evaluate/graft" {{:keys [pipeline data command row constants delimiter sheet-name] :as params} :params}
;                 (if-invalid [errors (validate-graft-run-request params)]
;                             {:status 422 :body errors}
;                             (if-let [row (and (not (blank? row)) (Integer/parseInt row))]
;                               {:status 200 :body
;                                        ;(preview-graft-with-row row data command pipeline (if (= "on" constants)
;                                        ;                                                              true
;                                        ;                                                              false) delimiter sheet-name)
;                                      (println "testing service")
;
;                                }
;                               {:status 200 :body
;                                        ;(execute-pipeline data command pipeline delimiter sheet-name)
;                                }))))