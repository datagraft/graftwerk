(defproject scalable-graftwerk "0.1.0-SNAPSHOT"

  :url "http://grafter.org/"
  :description "FIXME: write description"

  :license {:name "Eclipse Public License - v1.0 (c) 2016 Swirrl IT Ltd"
            :url "https://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"local" ~(str (.toURI (java.io.File. "maven_repository")))}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [data-graft/sparker_2.10 "0.1.0-SNAPSHOT"]
                 [ring-server "0.3.1"]
                 [compojure "1.4.0"]
                 [environ "1.0.0"]
                 [com.taoensso/timbre "4.0.1"]
                 [bouncer "0.3.2"]
                 [ring/ring-defaults "0.1.3"]
                 [ring/ring-session-timeout "0.1.0"]
                 [ring-middleware-format "0.6.0"]
                 [selmer "0.8.0"]
                 [prone "0.8.0"]
                 [noir-exception "0.2.3"]
                 [clojail "1.0.6"]
                 [org.clojure/data.csv "0.1.3"]
                 [ww-geo-coords "1.0"]
                 ]

  ;:min-lein-version "0.1.0-SNAPSHOT"

  :jvm-opts ["-Xmx1024m" "-Djava.security.policy=.java.policy"  "-Djava.security.manager" ]
  ;:jvm-opts [ "-Xmx1024m"]

  ;TODO: need to remove this and compile carefully only for flambo api classes
  :aot :all
  :main graftwerk.core
  :repl-options {:init-ns graftwerk.core
                 :init (-main)
                 :timeout 60000}
  :uberjar-name "scalable-graftwerk.jar"


  :plugins [[lein-ring "0.9.1"]
            [lein-environ "1.0.0"]
            [lein-ancient "0.6.0"]]

  :ring {:handler graftwerk.handler/app
         :init    graftwerk.handler/init
         :destroy graftwerk.handler/destroy
         :uberwar-name "scalable-graftwerk.war"}




  :profiles {:uberjar {:aot :all}
             :provided {:dependencies
                        [[org.apache.spark/spark-core_2.10 "1.6.0"]
                         [org.apache.spark/spark-sql_2.10 "1.6.0"]
                         [com.databricks/spark-csv_2.10 "1.3.0"]]}

             }





  )
