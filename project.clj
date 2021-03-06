(defproject cl-dbunit "0.0.1-SNAPSHOT"
  :description "Thin wrapper around DbUnit"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [korma "0.4.2"]
                 [org.dbunit/dbunit  "2.5.1"]
                 [org.postgresql/postgresql "9.3-1101-jdbc41"]]
  :profiles {:dev {:dependencies [[midje "1.5.1"]
                                  [com.h2database/h2 "1.4.187"]]}
             :test {:dependencies [[com.h2database/h2 "1.4.187"]]}})

