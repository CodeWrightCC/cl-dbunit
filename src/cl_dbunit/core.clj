(ns cl-dbunit.core)

(import [org.dbunit.JdbcDatabaseTester])
;(require '(clojure.java.io :as io))


(defn deftestdb [args]
  (let [{:keys [url user password]} args]
    (new org.dbunit.JdbcDatabaseTester "org.postgresql.Driver" url user password)
  ))

(defn find_resource [url]
  (let [res (clojure.java.io/resource url)]
    (if (nil? res) nil (clojure.java.io/input-stream res))))

(defn load_dataset [testdb operation url]
  (let [dataSet (.build (new org.dbunit.dataset.xml.FlatXmlDataSetBuilder) (find_resource url))]
    (let [dataSetWithReplacements (new org.dbunit.dataset.ReplacementDataSet dataSet)]
        (.addReplacementObject dataSetWithReplacements "[NULL]" nil)
        (.setDataSet testdb dataSetWithReplacements)
        (.setSetUpOperation testdb operation)
        (let [conn (.getConnection testdb)]
          (.close conn)
          (.onSetup testdb)
  ))))

(defn clean_load [testdb urls]
;  (apply map #(load_dataset testdb org.dbunit.operation.DatabaseOperation/CLEAN_INSERT %) urls))
  (map #(load_dataset testdb org.dbunit.operation.DatabaseOperation/CLEAN_INSERT % ) urls))

(defn delete_all [testdb urls]
  (map #(load_dataset testdb org.dbunit.operation.DatabaseOperation/DELETE_ALL %) urls))

(defn refresh [testdb urls]
  (map #(load_dataset testdb org.dbunit.operation.DatabaseOperation/REFRESH %) urls))
