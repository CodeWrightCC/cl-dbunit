(ns cl-dbunit.core)

(import [org.dbunit.JdbcDatabaseTester])
;(require '(clojure.java.io :as io))

(defn spec->jdbc_url [spec]
  (str "jdbc:" (spec :subprotocol) ":" (spec :subname))
  )

(defn defdbunit [spec]
  (let [url (spec->jdbc_url spec), user (spec :user), password (spec :password), classname (spec :classname) ]
    (new org.dbunit.JdbcDatabaseTester classname url user password)
  ))

(defn execute_sql [db sql]
  (.execute (.createStatement (.getConnection (.getConnection db))) sql))

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

(defn verify [db table sql dataset_url]
  (let [connection (.getConnection db)
        actualTable (.createQueryTable connection table  sql)
        expectedDataSet (.build (new org.dbunit.dataset.xml.FlatXmlDataSetBuilder) (find_resource dataset_url))
        expectedTable (.getTable expectedDataSet table)
        expectedWithReplacements (new org.dbunit.dataset.ReplacementTable expectedTable)
                    ]
      (.addReplacementObject expectedWithReplacements "[NULL]" nil)
      (org.dbunit.Assertion/assertEquals  expectedWithReplacements actualTable)
    ))
