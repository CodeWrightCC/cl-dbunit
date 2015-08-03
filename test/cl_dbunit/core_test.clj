(ns cl-dbunit.core-test
  (:use midje.sweet)
  (:use [cl-dbunit.core]))

(facts "about `deftestdb`"
  (fact "it creates a JdbcDatabaseTester"
    (.getName (.getClass (deftestdb {:url "jdbc:postgresql://localhost/messing2", :user "postgres" :password ""}))) => "org.dbunit.JdbcDatabaseTester"
  ))

(facts "about 'load_dataset'"
  (fact "it runs"
    (load_dataset (deftestdb {:url "jdbc:postgresql://localhost/messing", :user "postgres" :password ""}) org.dbunit.operation.DatabaseOperation/CLEAN_INSERT "dbunit/load.xml") => nil))

(facts "about 'clean_load'"
  (fact "it runs"
    (clean_load (deftestdb {:url "jdbc:postgresql://localhost/messing", :user "postgres" :password ""}) ["dbunit/load.xml"]) => [nil])
       )

(facts "about 'delete_all'"
  (fact "it runs"
    (delete_all (deftestdb {:url "jdbc:postgresql://localhost/messing", :user "postgres" :password ""}) ["dbunit/load.xml"]) => [nil]))

(facts "about 'refresh'"
  (fact "it runs"
    (refresh (deftestdb {:url "jdbc:postgresql://localhost/messing", :user "postgres" :password ""}) ["dbunit/load.xml"]) => [nil]))


(facts "about 'load_resource'"
  (fact "it returns an inputStream if the resource is found"
    (instance? java.io.InputStream (find_resource "dbunit/load.xml")) => true)
  (fact "it returns nil if the resource is NOT found"
    (find_resource "dbunit/doesnt_exist.xml") => nil)
       )
