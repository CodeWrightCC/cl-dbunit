(ns cl-dbunit.core-test
  (:use midje.sweet)
  (:use [cl-dbunit.core]))

(use 'korma.db)


(facts "about 'defdbunit'"
  (fact "it returns a JdbcDatabaseTester instance"
    (.getName (.getClass (defdbunit (h2 {:db "mem:testcreate"})))) => "org.dbunit.JdbcDatabaseTester"))

(facts "about 'find_resource'"
  (fact "it returns an inputStream if the resource is found"
    (instance? java.io.InputStream (find_resource "dbunit/load.xml")) => true)
  (fact "it returns nil if the resource is NOT found"
    (find_resource "dbunit/doesnt_exist.xml") => nil))

(facts "about 'spec->jdbc_url'"
  (fact "it generates a valid JDBC url"
    (spec->jdbc_url (h2 {:db "mem:testcreate"})) => "jdbc:h2:mem:testcreate"))

(against-background [(before :contents  [(def testdb (eval (defdbunit (h2 {:db "mem:testing"}))))])
                     (after :contents [(.execute (.createStatement (.getConnection (.getConnection testdb))) "drop table sample_exec")])]
  (facts "about 'execute_sql'"
    (fact "it executes SQL against the given DB"
      (execute_sql testdb "create table sample_exec (id integer)") => false)))

(against-background [(before :contents  [
                        (def testdb (eval (defdbunit (h2 {:db "mem:testing"}))))
                        (execute_sql testdb "create table sample (id integer primary key, data varchar(50))")])
                     (after :contents [(execute_sql testdb "drop table sample")])]
  (facts "about 'load_dataset'"
    (fact "it runs"
      (load_dataset testdb org.dbunit.operation.DatabaseOperation/CLEAN_INSERT "dbunit/load.xml") => nil))

  (facts "about 'clean_load'"
    (fact "it runs"
      (clean_load testdb ["dbunit/load.xml"]) => [nil]))

  (facts "about 'delete_all'"
    (fact "it runs"
      (delete_all testdb ["dbunit/load.xml"]) => [nil]))

  (facts "about 'refresh'"
    (fact "it runs"
      (refresh testdb ["dbunit/load.xml"]) => [nil]))

  (facts "about 'verify'"
    (fact "it runs without exception when data is as expected"
      (verify testdb "sample" "select * from sample", "dbunit/expected.xml") => nil)
    (fact "it exceptions when data is not as expected"
      (verify testdb "sample" "select * from sample", "dbunit/expected_incorrect.xml") =>
        (throws junit.framework.ComparisonFailure "value (table=sample, row=0, col=data) expected:<[WrongValue]> but was:<[Moo]>"))))
