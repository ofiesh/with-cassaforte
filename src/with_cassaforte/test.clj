(ns with-cassaforte.test
  (:require [with-cassaforte.core :refer :all]
            [clojurewerkz.cassaforte.cql :as cql]
            [clojurewerkz.cassaforte.query :refer [column-definitions]]))

(defn with-table [table & data]
  (println  (column-definitions (:column-definitons table))) 
  (create-table (:name table) (column-definitions (:column-definitions table)))
  (doseq [d data]
    (insert (:name table) d)))

(defmacro test-with-connection [host & forms]
  (let [host-name (:host host)
        keyspace (:keyspace host)]
    `(with-connection
       {:host ~host-name :port ~(:port host)}
       (if (describe-keyspace ~keyspace)
         (throw (Exception. "Keyspace already exists")))
       (try
         (create-keyspace ~keyspace (cql/with 
 {:replication
                                     {"class" "SimpleStrategy"
                                      "replication_factor" 1}}))
         (use-keyspace ~keyspace)
         ~@forms
         (catch Exception e#
           (drop-keyspace ~keyspace)
           (throw e#)))
       (drop-keyspace ~keyspace))))


(def foo {:name :bar
          :column-definitions {:id :text
                               :name :text
                               :primary-key [:id]}})

(test-with-connection
 {:host "127.0.0.1" :keyspace "test_keyspace"}
 (with-table foo {:id "1" :name "abc"} {:id "2" :name "xyz"})
 (println (select :bar)))
