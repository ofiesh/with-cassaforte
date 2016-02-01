(ns with-cassaforte.core
  (:require [clojurewerkz.cassaforte.client :as client]
            [clojurewerkz.cassaforte.cql :as cql]
            [clojurewerkz.cassaforte.query :as query]))

(def ^:dynamic *conn* nil)

(defn get-conn [host keyspace port]
  (client/connect [host] {:keyspace keyspace
                          :port (if (nil? port) 9042 port)}))

(defmacro with-connection
  [host  & forms]
  `(with-bindings {#'*conn*
                   (~get-conn ~(:host host) ~(:keyspace host) ~(:port host))}
     (do ~@forms)))

(defn select [& query-params]
  (apply cql/select (cons *conn* query-params)))
(defn update [& query-params]
  (apply cql/update (cons *conn* query-params)))
(defn insert [& query-params]
  (apply cql/insert (cons *conn* query-params)))
(defn delete [& query-params]
  (apply cql/delete (cons *conn* query-params)))
(defn describe-keyspace [ks]
  (cql/describe-keyspace *conn* ks))
(defn describe-table [ks table]
  (cql/describe-table *conn* ks table))
(defn describe-columns [ks table]
  (cql/describe-columns *conn* ks table))
(defn create-keyspace [& query-params]
  (apply cql/create-keyspace (cons *conn* query-params)))
(defn create-table [& query-params]
  (apply cql/create-table (cons *conn* query-params)))
(defn alter-table [& query-params]
  (apply cql/alter-table (cons *conn* query-params)))
(defn drop-table [table]
  (cql/drop-table *conn* table))
(defn drop-keyspace [keyspace]
  (cql/drop-keyspace *conn* keyspace))
(defn use-keyspace [keyspace]
  (cql/use-keyspace *conn* keyspace))


(defn describe-tables
  [keyspace]
  (select 
   (query/columns :columnfamily_name)
   (query/from :system :schema_columnfamilies)
   (query/where {:keyspace_name keyspace})))
