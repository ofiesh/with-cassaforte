(ns with-cassaforte.autoschema
  (:require [with-cassaforte.core :refer :all]
            [clojurewerkz.cassaforte.cql :as cql]
            [clojurewerkz.cassaforte.query :refer [from column-definitions add-column drop-column alter-column]]
            [clojure.set :refer [difference intersection]]))

(defn create-keyspace-if-not-exists [keyspace]
  (if (nil? (describe-keyspace keyspace))
    (create-keyspace keyspace (cql/with 
                                    {:replication
                                     {"class" "SimpleStrategy"
                                      "replication_factor" 1 }}))))

(defn transaction-types [keyspace tables]
  (let [schemas 
        (into #{}
              (map #(:columnfamily_name %) (describe-tables keyspace)))]
    {:add (difference tables schemas)
     :modify (intersection tables schemas)
     :drop (difference schemas tables)}))

(defn drop-tables [keyspace tables]
  (doseq [table tables]
    (drop-table (str keyspace "." table))))

(defn modify-table [keyspace table]
  (let [table-columns (remove #(= (first %) :primary-key) (:column-definitions table))
        schema-columns (describe-columns keyspace (:name table))
        schema-columns-map (into {} (map (fn [column] [(keyword (:column_name column)) column])) schema-columns)
        schema-keys (into #{} (keys schema-columns-map))
        table-column-set 
        (into #{} (map #(key %) table-columns))
        modify (intersection schema-keys table-column-set)
        add (difference table-column-set schema-keys)
        drop (difference schema-keys table-column-set)
        table-name (str keyspace "."  (-> table :name name))]
    (doseq [column drop] (alter-table table-name (drop-column column)))
    (doseq [column table-columns]
      (if (some #{(name (first column))} add)
        (alter-table table-name (apply add-column column))
        ;;add retype eventually, figure out renaming?
        ))
))

(defn autoschema [keyspace & tables]
  (create-keyspace-if-not-exists keyspace)
  (let [table-transactions (transaction-types keyspace (into #{} (map #(:name %) tables)))]
    (drop-tables keyspace (:drop table-transactions))
    (doseq [table tables]
      (if (some #{(:name table)} (:add table-transactions))
        (create-table (str keyspace "." (name (:name table))) (column-definitions (:column-definitions table)))
        (modify-table keyspace table)))))


(with-connection {:host "10.1.1.2"}
  (autoschema "foo"
              {:name :test
               :column-definitions
               {:name :text
                :abc :timeuuid
                :x :text
                :primary-key [:name]}}
              {:name "foobar"
               :column-definitions
               {:id :timeuuid
                :other_id :timeuuid
                :foobar :blob
                :primary-key [[:id] :other_id]}}))
