(ns with-cassaforte.autoschema
  (:require [with-cassaforte.core :refer :all]
            [clojurewerkz.cassaforte.cql :as cql]))





(with-connection {:host "10.1.1.2"}
  (autoschema "test-keyspace"
              {:table-name
               :column-definitions
               {:id :timeuuid
                :other_id :timeuuid
                :name :text
                :primary-key [[:id] :other_id]}}))
