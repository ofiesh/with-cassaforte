(ns with-cassaforte.core-test
  (:require [clojure.test :refer :all]
            [with-cassaforte.core :refer :all]))

(deftest test-get-conn-map
  (testing "get connection"
    (is (= {:host "localhost" :port "123" :keyspace "keyspace"}
           (get-conn-map "localhost" "123" "keyspace")))))

(deftest a-test
  (testing "FIXME, I fail."
    (is (= 0 1))))
