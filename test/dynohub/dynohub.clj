;;;;   -*- Mode: clojure; encoding: utf-8; -*-
;;
;; Copyright (C) 2014 Jong-won Choi
;; All rights reserved.
;;
;;;; Commentary:
;;
;;
;;
;;;; Code:
(ns ozjongwon.dynohub.tests.dynohub
  (:require [clojure.test :as test :refer :all]
            [ozjongwon.dynohub :as dh]
            [ozjongwon.dynolite :as dl]
;            [taoensso.encore  :as encore]
;            [taoensso.nippy   :as nippy]
            )
  #_
  (:import  [com.amazonaws.auth BasicAWSCredentials]
            [com.amazonaws.internal StaticCredentialsProvider]))

(defn- clear-all-tables []
  (doseq [t (dl/list-tables)]
    (dl/delete-table t)))

(deftest basic-table-tests
  (testing "Basic table tests"
    (let [new-opts {:access-key "test-accesskey" :secret-key "test-secretkey"
                    :endpoint "http://localhost:8000"}
          [table1 hash-keydef1]  [:test-table [:hash-keydef1 :s]]]

      (testing "default-client-opts updated?"
        (dl/set-default-client-opts new-opts)
        (is (= new-opts @dl/default-client-opts)))

      (testing "There are no tables after calling 'delete-table'"
        (clear-all-tables)
        (is (empty? (dl/list-tables))))

      (testing "create a simple table with hash-keydef only"
        (dl/create-table table1 hash-keydef1))

    (let [table-description (dl/describe-table table1)]

      (testing "Do all expected keys exist?"
        (is (=  #{:prim-keys :creation-date :gsindexes :item-count :lsindexes :throughput :name :size :status}
                (set (keys table-description)))))

      (testing "Check initial throughput"
        (is (= '(1 1) (map (:throughput table-description) [:read :write]))))

      (testing "Update throughput then check it after increasing throughput"
        (dl/update-table table1 {:write 30 :read 10})
        (is '(10 30) (map (:throughput (dl/describe-table table1)) [:read :write])))

      (testing "Decrease throughput"
        (dl/update-table table1 {:write 3 :read 1})
        (is '(1 3)  (map (:throughput (dl/describe-table table1)) [:read :write])))
    ))))

;;(create-table :site11 [:owner-email :s] :range-keydef [:name :s] :block? true :throughput {:read 1 :write 1})



;;; DYNOHUB.CLJ ends here
