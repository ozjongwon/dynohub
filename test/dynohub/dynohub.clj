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

(defonce test-opts {:access-key "test-accesskey" :secret-key "test-secretkey"
                    :endpoint "http://localhost:8000"})

(defmacro with-test-env [[table-name-def-map] & body]
  `(do
     (when (not= ~test-opts @dl/default-client-opts)
       (dl/set-default-client-opts ~test-opts))
     (clear-all-tables)
     (doseq [[name# def#] ~table-name-def-map]
       (apply dl/create-table name# def#))
     ~@body))

(deftest basic-table-tests
  (testing "Basic table tests - create, delete, list, update, describe"
    (let [[table1 hash-keydef1]  [:test-table [:hash-keydef1 :s]]]

      (testing "default-client-opts updated?"
        (dl/set-default-client-opts test-opts)
        (is (= test-opts @dl/default-client-opts)))

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
        (is (= '(10 30) (map (:throughput (dl/describe-table table1)) [:read :write]))))

      (testing "Decrease throughput"
        (dl/update-table table1 {:write 3 :read 1})
        (is (= '(1 3)  (map (:throughput (dl/describe-table table1)) [:read :write]))))

      (testing "Increase throughput to max-req = 5"
        ;; :read 32 needs 5 requests
        (dl/update-table table1 {:write 1 :read 32})
        (is (= '(32 1)  (map (:throughput (dl/describe-table table1)) [:read :write]))))

      (testing "Increase throughput to max-req < 6"
        ;; :write 64 needs 6 requests
        (is (thrown-with-msg? java.lang.AssertionError #"Got max-reqs "
              (dl/update-table table1 {:write 64 :read 1}))))

    ))))

(deftest basic-data-access
  (testing "Basic reading/writing data tests - put, batch-write, get, batch-get, query, scan"
    (with-test-env [{:country-state [[:country :s] :range-keydef [:state :s]]
                     :area-phone [[:area-number :n] :range-keydef [:phone-number :n]]}]
      (testing "Put items"
        (dl/put-item :country-state {:country "AU" :state "NSW" :suburb "Artarmon"})
        (let [scan-result (dl/scan :country-state)]
          (is (= 1 (count scan-result)))
          (is (= {:country "AU" :state "NSW" :suburb "Artarmon"} (first scan-result)))))

      (testing "batch-write with overwriting existing data"
        (dl/batch-write-item {:country-state {:put [{:country "AU" :state "NSW" :suburb "Chatswood"}
                                                    {:country "AU" :state "QLD" :suburb "CBD"}]}
                              :area-phone  {:put [{:area-number 1 :phone-number 1234 :name "Stig"}
                                                  {:area-number 1 :phone-number 2345 :name "Finn"}]}})
        (is (= 2 (count (dl/scan :country-state))))
        (is (= 2 (count (dl/scan :area-phone)))))

      (testing "batch-get"
        (let [batch-get-result (dl/batch-get-item {:country-state {:prim-kvs {:country "AU" :state ["NSW" "QLD"]}}
                                                   :area-phone    {:prim-kvs {:area-number [1] :phone-number [1234 2345]}}
                                                   })]
          (is (= 2 (count (:country-state batch-get-result))))
          (is (= 2 (count (:area-phone batch-get-result))))))

      (testing "query"
        (let [q1 (dl/query :country-state {:country [:eq "AU"] :state [:begins-with "N"]})
              q2 (dl/query :area-phone    {:area-number [:eq 1] :phone-number [:between [1 3000]]})
              q3 (dl/query :country-state {:country [:eq "AU"] :state [:gt "P"]})]
          (is (= 1 (count q1)))
          (is (= 2 (count q2)))
          (is (= 1 (count q3)))))
      )))


;;(create-table :site11 [:owner-email :s] :range-keydef [:name :s] :block? true :throughput {:read 1 :write 1})



;;; DYNOHUB.CLJ ends here
