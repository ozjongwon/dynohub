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

(ns ozjongwon.dynohub.tests.tx
  (:require [clojure.test :as test :refer :all]
            [ozjongwon.dynolite :as dl]
            [ozjongwon.dynotx :as dt]
            [ozjongwon.utils :as utils]))

(def test-table :tx-ex)

(defn- setup-db []
  (utils/ignore-errors (dl/delete-table   :_image_table_))
  (utils/ignore-errors (dl/delete-table   :_tx_table_ ))
  (utils/ignore-errors (dl/delete-table   test-table))
  (dt/init-tx)
  (dl/ensure-table test-table [:id :s]))

(deftest two-items-in-a-tx-test
  (testing "Two items in one transaction"
    (setup-db)
    (dt/with-transaction [t1]
      (dt/put-item test-table {:id "Item1"})
      (dt/put-item test-table {:id "Item2"}))
    (is (empty? (dl/scan :_tx_table_)))
    (is (empty? (dl/scan :_image_table_)))
    (is (= (count (dl/scan test-table)) 2))))

(deftest two-items-explicit-commit-test
  (testing "Two items in one transaction"
    (setup-db)
    (dt/with-transaction [t1]
      (dt/put-item test-table {:id "Item1"})
      (dt/put-item test-table {:id "Item2"})
      (dt/commit t1))
    (is (empty? (dl/scan :_tx_table_)))
    (is (empty? (dl/scan :_image_table_)))
    (is (= (count (dl/scan test-table)) 2))))


(deftest conflicting-tx-test
  (testing "Conflicts in two transactions"
    (setup-db)
    (dt/with-transaction [t1]
      (dt/put-item test-table {:id "conflictingTransactions_Item1" :which-transaction? "t1"})
      (dt/put-item test-table {:id "conflictingTransactions_Item2"})
      (dt/with-transaction [t2]
        (dt/put-item test-table {:id "conflictingTransactions_Item1" :which-transaction? "t2 - I win!"})
        (utils/ignore-errors (dt/commit t1))
        (dt/put-item test-table {:id "conflictingTransactions_Item3"})))
    (is (empty? (dl/scan :_tx_table_)))
    (is (empty? (dl/scan :_image_table_)))

    (is (empty? (dl/get-item test-table {:id "conflictingTransactions_Item2"})))
    (is (not (empty? (dl/get-item test-table {:id "conflictingTransactions_Item3"}))))
    (is (= (get (dl/get-item test-table {:id "conflictingTransactions_Item1"}) :which-transaction?)
           "t2 - I win!"))))

;;; TX.CLJ ends here
