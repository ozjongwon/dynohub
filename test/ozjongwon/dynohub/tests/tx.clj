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

(deftest update-tx-test
  (testing "Updates in a transaction"
    (setup-db)
    (dt/put-item test-table {:id "id1" :n 1})
    (dt/with-transaction []
      (dt/update-item test-table {:id "id1"} {:n [:add 1]} :expected {:n 1} :return :all-new))
    (is (empty? (dl/scan :_tx_table_)))
    (is (empty? (dl/scan :_image_table_)))
    (is (= (:n (dt/get-item test-table {:id "id1"} :attrs [:n]))
           2))))

(deftest conflicting-updates-tx-test
  (testing "Conflicting updates in two transactions"
    (setup-db)
    (dt/put-item test-table {:id "id1" :n 1})
    (dt/with-transaction []
      (let [return (dt/update-item test-table {:id "id1"} {:n [:add 1]} :expected {:n 1} :return :all-new)]
        (is (= (:n return) 2)))
      (dt/with-transaction []
        (let [return (dt/update-item test-table {:id "id1"} {:n [:add 2]} :expected {:n 1} :return :all-old)]
          (is (= (:n return) 1)))))
    (is (empty? (dl/scan :_tx_table_)))
    (is (empty? (dl/scan :_image_table_)))
    (is (= (:n (dt/get-item test-table {:id "id1"} :attrs [:n]))
           3))))

(deftest delete-tx-test
  (testing "Delete in a transaction"
    (setup-db)
    (dt/put-item test-table {:id "id1" :n 1})
    (dt/with-transaction []
      (let [return (dt/delete-item test-table {:id "id1"} :return :all-old)]
        (is (= (:n return) 1))))
    (is (empty? (dl/scan :_tx_table_)))
    (is (empty? (dl/scan :_image_table_)))
    (is (empty? (dl/scan test-table)))))

(deftest conflicting-deletes-tx-test
  (testing "Conflicting deletes in two transactions"
    (setup-db)
    (dt/put-item test-table {:id "id1" :n 1})
    (dt/with-transaction []
      (let [return (dt/delete-item test-table {:id "id1"} :return :all-old)]
        (is (= (:n return) 1)))
      (dt/with-transaction []
        (let [return (dt/update-item test-table {:id "id1"} {:n [:add 2]} :expected {:n 1} :return :all-old)]
          (is (= (:n return) 1)))))
    (is (empty? (dl/scan :_tx_table_)))
    (is (empty? (dl/scan :_image_table_)))
    (is (= (:n (dt/get-item test-table {:id "id1"} :attrs [:n]))
           3))))

;;; TX.CLJ ends here
