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
  (dl/ensure-table test-table [:id :s])
#_
  (with-transaction [t1]
    (put-item :tx-ex {:id "conflictingTransactions_Item1" :which-transaction? :t1})

    (put-item :tx-ex {:id "conflictingTransactions_Item2"})

    (with-transaction []
      (put-item :tx-ex {:id "conflictingTransactions_Item1" :which-transaction? :t2-win!})
      (try (commit t1)
           (catch ExceptionInfo e
             (sweep t1 0 0)
             (delete t1)))
      (put-item :tx-ex {:id "conflictingTransactions_Item3" :which-transaction? :t2-win!}))))

(deftest two-items-in-a-tx-test
  (testing "Two item in one transaction"
    (setup-db)
    (dt/with-transaction [t1]
      (dt/put-item test-table {:id "Item1"})
      (dt/put-item test-table {:id "Item2"})
      (dt/commit t1)
      (dt/delete t1))
    (is (empty? (dl/scan :_tx_table_)))
    (is (empty? (dl/scan :_image_table_)))
    (is (= (count (dl/scan test-table)) 2))))


(deftest conflicting-tx-test
  (testing "Two item in one transaction"
    (setup-db)
    (dt/with-transaction [t1]
      (dt/put-item test-table {:id "Item1"})
      (dt/put-item test-table {:id "Item2"})
      (dt/commit t1)
      (dt/delete t1))
    (is (empty? (dl/scan :_tx_table_)))
    (is (empty? (dl/scan :_image_table_)))
    (is (= (count (dl/scan test-table)) 2))))


(testing "Two item in one transaction"
  (setup-db)
  (dt/with-transaction [t1]
    (println "***1 " @dt/tx-map)
    (dt/put-item test-table {:id "conflictingTransactions_Item1" "WhichTransaction?" "t1"})
    (dt/put-item test-table {:id "conflictingTransactions_Item2"})
    (println "***2 " @dt/tx-map)
    (dt/with-transaction [t2]
      (println "***3 " @dt/tx-map)

      (dt/put-item test-table {:id "conflictingTransactions_Item1" "WhichTransaction?" "t2 - I win!"})
      (utils/ignore-errors (dt/commit t1))
      (dt/put-item test-table {:id "conflictingTransactions_Item3"})
      (println "***4 " @dt/tx-map)
    ;;  (println "*current-txid*
      (dt/commit t2)
      (dt/delete t2)
    (println "***5 " @dt/tx-map))
  (println "***6 " @dt/tx-map))


    (tx/get-item example-table-name {:item-id "conflictingTransactions_Item1"} :isolation-level :uncommited)


      (dt/commit t1)
      (dt/delete t1))
    (is (empty? (dl/scan :_tx_table_)))
    (is (empty? (dl/scan :_image_table_)))
    (is (= (count (dl/scan test-table)) 2))))


(with-transaction [t1]
  (with-transaction [t2]
    (tx/put-item example-table-name {:item-id "conflictingTransactions_Item1" "WhichTransaction?" "t2 - I win!"}
                 :tx @t2)
    (try (commit @t1)
         (catch TransactionRolledBackException _
           (delete t1)))
    (tx/put-item example-table-name {:item-id "conflictingTransactions_Item3"} :tx @t2)
    (commit @t2)
    (delete @t2)

    (tx/get-item example-table-name {:item-id "conflictingTransactions_Item1"} :isolation-level :uncommited)









;;; TX.CLJ ends here


image :  [{:_TxD 1404282052, :WhichTransaction? t1, :_TxI cc36d21e-2b17-4d7b-baa7-78862b30d0d6#1, :id conflictingTransactions_Item1, :_TxId cc36d21e-2b17-4d7b-baa7-78862b30d0d6}]
tx :  [{:_TxD 1404282053, :_TxR #{{:item {WhichTransaction? t1, :id conflictingTransactions_Item1}, :op :put-item, :table :tx-ex} {:item {:id conflictingTransactions_Item2}, :op :put-item, :table :tx-ex}}, :_TxS R, :_TxF true, :_TxId bab74b6a-b948-4113-9dbf-ebdf88c436bd, :_TxV 3} {:_TxD 1404282053, :_TxR #{{:item {WhichTransaction? t2 - I win!, :id conflictingTransactions_Item1}, :op :put-item, :table :tx-ex} {:item {:id conflictingTransactions_Item3}, :op :put-item, :table :tx-ex}}, :_TxS P, :_TxId cc36d21e-2b17-4d7b-baa7-78862b30d0d6, :_TxV 3}]
data :  [{:id conflictingTransactions_Item2} {:_TxD 1404282054, :_TxA true, :id conflictingTransactions_Item3, :_TxT true, :_TxId cc36d21e-2b17-4d7b-baa7-78862b30d0d6} {:_TxD 1404282052, :WhichTransaction? t2 - I win!, :_TxA true, :id conflictingTransactions_Item1, :_TxId cc36d21e-2b17-4d7b-baa7-78862b30d0d6}]


image :  []
tx :  [{:_TxD 1404282053, :_TxR #{{:item {WhichTransaction? t1, :id conflictingTransactions_Item1}, :op :put-item, :table :tx-ex} {:item {:id conflictingTransactions_Item2}, :op :put-item, :table :tx-ex}}, :_TxS R, :_TxF true, :_TxId bab74b6a-b948-4113-9dbf-ebdf88c436bd, :_TxV 3}]
data :  [{:id conflictingTransactions_Item2} {:id conflictingTransactions_Item3} {:WhichTransaction? t2 - I win!, :id conflictingTransactions_Item1}]
