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
            [ozjongwon.dynotx :as dt]))

(def test-table :tx-ex)

(defn- setup-db []
  (dl/delete-table   :_image_table_)
  (dl/delete-table   :_tx_table_ )
  (dl/delete-table   test-table)
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

(dt/with-transaction [t1]
  (dt/put-item test-table {:id "Item1"})
  (dt/put-item test-table {:id "Item2"})
  (dt/commit t1)
  (dt/delete t1))

(with-transaction [t1]
  (tx/put-item example-table-name {:item-id "conflictingTransactions_Item1" "WhichTransaction?" "t1"} :tx @t1)
  (tx/put-item example-table-name {:item-id "conflictingTransactions_Item2"} :tx @t1)
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
