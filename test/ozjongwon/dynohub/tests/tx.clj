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

(with-transaction [t1]
  (tx/put-item example-table-name {:item-id "Item1"} :tx @t1)
  (tx/put-item example-table-name {:item-id "Item2"} :tx @t1)
  (commit @t1)
  (delete @t1))

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
