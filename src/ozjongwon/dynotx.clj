;;;;   -*- Mode: clojure; encoding: utf-8; -*-
;;
;; Copyright (C) 2014 Jong-won Choi
;;
;; Distributed under the Eclipse Public License, the same as Clojure.
;;
;;;; Commentary:
;;      https://github.com/awslabs/dynamodb-transactions/blob/master/DESIGN.md
;;
;;      * Atomicity - multi-phase commit protocol
;;      * Isolation
;;
;;
;;;; Code:
(ns ozjongwon.dynotx
  "Clojure DynamoDB Transaction - idea from https://github.com/awslabs/dynamodb-transactions"
  {:author "Jong-won Choi"}
  (:require [ozjongwon.dynolite  :as dl]
            [ozjongwon.dynohub  :as dh])
  (:import  [com.amazonaws.services.dynamodbv2.model
             ConditionalCheckFailedException]
            [java.util UUID]))


    ;; public enum State {
    ;;     PENDING,
    ;;     COMMITTED,
    ;;     ROLLED_BACK
    ;; }


    ;; public enum IsolationLevel {
    ;;     UNCOMMITTED,
    ;;     COMMITTED,
    ;;     READ_LOCK // what does it mean to read an item you wrote to in a transaction?
    ;; }

;; As in Amazon's code & doc
(def ^:private ^:constant +txid+  "Primary key, UUID"
  "_TxId")
(def ^:private ^:constant +state+ "pending -> commited or rolled-back"
  "_TxS")
(def ^:private ^:constant +requests+ "list of items participating in the transaction with unique IDs"
  "_TxR")
(def ^:private ^:constant +version+ "a version number storing for detecting concurrent changes to a TX record."
  "_TxV")
(def ^:private ^:constant +finalized+ "_TxF")
(def ^:private ^:constant +image-id+ "_TxI")
(def ^:private ^:constant +applied+ "indicating whether the transaction has performed the write to the item yet."
  "_TxA")
(def ^:private ^:constant +transient+ "indicating if the item was inserted in order to acquire the lock."
  "_TxT")
(def ^:private ^:constant +date+   "indicating approximately when the item was locked."
 "_TxD")
(def ^:private ^:constant +pending+ "P")
(def ^:private ^:constant +committed+ "C")
(def ^:private ^:constant +rolled-back+ "R")

(def ^:private ^:constant special-attributes
  #{+txid+ +state+ +requests+ +version+ +finalized+ +image-id+ +applied+ +transient+ +date+})

(defmacro with-tx-attributes [[] & body]
  `(binding [dh/*special-enums* ~special-attributes]
     ~@body))

;;;
(defn- make-txid []
  (str (UUID/randomUUID)))

(defn- get-current-time []
  (quot (System/currentTimeMillis) 1000))

;;;
;;; Transaction
;;;
(defn- insert-and-return-tx-item [tx-table txid]
  (let [tx-item {+txid+ txid +state+  "P" +version+ 1 +date+ (get-current-time)}]
    (try (with-tx-attributes []
           (dl/put-item tx-table
                        tx-item
                        :expected {+txid+ false +state+ false}))
         (catch ConditionalCheckFailedException e
           (throw (ex-info "Failed to create new transaction with id " {:txid txid :exception e}))))
    tx-item))

(defn- transaction-item? [item]
  (contains? item +txid+))

(def ^:private tx-table-name (atom "_tx_table_"))
(def ^:private image-table-name (atom "_image_table_"))
(defn- set-tx-table-name [name]
  (reset! tx-table-name name))
(defn- set-image-table-name [name]
  (reset! image-table-name name))


;;
;; Public API
;;
(defn init-tx [& opts]
  [(apply dl/ensure-table @tx-table-name [+txid+ :s] opts)
   (apply dl/ensure-table @image-table-name [+image-id+ :s] opts)])

(defn get-tx-item [tx-table txid]
  (with-tx-attributes []
    (let [tx-item (dl/get-item tx-table {:txid txid} :consistent? true)]
      (if (empty? tx-item)
        (throw (ex-info "Transaction not found id " {:txid txid}))
        tx-item))))

;;(def ^:dynamic *current-txid*)


(defn begin-transaction []
  (insert-and-return-tx-item @tx-table-name (make-txid)))


;;; DYNOTX.CLJ ends here

;;(init-tx)
;;(begin-transaction)
;;(dl/get-item @tx-table-name {+txid+ "3ede573e-0087-4ddf-bd0b-37571cbd6b6c"})