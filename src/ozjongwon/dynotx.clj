;;;;   -*- Mode: clojure; encoding: utf-8; -*-
;;
;; Copyright (C) 2014 Jong-won Choi
;;
;; Distributed under the Eclipse Public License, the same as Clojure.
;;
;;;; Commentary:
;;
;;
;;
;;;; Code:
(ns ozjongwon.dynotx
  "Clojure DynamoDB Transaction - idea from https://github.com/awslabs/dynamodb-transactions"
  {:author "Jong-won Choi"}
  (:require [ozjongwon.dynolite  :as dl]))


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


(def ^:private attribute-name-map
  {:txid "_TxId"     :transient "_TxT"
   :date "_TxD"      :applied "_TxA"
   :requests "_TxR"  :state "_TxS"
   :version "_TxV"   :finalized "_TxF"
   :image-id "_TxI"})


;; Public API
(defn init-tx [tx-table-name image-table-name & opts]
  [(apply dl/ensure-table tx-table-name [(:txid attribute-name-map) :s] opts)
   (apply dl/ensure-table image-table-name [(:image-id attribute-name-map) :s] opts)])

;;; DYNOTX.CLJ ends here
