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
            [ozjongwon.dynohub  :as dh]
            [ozjongwon.utils    :as utils])
  (:import  [com.amazonaws.services.dynamodbv2.model
             ConditionalCheckFailedException]
            [java.util UUID]))

;;;
;;; Utility
;;;
(defn- get-current-time []
  (quot (System/currentTimeMillis) 1000))


(defn- make-txid []
  (str (UUID/randomUUID)))

(defn- %table-keys [table]
  (keys (:prim-keys (dl/describe-table table))))

(defonce table-keys (utils/bounded-memoize %table-keys 64))

;;;
;;; Transaction-Item
;;;

;; constants
(def ^:private ^:constant item-lock-acquire-attempts 3)
(def ^:private ^:constant item-commit-attempts 2)
(def ^:private ^:constant tx-lock-acquire-attempts 2)
(def ^:private ^:constant tx-lock-contention-resolution-attempts 3)

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



;; (def ^:private ^:constant +pending+ "P")
;; (def ^:private ^:constant +committed+ "C")
;; (def ^:private ^:constant +rolled-back+ "R")

;; (def ^:private ^:constant +uncommitted+ "U")
;; (def ^:private ^:constant +uncommitted+ "U")


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


(def ^:private ^:constant special-attributes
  #{+txid+ +state+ +requests+ +version+ +finalized+ +image-id+ +applied+ +transient+ +date+})

(defmacro with-tx-attributes [[] & body]
  `(binding [dh/*special-enums* ~special-attributes]
     ~@body))

(defn- insert-and-return-tx-item [txid]
  (let [tx-item {+txid+ txid +state+ +pending+ +version+ 1 +date+ (get-current-time)}]
    (try (with-tx-attributes []
           (dl/put-item @tx-table-name
                        tx-item
                        :expected {+txid+ false +state+ false}))
         (catch ConditionalCheckFailedException e
           (throw (ex-info "Failed to create new transaction"
                           {:type :transaction-exception :txid txid :exception e}))))
    tx-item))

(defn get-tx-item [txid]
  ;; tx-item {... :load-requests {:table1 {prim-keys1 request1} :table2 {prim-keys2 request2}}}
  (with-tx-attributes []
    (let [tx-item (dl/get-item @tx-table-name {:txid txid} :consistent? true)]
      (if (empty? tx-item)
        (throw (ex-info "Transaction not found" {:type :transaction-not-found :txid txid}))
        tx-item))))

;;;
;;; Transaction
;;;

;; Internal table names and setters
(def ^:private tx-table-name (atom "_tx_table_"))
(def ^:private image-table-name (atom "_image_table_"))
(defn- set-tx-table-name [name]
  (reset! tx-table-name name))
(defn- set-image-table-name [name]
  (reset! image-table-name name))

(defn- new-transaction []
  (let [txid (make-txid)]
    {:txid txid :tx-item (insert-and-return-tx-item txid)
     :fully-applied-requests (sorted-set)}))

#_
(defn- transaction-item? [item]
  (contains? item +txid+))

(defn- valid-return-value? [rv]
  (contains? #{:all-old :all-new :none} rv))

(defn- get-keys [table item]
  (utils/maphash #(do (when-not (contains? item %)
                        (throw (ex-info "Can't find required key" {:key  %})))
                      [% (% item)])
                 (table-keys table)))

(defn- finish [txid state & [version]]
  (when-not (contains? #{+committed+ +rolled-back+} state)
    (throw (ex-info "Unexpected state" {:type :assertion-failure :state state :txid txid})))
  ;; NB: Amazon's implementation seems wrong - it checks if return value is null,
  ;; which is impossilble with RETURN ALL_NEW.
  (dl/update-item @tx-table-name {+txid+ txid}
                  {+state+ [:put state] +date+ [:put (get-current-time)]}
                  :return :all-new
                  :expected (cond-> {+state+ +pending+ +finalized+ false}
                                    (not (nil? version)) (merge {+version+ version}))))

(defn- transaction-completed? [tx-item]
  (assert (contains? tx-item +finalized+))
  (let [finalized? (get tx-item +finalized+)
        state (get tx-item +state+)]
    (when (and finalized? (not (contains? #{+committed+ +rolled-back+} state)))
      (throw (ex-info "Unexpected terminal state for completed tx"
                      {:type :assertion-failure :state state :tx-item tx-item})))
    finalized?))


(defn- add-request [item tx-item]
  ;; 1, 2 see addRequest of TransactionItem.java
  (dl/update-item @tx-table-name
                  {+txid+ (get tx-item +txid+)}
                  {+requests+   [:add #{item}]
                   +version+    [:add 1]
                   +date+       [:put (get-current-time)]}
                  :expected     {+state+ +pending+ +version+ (get tx-item +version+)}
                  :return :all-new)
  ;;; FIXME: more...
  ;; ** tx-item version must be increased
  )


(defn- post-commit-cleanup [tx-item]
  (when (not= (get tx-item +state+) +committed+)
    (throw (ex-info "Unexpected state instead of COMMITTED"
                    {:type :assertion-failure :state (get tx-item +state+) :tx-item tx-item})))

  )

(defn- roll-back []
  )

(defn- rollback [txid]
  (let [tx-item (try (finish txid +rolled-back+)
                     ;; Fails when
                     ;; 1. it is not in PENDING state
                     ;; 2. Finalized already
                     (catch ConditionalCheckFailedException _
                       (try (get-tx-item txid)
                            ;; After conditional check failure only happens
                            ;; When it's gone!
                            (catch RuntimeException _ ;; I.e. :transaction-not-found
                              (throw (ex-info "Suddenly the transaction completed during ROLLED_BACK!"
                                              {:type :unknown-completed-transaction :txid txid}))))))]
    (case (:state tx-item)
      +committed+ (do (when-not (transaction-completed? tx-item) (post-commit-cleanup tx-item))
                      (throw (ex-info "Transaction commited (instead of rolled-back)"
                                      {:type :transaction-committed :txid txid})))
      +rolled-back+ (when-not (transaction-completed? tx-item) (roll-back))
      :else       (throw (ex-info "Unexpected state in (rollback txid)"
                      {:type :assertion-failure :state (get tx-item +state+) :txid txid})))))

(defn- release-read-lock [lock-holder table key-map]
  )

(defn- lock-item [tx-item table item item-expected? attempts]
  (let [key-map (get-keys table item)]
    (when (<= attempts 0)
      (throw (ex-info "Unable to acquire item lock" {:type :transaction-exception :keys key-map})))
    (let [expected (cond-> {+txid+ false}
                           item-expected? (merge key-map))
          update-map (cond-> {+txid+ [:put (get tx-item +txid+)] +date+ [:put (get-current-time)]}
                             (not item-expected?) {+transient+ [:put true]})]

      (let [db-item (try (dl/update-item table key-map update-map :expected expected :return :all-new)
                         ;; ConditionalCheckFailedException means:
                         ;; 1. +txid+ has a value already (lock-holder is this tx or else)
                         ;; 2. item is not there yet
                         (catch ConditionalCheckFailedException _
                           (dl/get-item table key-map :consistent? true)))
            lock-holder (get db-item +txid+)]
        (cond (empty? lock-holder) (recur tx-item table item false (dec attempts))
              (= lock-holder (get tx-item +txid+)) db-item
              (> attempts 1) (do (try (rollback lock-holder)
                                      (catch RuntimeException ex
                                        (case (:type (ex-data ex))
                                          :transaction-completed nil
                                          :transaction-not-found (release-read-lock lock-holder table key-map))))
                                 ;; FIXME: check false??
                                 ;; i.e., above rollback or release-read-lock
                                 ;; successfully delete the item,
                                 ;; it should be false!
                                 (recur tx-item table item true (dec attempts)))
              ;; i.e., attempts = 1
              :else (throw (ex-info "Cannot grab a lock"
                                    {:type :item-not-locked-exception :txid (:txid tx-item) :lock-holder lock-holder
                                     :table table :key key-map})))))))

(defn- verify-locks [tx-item]
  (:requests tx-item)

(defn- add-request-to-transaction [tx request redrive? num-lock-acquire-attempts]
  (if redrive?
    (when (not= (get tx-item +state+) +pending+)
      (throw (ex-info "Initial state must be in pending" {:type :assertion-failure :state (get tx-item +state+)})))
    (loop [i num-lock-acquire-attempts tx-item tx-item success false]
      (verify-locks)
      ))
  (let [item (lock-item tx-item table item true item-lock-acquire-attempts)]
    ;;; FIXME: ...
    (update-tx-table tx-item table item opts-map
    )))

(defn- attempt-to-add-request-to-tx [tx request]
  (loop [i tx-lock-contention-resolution-attempts tx tx]
    (if (<= i 0)
      tx                                ; return new tx
      (recur (dec i)
             (add-request-to-transaction tx request (> i 0) tx-lock-acquire-attempts))))
  ;;..............
  )

(defn- validate-special-attributes-exclusion [special-attributes]
  (when (some #(contains? special-attributes %) special-attributes)
    (throw (ex-info "Return must not contain any of reserved attributes"
                    {:invalid-attributes (filter #(contains? special-attributes %) special-attributes)}))))

(defn- validate-write-item-arguments [key-map opts]
  (when-not (valid-return-value? (:return opts))
    (throw (ex-info "Return value has to be one :all-old, :all-new, and :none"
                    {:type :invalid-request :return (:return opts)})))

  (validate-special-attributes-exclusion (keys key-map))
  ;; FIXME: dynohub does not support some option args
  ;;            (:return-icm is not in dynohub code)
  ;;            Revise dynohub!
  (doseq [op [:return-cc? :return-icm :expected]]
    (when (op opts)
      (throw (ex-info "Not supported option" {:type :invalid-request :invalid-option op}))))
  )

;;;
;;; Public API
;;;

(defn init-tx [& opts]
  [(apply dl/ensure-table @tx-table-name [+txid+ :s] opts)
   (apply dl/ensure-table @image-table-name [+image-id+ :s] opts)])


(defmacro with-transaction [[tx] & body]
  `(let [~tx (atom (new-transaction))]
     ~@body))

(defn delete-tx [tx]
  )

(defn commit-tx [tx]
  )

(defn commit-and-delete-tx [tx]
  )

(defn put-item [tx-item table item & {:keys [tx] :as opts}]
  (validate-write-item-arguments item opts)
  (attempt-to-add-request-to-tx tx {:op :put-item :table table :item item :opts opts}))

(defn get-item [tx-item table prim-kvs & {:keys [tx] :as opts}]
  (validate-special-attributes-exclusion (:keys prim-kvs))
  (attempt-to-add-request-to-tx tx {:op :get-item :table table :prim-kvs prim-kvs :opts opts}))

(defn update-item [tx-item table prim-kvs update-map & {:keys [tx] :as opts}]
  (validate-write-item-arguments (merge prim-kvs update-map) opts)
  (attempt-to-add-request-to-tx tx {:op :update-item :table table :prim-kvs prim-kvs
                                    :update-map update-map :opts opts}))

(defn delete-item [table prim-kvs & {:keys [tx] :as opts}]
  (validate-write-item-arguments prim-kvs opts)
  (attempt-to-add-request-to-tx tx {:op :delete-item :table table :prim-kvs prim-kvs :opts opts}))


;;; DYNOTX.CLJ ends here

;;(init-tx)
;;(begin-transaction)
;;(dl/get-item @tx-table-name {+txid+ "3ede573e-0087-4ddf-bd0b-37571cbd6b6c"})