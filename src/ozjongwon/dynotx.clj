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
;; TODO:
;;      Figure out when ConditionalCheckFailedException occurs
;;      Define TP's exceptions
;;      Is Amazon's DynamoDB Transaction library reliable??
;;      Read TP books and do it from the scratch
;;
;;      * Principles of TP
;;              Chap 4 - persistent queue
;;              Chap 6 - locking
;;              Chap 7 - log based recovery algorithm
;;              Chap 10 - API
;;              Chap 3 - SW components
;;
;;      * Basic terms
;;              start, commit, abort - TPM does get requests and perform
;;
;;      * 2PC between Transaction Manager and Resource Managers - perpare & commit phases
;;      * Locking rule - two-phase locking = a transaction must obtain all locks before releasing any of them.
;;                      growing phase then shrinking phase
;;
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
            [com.amazonaws
             AmazonServiceException]
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
(def ^:dynamic *current-tx*)

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
           (utils/error "Failed to create new transaction"
                    {:type :transaction-exception :txid txid :exception e})))
    tx-item))

(defn get-tx-item [txid]
  ;; tx-item {... :load-requests {:table1 {prim-keys1 request1} :table2 {prim-keys2 request2}}}
  (with-tx-attributes []
    (let [tx-item (dl/get-item @tx-table-name {:txid txid} :consistent? true)]
      (if (empty? tx-item)
        (utils/error "Transaction not found" {:type :transaction-not-found :txid txid})
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
     :fully-applied-request-versions (sorted-set)}))

#_
(defn- transaction-item? [item]
  (contains? item +txid+))

(defn- valid-return-value? [rv]
  (contains? #{:all-old :all-new :none} rv))

(defmulti get-keys (fn [req]
                     (:op req)))

(defmethod get-keys :put-item [req]
  (let [item (:item req)]
    (utils/maphash #(do (when-not (contains? item %)
                          (utils/error "Can't find required key" {:type :application-error :key  %}))
                        [% (get item %)])
                   (table-keys (:table req)))))

(defmethod get-keys :default [req]
  (:prim-kvs req))

(defn- mark-committed-or-rolled-back [txid state & [version]] ;; finish
  (utils/tx-assert (contains? #{+committed+ +rolled-back+} state) "Unexpected state" state :txid txid)
  (let [result (dl/update-item @tx-table-name {+txid+ txid}
                               {+state+ [:put state] +date+ [:put (get-current-time)]}
                               :return :all-new
                               :expected (cond-> {+state+ +pending+ +finalized+ false}
                                                 (not (nil? version)) (merge {+version+ version})))]
    (utils/tx-assert (not (empty? result)) "Unexpected null tx item after committing" :state state :txid txid)

    result))

(defn- transaction-completed? [tx-item]
  (utils/tx-assert (contains? tx-item +finalized+))
  (let [finalized? (get tx-item +finalized+)
        state (get tx-item +state+)]
    (utils/tx-assert (and finalized? (contains? #{+committed+ +rolled-back+} state))
                     "Unexpected terminal state for completed tx" :state state :tx-item tx-item)
    finalized?))

(defn- add-request-map-to-current-tx [tx-item request-atom]
  (let [kvs (get-keys @request-atom)
        {:keys [table op]} @request-atom
        map (or (:requests-map @*current-tx*) {})]
    (let [{existing-op :op} (get-in map [table kvs])]
      (cond (or (empty? existing-op) (and (= existing-op :get-item) (not= op :get-item)))
            (do
              ;; everything's fine. So update version and requests-map
              (swap! request-atom assoc :version (get tx-item +version+))
              (swap! *current-tx* assoc-in [:requests-map table kvs] @request-atom)

              (and (not= existing-op :get-item) (not= op :get-item))
              (utils/error "An existing request other than :get-item found!"
                           {:type :duplicate-request :txid (get tx-item +txid+) :table table :prim-kvs kvs}))))))

(defn- add-request-to-tx-item [tx-item request-atom]
  (let [current-version (get tx-item +version+)]
    (try (let [new-tx-item (dl/update-item @tx-table-name
                                         {+txid+ (get tx-item +txid+)}
                                         {+requests+   [:add #{@request-atom}]
                                          +version+    [:add 1]
                                          +date+       [:put (get-current-time)]}
                                         :expected     {+state+ +pending+ +version+ current-version}
                                         :return :all-new)
               new-version (get new-tx-item +version+)]

           ;; tx-item update is successful. Now update request-map, request version, etc
           (add-request-map-to-current-tx tx-item request-atom) ;; yes this uses current-version
           ;; update *current-tx* as well
           (swap! *current-tx* assoc :tx-item new-tx-item)
           (utils/tx-assert (= new-version (inc current-version)) "Unexpected version number from update result")
           new-tx-item)
         (catch AmazonServiceException e
           (utils/error "Unexpected AmazonServiceException. Updating failed" {:type :amazon-service-error :error e})))))

(defn- release-read-lock [txid table key-map]
  (try (dl/update-item table key-map {+txid+ [:delete] +date+ [:delete]}
                       :expected {+txid+ txid +transient+ false +applied+ false})
       (catch ConditionalCheckFailedException _
         (try (dl/delete-item table key-map :expected  {+txid+ txid +transient+ true +applied+ false})
              (catch ConditionalCheckFailedException _
                (let [item (dl/get-item table key-map)]
                  (utils/tx-assert (and (not (empty? item)) (= txid (get item +txid+)) (contains? item +applied+))
                                   "Item should not have been applied.  Unable to release lock" :item item)))))))

(defmulti unlock-item-after-commit-using-op (fn [txid req]
                                              (:op req)))

(defn- %put-or-update-request-unlock [txid req]
  (dl/update-item (:table req) (:prim-kvs req) {+txid+ [:delete] +transient+ [:delete] +applied+ [:delete] +date+ [:delete]}
                  :expected {+txid+ txid}))

(defmethod unlock-item-after-commit-using-op :update-item [txid req]
  (%put-or-update-request-unlock txid req))

(defmethod unlock-item-after-commit-using-op :put-item [txid req]
  (%put-or-update-request-unlock txid req))

(defmethod unlock-item-after-commit-using-op :delete-item [txid req]
  (dl/delete-item (:table req) (:prim-kvs req) :expected {+txid+ txid}))

(defmethod unlock-item-after-commit-using-op :get-item [txid req]
  (release-read-lock txid (:table req) (:prim-kvs req)))

(defn- unlock-item-after-commit [txid req]
  (try (unlock-item-after-commit-using-op txid req)
       (catch ConditionalCheckFailedException _)))

(defn- tx-item->image-id [tx-item]
  (let [version (get tx-item +version+)]
    (utils/tx-assert (> version 0))
    (str (get tx-item +txid+) "#" version)))

(defn- delete-item-image [tx-item]
  (dl/delete-item @image-table-name {+image-id+ (tx-item->image-id tx-item)}))

(defn- finalize-transaction [tx-item expected-current-state]
  (utils/tx-assert (contains? #{+committed+ +rolled-back+} expected-current-state))
  (let [txid (get tx-item +txid+)]
    (try (dl/update-item @tx-table-name {+txid+ txid} {+finalized+ [:put true] +date+ [:put (get-current-time)]}
                         :expected {+state+ expected-current-state})
         (catch ConditionalCheckFailedException _
           (try (let [tx-item (get-tx-item txid)]
                  (when-not (transaction-completed? tx-item)
                    (utils/tx-assert "Expected the transaction to be completed (no item), but there was one."))
                  tx-item)
                (catch RuntimeException e (when (not= (:type e) :transaction-not-found)
                                            (utils/error e))))))))

(defn- post-commit-cleanup [tx-item] ;; doCommit
  (let [state (get tx-item +state+)
        requests (:requests tx-item)]
    (utils/tx-assert (= state +committed+)
                     "Unexpected state instead of COMMITTED" :state state :tx-item tx-item)
    (for [request requests]
      (unlock-item-after-commit (:txid tx-item) request))

    (for [request requests]
      (delete-item-image tx-item))

    (finalize-transaction tx-item +committed+)))

(defn- %rollback [tx-item]
  (utils/tx-assert (= (get tx-item +state+) +rolled-back+) "Transaction state is not ROLLED_BACK" :state (get tx-item +state+) :tx-item tx-item)

  (for [request (:requests tx-item)]
    (rollback-item-and-release-lock request)
    (delete-item-image
  (try (mark-committed-or-rolled-back +rolled-back+)
       (catch

  )

(defn- rollback-using-txid [txid]
  (let [tx-item (try (mark-committed-or-rolled-back txid +rolled-back+)
                     ;; (Maybe??)Fails when
                     ;; 1. it is not in PENDING state
                     ;; 2. Finalized already
                     (catch ConditionalCheckFailedException _
                       (try (get-tx-item txid)
                            ;; After conditional check failure only happens
                            ;; When it's gone!
                            (catch RuntimeException _ ;; I.e. :transaction-not-found
                              (utils/error "Suddenly the transaction completed during ROLLED_BACK!"
                                       {:type :unknown-completed-transaction :txid txid})))))]
    (case (:state tx-item)
      +committed+ (do (when-not (transaction-completed? tx-item) (post-commit-cleanup tx-item))
                      (utils/error "Transaction commited (instead of rolled-back)"
                               {:type :transaction-committed :txid txid}))
      +rolled-back+ (when-not (transaction-completed? tx-item) (%rollback tx-item))
      :else (utils/tx-assert false "Unexpected state in (rollback-tx tx)" :state (get tx-item +state+) :txid txid))

    tx-item))



;;tx request true item-lock-acquire-attempts
;;(lock-item tx request true item-lock-acquire-attempts)
(defn- lock-item [tx request item-expected? attempts]
  (let [key-map (get-keys request)
        {:keys [table]} request
        txid  (get tx +txid+)]
    (when (<= attempts 0)
      (utils/error "Unable to acquire item lock" {:type :transaction-exception :keys key-map}))
    (let [expected (cond-> {+txid+ false}
                           item-expected? (merge key-map))
          update-map (cond-> {+txid+ [:put txid] +date+ [:put (get-current-time)]}
                             (not item-expected?) (merge {+transient+ [:put true]}))
          db-item (try (dl/update-item table key-map update-map :expected expected :return :all-new)
                       ;; ConditionalCheckFailedException means:
                       ;; 1. +txid+ has a value already (lock-holder is this tx or else)
                       ;; 2. item is not there yet
                       (catch ConditionalCheckFailedException _
                         (dl/get-item table key-map :consistent? true)))
          lock-holder (get db-item +txid+)]
      (cond (empty? lock-holder) (recur tx request false (dec attempts))
            (= lock-holder txid) db-item
            (> attempts 1) (do (try (rollback-using-txid lock-holder)
                                    (catch RuntimeException ex
                                      (case (:type (ex-data ex))
                                        :transaction-completed nil
                                        :transaction-not-found (release-read-lock lock-holder table key-map))))
                               ;; FIXME: check false??
                               ;; i.e., above rollback or release-read-lock
                               ;; successfully delete the item,
                               ;; item-expected? should be false!
                               (recur tx request true (dec attempts)))
            ;; i.e., attempts = 1
            :else (utils/error "Cannot grab a lock"
                               {:type :item-not-locked-exception :txid txid :lock-holder lock-holder
                                :table table :key key-map})))))

(declare add-request-to-transaction)
(defn- ensure-grabbing-all-locks [tx]
  (let [fully-applied-request-versions (:fully-applied-request-versions @tx)]
    (for [request (:requests @tx)]
      ;; request's version keeps increasing when failed to apply
      (when-not (contains? fully-applied-request-versions (:version request))
        (add-request-to-transaction tx request true item-lock-acquire-attempts)))))

(defn- save-item-image [request item]
  (when-not (or (= (:op request) :get-item) (contains? item +applied+) (contains? item +transient+))
    ;;(utils/tx-assert (= (get item +txid+) (:txid request)) "This will never happen in the real world!")
    ;;(utils/tx-assert (nil? (get item +image-id+)) "This will never happen in the real world!")
    (let [image-id (str (:txid request) "#" (:version request))]
      (try (dl/put-item @image-table-name (merge item {+image-id+ image-id +txid+ (:txid request)} :expected {+image-id+ false}))
           ;; Already exists! Ignore!
           (catch ConditionalCheckFailedException _)))))

(defn- add-request-to-transaction [tx request fight-for-a-lock? num-lock-acquire-attempts]
  (let [tx-item (:tx-item @tx)
        txid (get tx-item +txid+)
        request-atom (atom request)]
    (if fight-for-a-lock?
      (utils/tx-assert (= (get tx-item +state+) +pending+)
                       "To fight for a lock, TX must be in pending state" :state (get tx-item +state+))
      (loop [i num-lock-acquire-attempts]
        (if (<= i 0)
          (utils/error "Unable to add request to transaction - too much contention for the tx record"
                       {:type :transaction-exception :txid txid})
          (do (ensure-grabbing-all-locks tx)
              (or (try (add-request-to-tx-item tx-item request-atom) ;; This call ADDS a VERSION to the request
                       (catch ConditionalCheckFailedException _
                         ;; TX changed unexpectedly. Check its state
                         (let [current-state (get (get-tx-item txid) +state+)]
                           (when (not= current-state +pending+)
                             (utils/error "Attempted to add a request to a transaction that was not in PENDING state "
                                          {:type :transaction-not-in-pending-state :txid txid})))))
                  (recur (dec i)))))))
    (let [item (lock-item tx @request-atom true item-lock-acquire-attempts)]
      (save-item-image @request-atom item)

      (update-tx-table tx-item table item opts-map
                       ))))

(defn- attempt-to-add-request-to-tx [tx request]
  ;;
  ;; Repeatedly try to add this request to the tx.
  ;; If it fails because the 'item' of the request is already in a different transaction,
  ;; tries to rollback the other transaction.
  ;;
  (loop [i tx-lock-contention-resolution-attempts last-conflict nil]
    (if (<= i 0)
      (utils/error last-conflict)
      (let [[item ex] (try [(add-request-to-transaction tx request (< i tx-lock-contention-resolution-attempts) tx-lock-acquire-attempts) nil]
                           (catch clojure.lang.ExceptionInfo e
                             (when (> i 1) ;; avoid unnecessary rollback
                               (if-let [other-txid (:txid (ex-data e))]
                                 (utils/ignore-errors (rollback-using-txid other-txid))
                                 (utils/error e)))
                             [nil e]))]
        (if item
          item
          (recur (dec i) ex))))))

(defn- validate-special-attributes-exclusion [special-attributes]
  (when (some #(contains? special-attributes %) special-attributes)
    (utils/error "Return must not contain any of reserved attributes"
             {:invalid-attributes (filter #(contains? special-attributes %) special-attributes)})))

(defn- validate-write-item-arguments [key-map opts]
  (when-not (valid-return-value? (:return opts))
    (utils/error "Return value has to be one :all-old, :all-new, and :none"
             {:type :invalid-request :return (:return opts)}))

  (validate-special-attributes-exclusion (keys key-map))
  ;; FIXME: dynohub does not support some option args
  ;;            (:return-icm is not in dynohub code)
  ;;            Revise dynohub!
  (doseq [op [:return-cc? :return-icm :expected]]
    (when (op opts)
      (utils/error "Not supported option" {:type :invalid-request :invalid-option op})))
  )

;;;
;;; Public API
;;;

(defn init-tx [& opts]
  [(apply dl/ensure-table @tx-table-name [+txid+ :s] opts)
   (apply dl/ensure-table @image-table-name [+image-id+ :s] opts)])

(defmacro with-transaction [[& tx] & body]
  (if tx
    `(let [~@tx (atom (new-transaction))]
       (binding [*current-tx* ~@tx]
         ~@body))
    `(binding [*current-tx* (atom (new-transaction))]
         ~@body)))

(defn delete-tx [tx]
  )

(defn commit-tx [tx]
  )

(defn commit-and-delete-tx [tx]
  )

(defn put-item [table item & opts]
  (validate-write-item-arguments item opts)
  (attempt-to-add-request-to-tx *current-tx* {:op :put-item :table table :item item :opts opts}))

(defn get-item [table prim-kvs & opts]
  (validate-special-attributes-exclusion (:keys prim-kvs))
  (attempt-to-add-request-to-tx *current-tx* {:op :get-item :table table :prim-kvs prim-kvs :opts opts}))

(defn update-item [table prim-kvs update-map & opts]
  (validate-write-item-arguments (merge prim-kvs update-map) opts)
  (attempt-to-add-request-to-tx *current-tx* {:op :update-item :table table :prim-kvs prim-kvs
                                                    :update-map update-map :opts opts}))

(defn delete-item [table prim-kvs & opts]
  (validate-write-item-arguments prim-kvs opts)
  (attempt-to-add-request-to-tx *current-tx* {:op :delete-item :table table :prim-kvs prim-kvs :opts opts}))


;;; DYNOTX.CLJ ends here

;;(init-tx)
;;(begin-transaction)
;;(dl/get-item @tx-table-name {+txid+ "3ede573e-0087-4ddf-bd0b-37571cbd6b6c"})