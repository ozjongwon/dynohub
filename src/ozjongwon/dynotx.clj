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
            [clojure.lang ExceptionInfo]
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

;; Internal table names and setters
(def ^:private tx-table-name (atom "_tx_table_"))
(def ^:private image-table-name (atom "_image_table_"))
(defn- set-tx-table-name [name]
  (reset! tx-table-name name))
(defn- set-image-table-name [name]
  (reset! image-table-name name))

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
  :_TxId)
(def ^:private ^:constant +state+ "pending -> commited or rolled-back"
  :_TxS)
(def ^:private ^:constant +requests+ "list of items participating in the transaction with unique IDs"
  :_TxR)
(def ^:private ^:constant +version+ "a version number storing for detecting concurrent changes to a TX record."
  :_TxV)
(def ^:private ^:constant +finalized+ :_Tx)
(def ^:private ^:constant +image-id+ :_TxI)
(def ^:private ^:constant +applied+ "indicating whether the transaction has performed the write to the item yet."
  :_TxA)
(def ^:private ^:constant +transient+ "indicating if the item was inserted in order to acquire the lock."
  :_TxT)
(def ^:private ^:constant +date+   "indicating approximately when the item was locked."
 :_TxD)

;; State
(def ^:private ^:constant +pending+ "P")
(def ^:private ^:constant +committed+ "C")
(def ^:private ^:constant +rolled-back+ "R")

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
    (let [tx-item (dl/get-item @tx-table-name {+txid+ txid} :consistent? true)]
      (if (empty? tx-item)
        (utils/error "Transaction not found" {:type :transaction-not-found :txid txid})
        tx-item))))

;;;
;;; Transaction
;;;

(defn- new-transaction []
  (let [txid (make-txid)]
    {:txid txid :tx-item (insert-and-return-tx-item txid)
     :fully-applied-request-versions (sorted-set)}))

(defn- valid-return-value? [rv]
  (contains? #{:all-old :all-new :none} rv))

(defmulti prim-kvs :op)

(defmethod prim-kvs :put-item [req]
  (let [item (:item req)
        table-keys (table-keys (:table req))
        kvs  (select-keys item table-keys)]
    (if (= (count kvs) (count table-keys))
      kvs
      (utils/error "Can't find required keys" {:type :application-error :keys (filter #(not (contains? kvs)) table-keys)}))))

(defmethod prim-kvs :default [req]
  (:prim-kvs req))

(defn- mark-committed-or-rolled-back [txid state & [version]] ;; finish
  (utils/tx-assert (contains? #{+committed+ +rolled-back+} state) "Unexpected state(mark-committed-or-rolled-back)" state :txid txid)
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
  ;; request-map = {:table1 {prim-k1 v1} ...}
  (let [kvs (prim-kvs @request-atom)
        {:keys [table op]} @request-atom
        map (or (:requests-map @*current-tx*) {})]
    (let [{existing-op :op} (get-in map [table kvs])]
      ;; write op always win
      ;; only first get op win when more one get op occurs
      ;; multiple write ops are not allowed
      (cond (or (empty? existing-op)
                ;; overwrite
                (and (= existing-op :get-item) (not= op :get-item)))
            (do
              ;; everything's fine. So update version and requests-map
              (swap! request-atom assoc :version (get tx-item +version+))
              (swap! *current-tx* assoc-in [:requests-map table kvs] @request-atom))

            (and (not= existing-op :get-item) (not= op :get-item))
            (utils/error "An existing request other than :get-item found!"
                         {:type :duplicate-request :txid (get tx-item +txid+) :table table :prim-kvs kvs})))))

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

(defmulti unlock-item-after-commit-using-op :op)

(defn- %put-or-update-request-unlock [txid req]
  (dl/update-item (:table req) (:prim-kvs req) {+txid+ [:delete] +transient+ [:delete] +applied+ [:delete] +date+ [:delete]}
                  :expected {+txid+ txid}))

(defmethod unlock-item-after-commit-using-op :update-item [req txid]
  (%put-or-update-request-unlock txid req))

(defmethod unlock-item-after-commit-using-op :put-item [req txid]
  (%put-or-update-request-unlock txid req))

(defmethod unlock-item-after-commit-using-op :delete-item [req txid]
  (dl/delete-item (:table req) (:prim-kvs req) :expected {+txid+ txid}))

(defmethod unlock-item-after-commit-using-op :get-item [req txid]
  (release-read-lock txid (:table req) (:prim-kvs req)))

(defn- unlock-item-after-commit [txid req]
  (try (unlock-item-after-commit-using-op req txid)
       (catch ConditionalCheckFailedException _)))

(defn- delete-item-image [tx-item version]
  (dl/delete-item @image-table-name {+image-id+ (str (get tx-item +txid+) "#" version)}))

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
                (catch ExceptionInfo e (when (not= (:type e) :transaction-not-found)
                                            (utils/error e))))))))

(defn- get-requests-from-tx
  ([]   (get-requests-from-tx *current-tx*))
  ([tx] (mapcat (fn [[_ map]] (vals map)) (:requests-map @tx))))

(defn- post-commit-cleanup [tx-item] ;; doCommit
  (let [state (get tx-item +state+)
        requests (get-requests-from-tx)]
    (utils/tx-assert (= state +committed+)
                     "Unexpected state instead of COMMITTED" :state state :tx-item tx-item)
    (for [request requests]
      (unlock-item-after-commit (:txid tx-item) request))

    (for [request requests]
      (delete-item-image tx-item (:version request)))

    (finalize-transaction tx-item +committed+)))

(defn- rollback-item-and-release-lock [request])
(defn- post-rollback-cleanup [tx-item]
  (utils/tx-assert (= (get tx-item +state+) +rolled-back+)
                   "Transaction state is not ROLLED_BACK" :state (get tx-item +state+) :tx-item tx-item)
  (doseq [request (get-requests-from-tx)]
    (rollback-item-and-release-lock request)
    (delete-item-image tx-item (:version request)))

  (finalize-transaction tx-item +rolled-back+))

(defn- rollback-using-txid [txid]
  (let [tx-item (try (mark-committed-or-rolled-back txid +rolled-back+)
                     ;; (Maybe??)Fails when
                     ;; 1. it is not in PENDING state
                     ;; 2. Finalized already
                     (catch ConditionalCheckFailedException _
                       (try (get-tx-item txid)
                            ;; After conditional check failure only happens
                            ;; When it's gone!
                            (catch ExceptionInfo _ ;; I.e. :transaction-not-found
                              (utils/error "Suddenly the transaction completed during ROLLED_BACK!"
                                       {:type :unknown-completed-transaction :txid txid})))))]
    (case (:state tx-item)
      +committed+ (do (when-not (transaction-completed? tx-item) (post-commit-cleanup tx-item))
                      (utils/error "Transaction commited (instead of rolled-back)"
                               {:type :transaction-committed :txid txid}))
      +rolled-back+ (when-not (transaction-completed? tx-item) (post-rollback-cleanup tx-item))
      (utils/tx-assert false "Unexpected state in (rollback-tx tx)" :state (get tx-item +state+) :txid txid))

    tx-item))

(defn- lock-item [txid request item-expected? attempts]
  (let [key-map (prim-kvs request)
        {:keys [table]} request]
    (when (<= attempts 0)
      (utils/error "Unable to acquire item lock" {:type :transaction-exception :keys key-map}))
    (let [expected (cond-> {+txid+ false}
                           item-expected? (merge key-map))
          update-map (cond-> {+txid+ [:put txid] +date+ [:put (get-current-time)]}
                             (not item-expected?) (merge {+transient+ [:put true]}))
          db-item (try (dl/update-item table key-map update-map :expected expected :return :all-new)
                       ;; ConditionalCheckFailedException means:
                       ;; 1. +txid+ has a value already (lock-holder is this txid or else)
                       ;; 2. item is not there yet
                       (catch ConditionalCheckFailedException _
                         (dl/get-item table key-map :consistent? true)))
          lock-holder (get db-item +txid+)]
      (cond (empty? lock-holder) (recur txid request false (dec attempts))
            (= lock-holder txid) db-item
            (> attempts 1) (do (try (rollback-using-txid lock-holder)
                                    (catch ExceptionInfo ex
                                      (case (:type (ex-data ex))
                                        :transaction-completed nil
                                        :transaction-not-found (release-read-lock lock-holder table key-map)
                                        (utils/error ex))))
                               ;; FIXME: check false??
                               ;; i.e., above rollback or release-read-lock
                               ;; successfully delete the item,
                               ;; item-expected? should be false!
                               (recur txid request true (dec attempts)))
            ;; i.e., attempts = 1
            :else (utils/error "Cannot grab a lock"
                               {:type :item-not-locked-exception :txid txid :lock-holder lock-holder
                                :table table :key key-map})))))

(declare add-request-to-transaction)
(defn- ensure-grabbing-all-locks [tx]
  (let [fully-applied-request-versions (:fully-applied-request-versions @tx)]
    (for [request (get-requests-from-tx tx)]
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

(defn- release-read-lock [table keys txid]
  (try (dl/update-item table keys {+txid+ [:delete] +date+ [:delete]}
                       :expected {+txid+ txid +transient+ false +applied+ false})
       (catch ConditionalCheckFailedException _
         (try (dl/delete-item table keys :expected {+txid+ txid +transient+ true +applied+ false})
              (catch ConditionalCheckFailedException _
                (let [item (dl/get-item table keys :consistent? true)]
                  (utils/tx-assert (not (and item (= (get item +txid+) txid) (contains? item +applied+)))
                                   "Item should not have been applied. Unable to release lock item" item)))))))

(defmulti apply-request-op :op)
(defmethod apply-request-op :put-item [request locked-item]
  (apply dl/put-item
         (:table request)
         (merge (assoc (:item request) +applied+ true)
                (select-keys locked-item [+txid+ +date+ +transient+]))
         :expected {+txid+ (get locked-item +txid+) +applied+ false}
         (utils/hash-map->list (:opts request))))

(defmethod apply-request-op :update-item [request locked-item]
  (apply dl/update-item
         (:table request)
         (:prim-kvs request)
         (assoc (:update-map request) +applied+ true)
         :expected {+txid+ (get locked-item +txid+) +applied+ false}
         (utils/hash-map->list (:opts request))))

(defmethod apply-request-op :default [_ _]
  ;; noop for delete-item and get-item
  nil)

(defn- load-item-image [version]
  (dissoc (dl/get-item @image-table-name {+image-id+ (str (get *current-tx* +txid+) "#" version)} :consistent? true)
          +image-id+))

(defn- compute-item-put-or-update [request applied-item locked-item return]
  (case return
    :all-old (or applied-item
                 (load-item-image (:version request))
                 (utils/error "Transaction must have completed since the old copy of the image is missing"
                              {:type :unknown-completed-transaction :txid (get locked-item +txid+)}))
    :all-new (or applied-item
                 (let [applied-item (dl/get-item (:table request) (prim-kvs request))]
                   (if applied-item
                     (if (= (get locked-item +txid+) (get applied-item +txid+))
                       applied-item
                       (utils/error "Transaction IDs mismatched"
                                    {:type :item-not-locked :table (:table request) :expected locked-item :get applied-item}))
                     (utils/error "Transaction must have completed since the item no longer exists"
                                  {:type :unknown-completed-transaction :txid (get locked-item +txid+)}))))
    nil))

(defmulti item-with-proper-return-value (fn [r _ _ _ _]
                                          (:op r)))

(defmethod item-with-proper-return-value :put-item [request applied-item locked-item _ return]
  (compute-item-put-or-update request applied-item locked-item return))

(defmethod item-with-proper-return-value :update-item [request applied-item locked-item _ return]
  (compute-item-put-or-update request applied-item locked-item return))

(defmethod item-with-proper-return-value :delete-item [_ _ locked-item _ return]
  ;; Assume it is not transient
  (when (= return :all-old)
    locked-item))

(defmethod item-with-proper-return-value :get-item [request _ locked-item transient-item? _]
  (let [locking-request-op (get-in (:requests-map @*current-tx*) [(:table request) (prim-kvs request)])]
    ;; No item for deleted item & repeated get-item on a transient item
    (when-not (or (= locking-request-op :delete-item)
                  (and (= locking-request-op :get-item) transient-item?))
      (when-let [attrs-to-get (get-in request [:opts :attrs])]
        (select-keys locked-item attrs-to-get)))))

(defn- apply-and-keep-lock [request locked-item]
  (let [return          (get-in request [:opts :return])
        transient-item? (contains? locked-item +transient+)]
    (let [applied-item (when-not (contains? locked-item +applied+) ; not applied yet, apply!
                         (try (apply-request-op request locked-item)
                              ;; this won't happen! - 'Applied' already
                              (catch ConditionalCheckFailedException _)))]
      ;; No item for transient + :all-old
      (when-not (and (= return :all-old) transient-item?)
        (item-with-proper-return-value request applied-item locked-item transient-item? return)))))

(defn- add-request-to-transaction [tx request fight-for-a-lock? num-lock-acquire-attempts]
  (let [tx-item (:tx-item @tx)
        txid (get tx-item +txid+)
        request-atom (atom request)]
    (if fight-for-a-lock?
      (utils/tx-assert (= (get tx-item +state+) +pending+)
                       "To fight for a lock, TX must be in pending state" :state (get tx-item +state+))
      (loop [i num-lock-acquire-attempts success? false]
        (cond (<= i 0)
              (utils/error "Unable to add request to transaction - too much contention for the tx record"
                           {:type :transaction-exception :txid txid})
              success? success?
              :else
              (do (ensure-grabbing-all-locks tx)
                  (let [success? (try (do (add-request-to-tx-item tx-item request-atom) ;; This call ADDS a VERSION to the request
                                          true)
                                      (catch ConditionalCheckFailedException _
                                        ;; TX changed unexpectedly. Check its state
                                        (let [current-state (get (get-tx-item txid) +state+)]
                                          (when (not= current-state +pending+)
                                            (utils/error "Attempted to add a request to a transaction that was not in PENDING state "
                                                         {:type :transaction-not-in-pending-state :txid txid})))
                                        false))]
                      (recur (dec i) success?))))))
    ;; When the item locked, save it to the item image
    (let [item  (lock-item txid @request-atom true item-lock-acquire-attempts)
          _     (save-item-image @request-atom item)
          tx-item (try (get-tx-item txid)
                       (catch ExceptionInfo e
                         (release-read-lock (:table request) (prim-kvs request) txid)
                         (utils/error e)))]
      (condp = (get tx-item +state+)
        +committed+        (do (post-commit-cleanup tx-item)
                               (utils/error "The transaction already committed"
                                            {:type :transaction-commited :txid txid}))
        +rolled-back+      (do (post-rollback-cleanup tx-item)
                               (utils/error "The transaction already rolled back"
                                            {:type :transaction-commited :txid txid}))
        +pending+          nil
        (utils/error "Unexpected state(add-request-to-tx-item)" {:type :transaction-exception :state (get tx-item +state+)}))
      (let [final-item (apply-and-keep-lock @request-atom item)]
        (when-not (nil? (:version request))
          (swap! tx assoc :fully-applied-request-versions (conj (:fully-applied-request-versions @tx) (:version request))))
        final-item))))

(defn- attempt-to-add-request-to-tx [tx request]
  ;;
  ;; Repeatedly try to add this request to the tx.
  ;; If it fails because the 'item' of the request is already in a different transaction,
  ;; tries to rollback the other transaction.
  ;;
  (loop [i tx-lock-contention-resolution-attempts last-conflict nil]
    (if (<= i 0)
      (utils/error last-conflict)
      (let [[success? ex] (try (do (add-request-to-transaction tx request (< i tx-lock-contention-resolution-attempts) tx-lock-acquire-attempts)
                                   [true nil])
                           (catch clojure.lang.ExceptionInfo e
                             (when (> i 1) ;; avoid unnecessary rollback
                               (if-let [other-txid (:txid (ex-data e))]
                                 (utils/ignore-errors (rollback-using-txid other-txid))
                                 (utils/error e)))
                             [nil e]))]
        (when-not success?
          (recur (dec i) ex))))))

(defn- validate-special-attributes-exclusion [attributes]
  (when (some #(contains? special-attributes %) attributes)
    (utils/error "Return must not contain any of reserved attributes"
             {:invalid-attributes (filter #(contains? special-attributes %) attributes)})))

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
  ;; FIXME: remove empty 'opts'
  (let [opts (apply hash-map opts)]
    (validate-write-item-arguments item opts)
    (attempt-to-add-request-to-tx *current-tx* {:op :put-item :table table :item item :opts opts})))

(defn get-item [table prim-kvs & opts]
  (validate-special-attributes-exclusion (:keys prim-kvs))
  (attempt-to-add-request-to-tx *current-tx* {:op :get-item :table table :prim-kvs prim-kvs :opts (apply hash-map opts)}))

(defn update-item [table prim-kvs update-map & opts]
  (let [opts (apply hash-map opts)]
    (validate-write-item-arguments (merge prim-kvs update-map) opts)
    (attempt-to-add-request-to-tx *current-tx* {:op :update-item :table table :prim-kvs prim-kvs
                                                :update-map update-map :opts opts})))

(defn delete-item [table prim-kvs & opts]
  (let [opts (apply hash-map opts)]
    (validate-write-item-arguments prim-kvs opts)
    (attempt-to-add-request-to-tx *current-tx* {:op :delete-item :table table :prim-kvs prim-kvs :opts opts})))


;;; DYNOTX.CLJ ends here

;;(init-tx)
;;(begin-transaction)
;;(dl/get-item @tx-table-name {+txid+ "3ede573e-0087-4ddf-bd0b-37571cbd6b6c"})