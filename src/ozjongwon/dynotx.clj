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
;;      Is Amazon's DynamoDB Transaction library reliable??
;;
;;
;;;; Code:
(ns ozjongwon.dynotx
  "Clojure DynamoDB Transaction - idea from https://github.com/awslabs/dynamodb-transactions"
  {:author "Jong-won Choi"}
  (:require [ozjongwon.dynolite  :as dl]
            [ozjongwon.dynohub  :as dh]
            [ozjongwon.utils    :as utils]
            [clojure.set])
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
(def ^:private ^:constant +finalized+ :_TxF)
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

;; public enum IsolationLevel {
;;     UNCOMMITTED,
;;     COMMITTED,
;;     READ_LOCK // what does it mean to read an item you wrote to in a transaction?


(def ^:private ^:constant special-attributes
  #{+txid+ +state+ +requests+ +version+ +finalized+ +image-id+ +applied+ +transient+ +date+})

(defmacro with-tx-attributes [[] & body]
  `(binding [dh/*special-enums* ~special-attributes]
     ~@body))

;;;
;;; Primitive functions, etc
;;;
;;;
;;; tx(transaction) = tx-item + :fully-applied-request-versions + requests-map
;;;
(def tx-map (ref {}))

(defn- txid->tx [txid]
  (get @tx-map txid))

(defn- tx-item->tx [tx-item]
  (let [existing-tx (get @tx-map (+txid+ tx-item))]
    (assoc existing-tx

      :fully-applied-request-versions
      (:fully-applied-request-versions existing-tx)

      :requests-map
      (:requests-map existing-tx))))

(defn- update-tx-map!
  ([tx-item]
     (update-tx-map! tx-item nil))
  ([tx-item kv-map]
     (let [txid (+txid+ tx-item)]
       (dosync (alter tx-map assoc txid tx-item)
               (doseq [[k v] kv-map]
                 (alter tx-map assoc-in `[~txid ~@k] v))))))

(defmacro with-updating-tx-map-on-success [[& {:keys [transformer] :or {transformer 'identity}}] & body]
  `(let [tx-item# (do ~@body)]
     (utils/tx-assert (contains? tx-item# +txid+) "Unexpected result for with-updating-tx-map-on-success")
     (update-tx-map! (~transformer tx-item#))
     tx-item#))

(defn- transaction->tx-item [tx]
  (dissoc tx :fully-applied-request-versions))

(defn- fully-applied-request-versions [txid]
  (:fully-applied-request-versions (txid->tx txid)))

(defn- get-tx-requests [txid]
  (mapcat (fn [[_ map]] (vals map)) (:requests-map (txid->tx txid))))

(defn- insert-and-return-tx-item
  "Insert a new transaction item with txid into transaction table"
  [txid]
  ;; make-transaction uses this function and tx-map update happens in make-transaction
  (with-tx-attributes []
    (let [item {+txid+ txid +state+ +pending+ +version+ 1 +date+ (get-current-time)}]
      (dl/put-item @tx-table-name
                   item
                   :expected {+txid+ false})
      (assoc item :fully-applied-request-versions #{} :requests-map {}))))

(defn get-tx-item [txid]
  ;; tx-item {... :load-requests {:table1 {prim-keys1 request1} :table2 {prim-keys2 request2}}}
  (with-tx-attributes []
    (with-updating-tx-map-on-success [:transformer tx-item->tx]
      (let [tx-item (dl/get-item @tx-table-name {+txid+ txid} :consistent? true)]
        (when-not tx-item
          (utils/error "Transaction not found" {:type :transaction-not-found :txid txid}))
        tx-item))))

(defn- make-transaction
  "Make a new transaction. Transaction = tx-item + fully-applied-request-versions"
  ([]
     (make-transaction (make-txid)))
  ([tx-id-or-item]
     (with-updating-tx-map-on-success []
       (let [result (utils/type-case tx-id-or-item
                       String           (make-transaction (insert-and-return-tx-item tx-id-or-item))
                       clojure.lang.IPersistentMap tx-id-or-item
                       :else (utils/error "Unexpected argument for make-transaction" {:arg tx-id-or-item}))]
         result))))

(defn- valid-return-value? [rv]
  (or (nil? rv) (contains? #{:all-old :all-new :none} rv)))

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




;;;
;;; Transaction logic
;;;
(defn- mark-committed-or-rolled-back [txid state]
  (utils/tx-assert (contains? #{+committed+ +rolled-back+} state) "Unexpected state in (mark-committed-or-rolled-back)" state :txid txid)
  (with-updating-tx-map-on-success [:transformer tx-item->tx]
    (let [version (get-in @tx-map [txid +version+])
          result (dl/update-item @tx-table-name {+txid+ txid}
                                 {+state+ [:put state] +date+ [:put (get-current-time)]}
                                 :return :all-new
                                 :expected (cond-> {+state+ +pending+ +finalized+ false}
                                                   (not (nil? version)) (merge {+version+ version})))]
      (utils/tx-assert result "Unexpected null tx item after committing" :state state :txid txid)

      result)))

(defn- transaction-completed? [txid]
  (let [tx (txid->tx txid)
        finalized? (contains? tx +finalized+)
        state (+state+ tx)]
    (when finalized?
      (utils/tx-assert (contains? #{+committed+ +rolled-back+} state)
                       "Unexpected terminal state for completed tx" :state state :tx tx))
    finalized?))

(defn- update-tx-request-map [tx-item-from-db request]
  ;; request-map = {:table1 {prim-k1 v1} ...}
  (let [kvs (prim-kvs request)
        {:keys [table op]} request]
    (let [{existing-op :op} (get-in map [table kvs])]
      ;; write op always win
      ;; only first get op win when more one get op occurs
      ;; multiple write ops are not allowed
      (cond (or (nil? existing-op)
                ;; overwrite
                (and (= existing-op :get-item) (not= op :get-item)))
            (update-tx-map! (tx-item->tx tx-item-from-db) ;; copy tx attributes
                            {[:requests-map table kvs :version] (+version+ tx-item-from-db)})

            (and (not= existing-op :get-item) (not= op :get-item))
            (utils/error "An existing request other than :get-item found!"
                         {:type :duplicate-request :tx-item tx-item-from-db :request request})))))

(defn- update-tx-with-request [txid request]
  (let [tx (txid->tx txid)
        current-version (+version+ tx)]
    (try (let [new-tx-item (dl/update-item @tx-table-name
                                           {+txid+ txid}
                                           {+requests+   [:add #{request}]
                                            +version+    [:add 1]
                                            +date+       [:put (get-current-time)]}
                                           :expected     {+state+ +pending+ +version+ current-version}
                                           :return :all-new)
               new-version (get new-tx-item +version+)]
           ;; tx-item update is successful. Now update request-map, request version, etc
           ;; update-tx-request-map updates tx-map
           (utils/tx-assert (= new-version (inc current-version)) "Unexpected version number from update result")
           (update-tx-request-map new-tx-item request)
           new-tx-item)
         (catch AmazonServiceException e
           (utils/error "Unexpected AmazonServiceException. Updating failed" {:type :amazon-service-error :error e})))))

(defn- release-read-lock [table keys txid]
  (try (dl/update-item table keys {+txid+ [:delete] +date+ [:delete]}
                       :expected {+txid+ txid +transient+ false +applied+ false})
       (catch ConditionalCheckFailedException _
         (try (dl/delete-item table keys :expected {+txid+ txid +transient+ true +applied+ false})
              (catch ConditionalCheckFailedException _
                (let [item (dl/get-item table keys :consistent? true)]
                  (utils/tx-assert (not (and (not (empty? item)) (= (get item +txid+) txid) (contains? item +applied+)))
                                   "Item should not have been applied. Unable to release lock item" item)))))))

(defmulti unlock-item-after-commit-using-op :op)

(defn- %put-or-update-request-unlock [txid req]
  ;; after put or update, item still exists - just remove TX attributes
  (dl/update-item (:table req) (prim-kvs req)
                  {+txid+ [:delete] +transient+ [:delete] +applied+ [:delete] +date+ [:delete]}
                  :expected {+txid+ txid}))

(defmethod unlock-item-after-commit-using-op :update-item [req txid]
  (%put-or-update-request-unlock txid req))

(defmethod unlock-item-after-commit-using-op :put-item [req txid]
  (%put-or-update-request-unlock txid req))

(defmethod unlock-item-after-commit-using-op :delete-item [req txid]
  ;; delete is delete
  (dl/delete-item (:table req) (prim-kvs req) :expected {+txid+ txid}))

(defmethod unlock-item-after-commit-using-op :get-item [req txid]
  (release-read-lock txid (:table req) (prim-kvs req)))

(defn- unlock-item-after-commit [txid req]
  (try (unlock-item-after-commit-using-op req txid)
       (catch ConditionalCheckFailedException _)))

(defn- delete-item-image [txid version]
  (dl/delete-item @image-table-name {+image-id+ (str txid "#" version)}))

(defn- finalize-transaction [txid expected-current-state]
  (utils/tx-assert (contains? #{+committed+ +rolled-back+} expected-current-state))
  (let [now (get-current-time)]
    (with-updating-tx-map-on-success []
      (try (do (dl/update-item @tx-table-name {+txid+ txid} {+finalized+ [:put true] +date+ [:put now]}
                               :expected {+state+ expected-current-state})
               ;; hand craft tx-item (update-item returns nothing)
               (merge (txid->tx txid) {+finalized+ true +date+ now}))
           (catch ConditionalCheckFailedException _
             (try (let [tx-item (get-tx-item txid)]
                    (when-not (transaction-completed? txid)
                      (utils/error "Expected the transaction to be completed (no item), but there was one."))
                    tx-item)
                  (catch ExceptionInfo e (when (not= (:type (ex-data e)) :transaction-not-found)
                                           (utils/error e)))))))))

(defn- post-commit-cleanup [txid] ;; doCommit
  (let [tx-item (txid->tx txid)
        state   (+state+ tx-item)
        txid    (+txid+ tx-item)
        requests (get-tx-requests txid)]
    (utils/tx-assert (= state +committed+)
                     "Unexpected state instead of COMMITTED" :state state :tx-item tx-item)
    (doseq [request requests]
      (unlock-item-after-commit txid request))

    (doseq [request requests]
      (delete-item-image txid (:version request)))

    (finalize-transaction txid +committed+)))


(defn- rollback-item-and-release-lock [txid request]
  (let [expected {+txid+ txid}
        put-or-update-op (fn []
                           (dl/update-item (:table request) (prim-kvs request)
                                           {+txid+ [:delete] +transient+ [:delete] +applied+ [:delete] +date+ [:delete]}
                                           :expected expected))]
    (try (case (:op request)
           :put-item    (put-or-update-op)
           :update-item (put-or-update-op)
           :delete-item (dl/delete-item (:table request) (prim-kvs request) :expected expected)
           :get-item    (release-read-lock txid (:table request) (prim-kvs request)))
         (catch ConditionalCheckFailedException _))))


(defn- post-rollback-cleanup [txid]
  (let [tx-item (txid->tx txid)]
    (utils/tx-assert (= (+state+ tx-item) +rolled-back+)
                     "Transaction state is not ROLLED_BACK" :state (+state+ tx-item) :tx-item tx-item)
    (doseq [request (get-tx-requests txid)]
      (rollback-item-and-release-lock txid request)
      (delete-item-image txid (:version request)))
    (finalize-transaction txid +rolled-back+)))

;;;;;;;;;;;;;;;;;;;;





(defn- rollback-using-txid [txid] ;; rollback
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
    (condp = (+state+ tx-item)
      +committed+ (do (when-not (transaction-completed? tx-item) (post-commit-cleanup txid))
                      (utils/error "Transaction commited (instead of rolled-back)"
                               {:type :transaction-committed :txid txid}))
      +rolled-back+ (when-not (transaction-completed? tx-item) (post-rollback-cleanup tx-item))
      (utils/tx-assert false "Unexpected state in (rollback-tx tx)" :state (+state+ tx-item) :txid txid))

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
(defn- ensure-grabbing-all-locks [tx-atom]
  (let [fully-applied-request-versions (:fully-applied-request-versions @tx-atom)]
    (doseq [request (get-tx-requests (+txid+ @tx-atom))]
      (when-not (contains? fully-applied-request-versions (:version request))
        (add-request-to-transaction tx-atom request true item-lock-acquire-attempts)))))

(defn- save-item-image [request item]
  (when-not (or (= (:op request) :get-item) (contains? item +applied+) (contains? item +transient+))
    ;;(utils/tx-assert (= (get item +txid+) (:txid request)) "This will never happen in the real world!")
    ;;(utils/tx-assert (nil? (get item +image-id+)) "This will never happen in the real world!")
    (let [image-id (str (+txid+ item) "#" (:version request))]
      (try (dl/put-item @image-table-name (merge item {+image-id+ image-id +txid+ (+txid+ item)}) :expected {+image-id+ false})
           ;; Already exists! Ignore!
           (catch ConditionalCheckFailedException _)))))

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
         (prim-kvs request)
         (assoc (:update-map request) +applied+ [:put true])
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
      (if-let [attrs-to-get (get-in request [:opts :attrs])]
        (select-keys locked-item attrs-to-get)
        locked-item))))

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
        txid (+txid+ tx-item)
        request-atom (atom request)]
    (if fight-for-a-lock?
      (utils/tx-assert (= (+state+ tx-item) +pending+)
                       "To fight for a lock, TX must be in pending state" :state (+state+ tx-item))
      (loop [i num-lock-acquire-attempts item nil]
        (cond (<= i 0)
              (utils/error "Unable to add request to transaction - too much contention for the tx record"
                           {:type :transaction-exception :txid txid})
              item item
              :else
              (do (ensure-grabbing-all-locks tx)
                  (let [item (try (update-tx-with-request tx-item request-atom) ;; This call ADDS a VERSION to the request
                                  (catch ConditionalCheckFailedException _
                                    ;; TX changed unexpectedly. Check its state
                                    (let [current-state (get (get-tx-item txid) +state+)]
                                      (when (not= current-state +pending+)
                                        (utils/error "Attempted to add a request to a transaction that was not in PENDING state "
                                                     {:type :transaction-not-in-pending-state :txid txid})))))]
                    (recur (dec i) item))))))
    ;; When the item locked, save it to the item image
    (let [item  (lock-item txid @request-atom true item-lock-acquire-attempts)
          _     (save-item-image @request-atom item)
          tx-item (try (get-tx-item txid)
                       (catch ExceptionInfo e
                         (release-read-lock (:table request) (prim-kvs request) txid)
                         (utils/error e)))]
      (condp = (+state+ tx-item)
        +committed+        (do (post-commit-cleanup txid)
                               (utils/error "The transaction already committed"
                                            {:type :transaction-commited :txid txid}))
        +rolled-back+      (do (post-rollback-cleanup tx-item)
                               (utils/error "The transaction already rolled back"
                                            {:type :transaction-rolled-back :txid txid}))
        +pending+          nil
        (utils/error "Unexpected state(update-tx-with-request)" {:type :unknown-completed-transaction :state (+state+ tx-item)}))
      (let [final-item (apply-and-keep-lock @request-atom item)]
        (when-not (nil? (:version request))
          ;; FIXME: new ref & alter
          (dosync (alter tx-map update-in [txid :fully-applied-request-versions] conj (:version request)))
   ;;       (swap! tx assoc :fully-applied-request-versions (conj (:fully-applied-request-versions @tx) (:version request))))
        final-item))))

(defn- stirp-special-attributes [item]
  (apply dissoc item special-attributes))

(defn- attempt-to-add-request-to-tx [tx request]
  ;;
  ;; Repeatedly try to add this request to the tx.
  ;; If it fails because the 'item' of the request is already in a different transaction,
  ;; tries to rollback the other transaction.
  ;;
  (loop [i tx-lock-contention-resolution-attempts last-conflict nil]
    (if (<= i 0)
      (utils/error last-conflict)
      (let [[item ex] (try [(add-request-to-transaction tx request (< i tx-lock-contention-resolution-attempts) tx-lock-acquire-attempts)
                            nil]
                           (catch clojure.lang.ExceptionInfo e
                             (when (> i 1) ;; avoid unnecessary rollback
                               (if-let [other-txid (:txid (ex-data e))]
                                 (utils/ignore-errors (rollback-using-txid other-txid))
                                 (utils/error e)))
                             [nil e]))]
        (if ex
          (recur (dec i) ex)
          (stirp-special-attributes item))))))

(defn- validate-special-attributes-exclusion [attributes]
  (when (some #(contains? special-attributes %) attributes)
    (utils/error "Return must not contain any of reserved attributes"
             {:invalid-attributes (filter #(contains? special-attributes %) attributes)})))

(defn- validate-write-item-arguments [key-map opts]
  (when opts
    (when-not (valid-return-value? (:return opts))
      (utils/error "Return value has to be one :all-old, :all-new, and :none"
                   {:type :invalid-request :return (:return opts)}))
    ;; FIXME: dynohub does not support some option args
    ;;            (:return-icm is not in dynohub code)
    ;;            Revise dynohub!
    (let [invalid-opts (select-keys opts [:return-cc? :return-icm :expected])]
      (when-not (empty? invalid-opts)
        (utils/error "Not supported options" {:type :invalid-request :invalid-options (keys invalid-opts)}))))
  (validate-special-attributes-exclusion (keys key-map)))

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
       (let [result# (do ~@body)]
         (commit *current-tx*)
         (delete @*current-tx*)
         result#))))

(defn item-locked? [item]
  (contains? item +txid+))

(defn item-applied? [item]
  (contains? item +applied+))


(defn is-transient [item]
  (contains? item +transient+))

(defn- delete-tx-item [txid]
  (dl/delete-item @tx-table-name {+txid+ txid} :expected {+finalized+ true}))

(defn delete
  ([tx] (delete tx -1000))
  ([tx timeout]
     (letfn [(get-completed-tx-item [tx]
               (let [tx-item (:tx-item tx)
                     txid (:txid tx)]
                 (when-not (transaction-completed? tx-item)
                   (try (let [tx-item (get-tx-item txid)]
                          (or (transaction-completed? tx-item)
                              (utils/error "You can only delete a transaction that is completed"
                                           {:type :transaction-exception :tx-item tx-item}))
                          tx-item)
                        (catch ExceptionInfo e
                          (case (type e)
                            :transaction-not-found nil ;; deleted
                            (utils/error e)))))))]
       (let [tx (if (instance? clojure.lang.Atom tx)
                  @tx
                  tx)]
         (when-let [tx-item (get-completed-tx-item tx)]
           (when (< (+ (+date+ tx-item) timeout) (get-current-time))
             (delete-tx-item (:txid tx))))))))

(defn sweep [tx rollback-timeout delete-timeout]
  (let [tx (if (instance? clojure.lang.Atom tx)
                  @tx
                  tx)
        tx-item (:tx-item tx)]
    (if (transaction-completed? tx-item)
      (delete tx-item delete-timeout)
      (let [state (+state+ tx-item)
            rollback-fn (fn []
                          (try (rollback-using-txid (:txid tx))
                               (catch ExceptionInfo e
                                 (when-not (= (type (ex-data e)) :transaction-completed)
                                   (utils/error e)))))]
        (cond (= state +pending+)
              (when (< (+ (+date+ tx-item) rollback-timeout) (get-current-time))
                (rollback-fn))

              (or (= state +committed+) (= state +rolled-back+))
              (try (rollback-fn)
                   (catch ExceptionInfo e
                     (when-not (:type (ex-data e) :transaction-completed)
                       (utils/error e))))

              :else
              (utils/error (str "Unexpected state in transaction: " state)))))))

(defn commit [tx-atom] ;; commit
  (let [txid (:txid @tx-atom)]
    (loop [i item-commit-attempts success? false]
      (cond success? :success

            (<= i 0) (utils/error (str "Unable to commit transaction after " (inc item-commit-attempts) " attempts")
                                  {:type :transaction-exception :txid txid})

            :else
            (let [tx-item (try (get-tx-item txid)
                               (catch ExceptionInfo e
                                 (if (= (type (ex-data e)) :transaction-not-found)
                                   (utils/error "In transaction 'commited' attempt, transaction either rolled back or committed"
                                                {:type :unknown-completed-transaction :txid txid})
                                   (utils/error e))))]

              (let [completed? (transaction-completed? tx-item)
                    state      (+state+ tx-item)
                    success?   (cond (and completed? (= state +committed+)) true

                                     (and completed? (= state +rolled-back+))
                                     (utils/error "Transaction was rolled back"
                                                  {:type :transaction-rolled-back :txid txid})

                                     completed?   (utils/error (str "Unexpected state for transaction: "
                                                                    (+state+ tx-item)))

                                     (= state +committed+) (do (post-commit-cleanup txid)
                                                               true)

                                     (= state +rolled-back+)
                                     (do (post-rollback-cleanup tx-item)
                                         (utils/error "Transaction was rolled back"
                                                      {:type :transaction-rolled-back :txid txid}))
                                     :else false)]
                (ensure-grabbing-all-locks tx-atom)
                (try (mark-committed-or-rolled-back txid +committed+)
                     (catch ConditionalCheckFailedException _ false))
                (recur (dec i) success?)))))))

(defn get-item [table prim-kvs & opts]
  (validate-special-attributes-exclusion (:keys prim-kvs))
  (attempt-to-add-request-to-tx *current-tx* (cond-> {:op :get-item :table table :prim-kvs prim-kvs}
                                                     opts (assoc :opts (apply hash-map opts)))))

(defn- get-item-uncommitted [table prim-kvs opts-map]
  (let [item (dl/get-item table prim-kvs opts-map)]
    (if (and (+transient+ item) (not (+applied+ item)))
      nil
      item)))

;; (defn- get-item-committed [table prim-kvs opts-map]
;;   (loop [i 3]
;;     (if (<= i 0)
;;       (utils/error "Ran out of attempts to get a committed image of the item")
;;       (let [item (dl/get-item table prim-kvs opts-map)
;;             owner (+txid+ item)]
;;         (if (or (nil? item) (+transient+ item) (not (+applied+ item)) (nil? owner))
;;           item
;;           (let [tx-item (get-tx-item owner)]
;;             (if (= (+state+ tx-item) +committed+)
;;               item
;;               (let [locking-request-op (get-in (:requests-map @*current-tx*) [(:table request) (prim-kvs request)])
;;                     _ (utils/tx-assert locking-request-op "Expected transaction to be locking request, but no request found for tx")
;;                     old-item (binding [*current-tx* tx-item]
;;                                (load-item-image (:version request)))]

;;       (recur (dec i)))))


(defn get-item-with-isolation-level [table prim-kvs isolation-level & opts]
  (let [opts-map (hash-map opts)
        opts-map+ (if (contains? opts-map :attr)
                    (update-in [:attr] (partial apply conj) (vec special-attributes))
                    opts-map)]
    (case isolation-level
        :uncommitted    (get-item-uncommitted table prim-kvs opts-map+)
;;        :committed      (get-item-committed table prim-kvs opts-map+))))
        )))


(defn- mutate-item [tx attributes request opts]
  (let [opts (when opts (apply hash-map opts))]
    (validate-write-item-arguments attributes opts)
    (attempt-to-add-request-to-tx tx (cond-> request
                                             opts (assoc :opts opts)))))
(defn put-item [table item & opts]
  (mutate-item *current-tx* item {:op :put-item :table table :item item :opts opts} opts))

(defn update-item [table prim-kvs update-map & opts]
  (mutate-item *current-tx* (merge prim-kvs update-map) {:op :update-item :table table :prim-kvs prim-kvs :update-map update-map} opts))

(defn delete-item [table prim-kvs & opts]
  (mutate-item *current-tx* prim-kvs {:op :delete-item :table table :prim-kvs prim-kvs :opts opts} opts))

;;; DYNOTX.CLJ ends here

;;(init-tx)
;;(begin-transaction)
;;(dl/get-item @tx-table-name {+txid+ "3ede573e-0087-4ddf-bd0b-37571cbd6b6c"})

;; (with-transaction []
;;   (put-item :tx-ex {:id "item1"})
;;   (put-item :tx-ex {:id "item2"}))

(defn foo []
  (dl/delete-table   :_image_table_)
  (dl/delete-table   :_tx_table_ )
  (dl/delete-table   :tx-ex)
  (init-tx)
  (dl/ensure-table :tx-ex [:id :s])

  (with-transaction [t1]
    (put-item :tx-ex {:id "conflictingTransactions_Item1" :which-transaction? :t1})

    (put-item :tx-ex {:id "conflictingTransactions_Item2"})

    (with-transaction []
      (put-item :tx-ex {:id "conflictingTransactions_Item1" :which-transaction? :t2-win!})
      (try (commit t1)
           (catch ExceptionInfo e
             (println "T1 was rolled back" (ex-data e))
             (sweep t1 0 0)
             (delete t1)))
      (put-item :tx-ex {:id "conflictingTransactions_Item3" :which-transaction? :t2-win!}))))

#_
(update-tx-map! {:requests-map {}, :fully-applied-request-versions #{}, :_TxId "af9d7060-8b41-4243-a488-a82e7b8bbf02", :_TxS "P", :_TxV 1, :_TxD 1403150833}
                {[:requests-map :test-table [:id "val"] :version] 10})