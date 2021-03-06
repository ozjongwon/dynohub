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
(ns ozjongwon.dynohub
  "Clojure DynamoDB client. This experimental project started from Faraday by Peter Taoussanis.
  Ref. https://github.com/ptaoussanis/faraday (Faraday),
       http://goo.gl/22QGA (DynamoDBv2 API)"
  {:author "Jong-won Choi"}
  (:require [clojure.string     :as str]
            [clojure.edn        :as edn]
            [ozjongwon.utils    :as utils])
  (:import  [clojure.lang BigInt]
            [java.util Collections]
            [java.lang Math]
            [com.amazonaws.services.dynamodbv2.model
             AttributeDefinition
             AttributeValue
             AttributeValueUpdate
             BatchGetItemRequest
             BatchGetItemResult
             BatchWriteItemRequest
             BatchWriteItemResult
             Condition
             ConsumedCapacity
             CreateTableRequest
             CreateTableResult
             DeleteItemRequest
             DeleteItemResult
             DeleteRequest
             DeleteTableRequest
             DeleteTableResult
             DescribeTableRequest
             DescribeTableResult
             ExpectedAttributeValue
             GetItemRequest
             GetItemResult
             ItemCollectionMetrics
             KeysAndAttributes
             KeySchemaElement
             ListTablesRequest
             ListTablesResult
             LocalSecondaryIndex
             LocalSecondaryIndexDescription
             GlobalSecondaryIndex
             GlobalSecondaryIndexDescription
             Projection
             ProvisionedThroughput
             ProvisionedThroughputDescription
             PutItemRequest
             PutItemResult
             PutRequest
             QueryRequest
             QueryResult
             ScanRequest
             ScanResult
             TableDescription
             UpdateItemRequest
             UpdateItemResult
             UpdateTableRequest
             UpdateTableResult
             WriteRequest
             ConditionalCheckFailedException
             InternalServerErrorException
             ItemCollectionSizeLimitExceededException
             LimitExceededException
             ProvisionedThroughputExceededException
             ResourceInUseException
             ResourceNotFoundException]
            com.amazonaws.ClientConfiguration
            com.amazonaws.auth.AWSCredentials
            com.amazonaws.auth.AWSCredentialsProvider
            com.amazonaws.auth.BasicAWSCredentials
            com.amazonaws.auth.DefaultAWSCredentialsProviderChain
            com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
            com.amazonaws.services.dynamodbv2.util.Tables
            java.nio.ByteBuffer))

(defn error [s & more]
  (throw (Exception. (apply str more))))

(defn- get-bignum-precision [x]
  (.precision (if (string? x) (BigDecimal. ^String x) (bigdec x))))

(defn- dynamo-db-number? [^Number x]
  (let [type (class x)]
    (or (contains? #{Long Double Integer Float} type)
        (and (contains? #{BigInt BigDecimal BigInteger} type)
             (<= (get-bignum-precision x) 38)))))

(defn- str->dynamo-db-num ^Number [^String s]
  (let [n (edn/read-string s)]
    (if (number? n)
      n
      (error (str "Invalid number type value " s " from DynamoDB!")))))

(defn- cartesian-product [& seqs]
  ;; From Pascal Costanza's CL code
  (letfn [(simple-cartesian-product [s1 s2]
            (for [x s1 y s2]
              (conj x y)))]
    (reduce simple-cartesian-product [[]] seqs)))

(defn- ensure-vector [e]
  (if (vector? e) e [e]))

;;;
;;; Simple binary reader/writer
;;;
(defn- sexp->byte-buffer ^ByteBuffer [sexp]
  (-> (pr-str sexp)
      (.getBytes)
      (ByteBuffer/wrap)))

(defn- byte-buffer->sexp [^ByteBuffer buf]
  (-> (.array buf)
      (String.)
      (edn/read-string)))

(def ^:dynamic *binary-writer* sexp->byte-buffer)
(def ^:dynamic *binary-reader* byte-buffer->sexp)

(defmacro with-binary-reader-writer [[& {:keys [writer reader]
                                         :or {reader 'ozjongwon.dynohub/*binary-reader* writer 'ozjongwon.dynohub/*binary-writer*}}]
                                     & body]
  `(binding [*binary-writer* ~writer *binary-reader* ~reader]
     ~@body))

(defmacro without-binary-reader-writer [[] & body]
  `(binding [*binary-writer* identity *binary-reader* identity]
     ~@body))

;;;
;;; DynamoDB interface
;;;

;; DynamoDB enum <-> Clojure keyword
;;
;; "B" "N" "S" "BS" "NS" "SS"
;; "PUT" "DELETE" "ADD"
;; "CREATING" "UPDATING" "DELETING" "ACTIVE"
;; "HASH" "RANGE"
;; "KEYS_ONLY" "INCLUDE" "ALL"
;;
(def ^:dynamic *special-enums*)
(defn- DynamoDB-enum-str->keyword ^clojure.lang.Keyword [^String s]
  (if (and (bound? #'*special-enums*)
           (contains? *special-enums* s))
    s
    (-> s
        (str/replace "_" "-")
        (str/lower-case)
        (keyword))))

(defn- keyword->DynamoDB-enum-str ^String [^String k]
  ;; A speical case which is for 'as it is' string
  k)

(defn- keyword->DynamoDB-enum-str ^String [^clojure.lang.Keyword k]
  (when k
    (or (k {:> "GT" :>= "GE" :< "LT" :<= "LE" := "EQ"})
        (-> k
            (name)
            (str/replace "-" "_")
            (str/upper-case)))))

(defmacro inline-secondary-index-description-result [d & {:keys [throughput?]}]
  `(hash-map :name       (keyword (.getIndexName ~d))
             :size       (.getIndexSizeBytes ~d)
             :item-count (.getItemCount ~d)
             :key-schema (java->clojure (.getKeySchema ~d))
             :projection (java->clojure (.getProjection ~d))
             ~@(when throughput?
                 `(:throughput (java->clojure (.getProvisionedThroughput ~d))))))

;;;
;;; Java -> Clojure Mapping
;;;

(defprotocol Java->Clojure
  (java->clojure [x]))

(defmulti make-DynamoDB-parts (fn [elem & _] elem))

(defmethod make-DynamoDB-parts :key-schema-elements [_ [hname _] [rname _]]
  (mapv (fn [[n k]] (KeySchemaElement. (name n) (keyword->DynamoDB-enum-str k)))
                            `([~hname :hash] ~@(when rname [[rname :range]]))))

(defmethod make-DynamoDB-parts :provisioned-throughput [_ {read-units :read write-units :write :as throughput}]
  (ProvisionedThroughput. (long read-units) (long write-units)))

(defmethod make-DynamoDB-parts :attribute-definitions [_  hash-keydef range-keydef lsindexes gsindexes]
  (->> `(~@hash-keydef ~@range-keydef ~@(mapcat :range-keydef lsindexes)
         ~@(mapcat :hash-keydef gsindexes) ~@(mapcat :range-keydef gsindexes))
       (apply hash-map)
       (mapv (fn [[n t :as def]]
               (assert (t #{:s :n :b :ss :ns :bs}) (str "Invalid keydef: " def))
               (AttributeDefinition. (name n) (keyword->DynamoDB-enum-str t))))))

(defmethod make-DynamoDB-parts :local-secondary-indexes [_ hash-keydef lsindexes]
  (->> lsindexes
       (map (fn [{kname :name :keys [range-keydef projection] :or {projection :all} :as index}]
              (assert (and kname range-keydef) ;; optional - projection
                      (str "Malformed local secondary index (LSI): " index))
              [(name kname)
               (make-DynamoDB-parts :key-schema-elements
                                    hash-keydef range-keydef)
               (make-DynamoDB-parts :projection projection)]))
       (mapv (fn [[n ks p]]
               (doto (LocalSecondaryIndex.)
                 (.setIndexName n)
                 (.setKeySchema ks)
                 (.setProjection p))))))

(defmethod make-DynamoDB-parts :projection [_ projection]
  (let [[projection-type non-key-attributes] (cond (keyword? projection) [projection]
                                                   (vector? projection) [:include (mapv name projection)]
                                                   :else (error "Unknown projection type(and value): " projection))]
    (utils/doto-cond (Projection.)
                     true (.setProjectionType (keyword->DynamoDB-enum-str projection-type))
                     non-key-attributes (.setNonKeyAttributes (mapv name non-key-attributes)))))

(defmethod make-DynamoDB-parts :global-secondary-indexes [_ gsindexes]
  (->> gsindexes
       (map (fn [{kname :name :keys [hash-keydef range-keydef projection throughput] :or {projection :all} :as index}]
              (assert (and kname hash-keydef throughput) ;; optionals - range-keydef,  projection
                      (str "Malformed global secondary index (GSI): " index))
              (doto (GlobalSecondaryIndex.)
                (.setIndexName (name kname))
                (.setKeySchema (make-DynamoDB-parts :key-schema-elements hash-keydef range-keydef))
                (.setProjection (make-DynamoDB-parts :projection projection))
                (.setProvisionedThroughput (make-DynamoDB-parts :provisioned-throughput throughput)))))))

(defmethod make-DynamoDB-parts :attribute-value [_ v]
  (let [a (AttributeValue.)]
    (cond
     ;; s
     (string? v) (do (assert (not (empty? v)) (str "Invalid DynamoDB value: empty string: " v))
                     (.setS a v))
     ;; n
     (dynamo-db-number? v) (.setN a (str v))

     ;; set
     (set? v) (do (assert (not (empty? v)) (str "Invalid DynamoDB value: empty set: " v))
                  (cond
                   ;; ss
                   (every? string? v) (doto a (.setSS (vec v)))
                   ;; ns
                   (every? dynamo-db-number? v) (doto a (.setNS (mapv str  v)))
                   ;; bs
                   :else (doto a (.setBS (mapv *binary-writer* v)))))
     ;; b
     :else (.setB a (*binary-writer* v)))
    a))


(defmethod make-DynamoDB-parts :attribute-values [_ prim-kvs]
  (utils/maphash (fn [[k v]]
             [(name k) (make-DynamoDB-parts :attribute-value v)])
           prim-kvs))

(defmethod make-DynamoDB-parts :expected-attribute-values [_ expected]
  (utils/maphash (fn [[k v]]
             [(name k) (ExpectedAttributeValue. (if (= v false)
                                                  false
                                                  (make-DynamoDB-parts :attribute-value v)))])
           expected))

(defmethod make-DynamoDB-parts :attribute-value-updates [_ update-map]
  (utils/maphash (fn [[k [action val]]]
                   [(name k) (AttributeValueUpdate. (when-not (nil? val) (make-DynamoDB-parts :attribute-value val))
                                                    (keyword->DynamoDB-enum-str action))])
                 update-map))

(defmethod make-DynamoDB-parts :keys-and-attributes [_ requests]
  (utils/maphash (fn [[k {:keys [prim-kvs attrs consistent?]}]]
             [(name k) (utils/doto-cond (KeysAndAttributes.)
                                        true (.setKeys (make-DynamoDB-parts :key-attributes prim-kvs))
                                        attrs (.setAttributesToGet (mapv name attrs))
                                        consistent? (.setConsistentRead  consistent?))])
           requests))

(defmethod make-DynamoDB-parts :key-attributes [_ prim-kvs]
  (letfn [(map->cartesian-product [m]
            (apply cartesian-product
                   (mapv (fn [[k v]]
                           (if (vector? v)
                             (cartesian-product [k] v)
                             (list [k v])))
                         m)))]
    (->> (if (map? prim-kvs) [prim-kvs] prim-kvs)
         (mapcat map->cartesian-product)
         (mapv #(into {} %))
         (mapv #(make-DynamoDB-parts :attribute-values %)))))

(defmethod make-DynamoDB-parts :conditions [_ conds]
  (when-not (empty? conds)
    (utils/maphash (fn [[k op-def]]
                     (let [[op v] (ensure-vector op-def)
                           condition (doto (Condition.) (.setComparisonOperator (keyword->DynamoDB-enum-str op)))]
                       (when v ;; i.e. not unary operator
                         (.setAttributeValueList condition (mapv #(make-DynamoDB-parts :attribute-value %)
                                                                 (ensure-vector v))))
                       [(name k) condition]))
                   conds)))

(defmethod make-DynamoDB-parts :write-requests [_ table-req]
  (letfn [(write-req [op m]
            (let [attr-map (make-DynamoDB-parts :attribute-values m)]
              (WriteRequest. (case op
                               :put        (PutRequest. attr-map)
                               :delete     (DeleteRequest. attr-map)))))]
    (reduce into [] (for [[op attr-maps] table-req]
                      (map #(write-req op %) attr-maps)))))

(defn- java-result->clojure-with-cc-units-meta [result map]
  (when map
    (with-meta (utils/maphash (fn [[k v]]
                          [(keyword k) (java->clojure v)])
                        map)
      {:cc-units (java->clojure (.getConsumedCapacity result))})))

(defmacro query-or-scan-result [r]
  `(hash-map :items (mapv java->clojure (.getItems ~r))
             :count (.getCount ~r)
             :cc-units (java->clojure (.getConsumedCapacity ~r))
             :last-prim-kvs (java->clojure (.getLastEvaluatedKey ~r))
             :scanned-count (.getScannedCount ~r)))

;;;
;;; Implementation of java->clojure
;;;
(extend-protocol Java->Clojure
  nil (java->clojure [_] nil)

  java.util.ArrayList (java->clojure [a] (mapv java->clojure a))

  java.util.HashMap   (java->clojure [m]
                        (utils/maphash (fn [[k v]]
                                   [(keyword k) (java->clojure v)])
                                 m))

  CreateTableResult
  (java->clojure [r] (java->clojure (.getTableDescription r)))

  KeySchemaElement
  (java->clojure [e] {:name (keyword (.getAttributeName e))
                      :type (DynamoDB-enum-str->keyword (.getKeyType e))})

  AttributeDefinition
  (java->clojure [d] {:name (keyword (.getAttributeName d))
                      :type (DynamoDB-enum-str->keyword (.getAttributeType d))})

  DeleteTableResult
  (java->clojure [r] (java->clojure (.getTableDescription r)))

  TableDescription
  (java->clojure [d]
    {:name          (keyword (.getTableName d))
     :creation-date (.getCreationDateTime d)
     :item-count    (.getItemCount d)
     :size          (.getTableSizeBytes d)
     :throughput    (java->clojure (.getProvisionedThroughput  d))
     :lsindexes     (java->clojure (.getLocalSecondaryIndexes  d))
     :gsindexes     (java->clojure (.getGlobalSecondaryIndexes d))
     :status        (DynamoDB-enum-str->keyword (.getTableStatus d))
     :prim-keys (merge-with merge
                                (reduce-kv (fn [m _ v] (assoc m (:name v) {:key-type  (:type v)}))
                                           {} (java->clojure (.getKeySchema d)))
                                (reduce-kv (fn [m _ v] (assoc m (:name v) {:data-type (:type v)}))
                                           {} (java->clojure (.getAttributeDefinitions d))))})

  ProvisionedThroughputDescription
  (java->clojure [d] {:read                (.getReadCapacityUnits d)
                      :write               (.getWriteCapacityUnits d)
                      :last-decrease       (.getLastDecreaseDateTime d)
                      :last-increase       (.getLastIncreaseDateTime d)
                      :num-decreases-today (.getNumberOfDecreasesToday d)})

  LocalSecondaryIndexDescription
  (java->clojure [d] (inline-secondary-index-description-result d))

  GlobalSecondaryIndexDescription
  (java->clojure [d] (inline-secondary-index-description-result d :throughput? true))

  Projection
  (java->clojure [p] {:projection-type    (.getProjectionType p)
                      :non-key-attributes (.getNonKeyAttributes p)})
  AttributeValue
  (java->clojure [av] (or (.getS av)
                          (some->> (.getN  av) str->dynamo-db-num)
                          (some->> (.getSS av) (into #{}))
                          (some->> (.getNS av) (mapv str->dynamo-db-num) (into #{}))
                          (some->> (.getBS av) (mapv *binary-reader*) (into #{}))
                          (some->> (.getB  av) *binary-reader*)) ; Last, may be falsey
      )

  ConsumedCapacity
  (java->clojure [cc] (some-> cc (.getCapacityUnits)))

  GetItemResult
  (java->clojure [r] (java-result->clojure-with-cc-units-meta r (.getItem r)))

  PutItemResult
  (java->clojure [r] (java-result->clojure-with-cc-units-meta r (.getAttributes r)))

  UpdateItemResult
  (java->clojure [r] (java-result->clojure-with-cc-units-meta r (.getAttributes r)))

  DeleteItemResult
  (java->clojure [r] (java-result->clojure-with-cc-units-meta r (.getAttributes r)))

  DescribeTableResult
  (java->clojure [r] (java->clojure (.getTable r)))

  BatchGetItemResult
  (java->clojure [r]
    {:items       (java->clojure (.getResponses r))
     :unprocessed (.getUnprocessedKeys r)
     :cc-units    (java->clojure (.getConsumedCapacity r))})

  QueryResult
  (java->clojure [r]
    (query-or-scan-result r))

  ScanResult
  (java->clojure [r]
    (query-or-scan-result r))

  BatchWriteItemResult
  (java->clojure [r]
    {:unprocessed (.getUnprocessedItems r)
     :cc-units    (java->clojure (.getConsumedCapacity r))})
  )

;;;;
;;;; Public API for Amazon DynamoDB operations
;;;;

(defonce cc-total (keyword->DynamoDB-enum-str :total))

(defn- %merge-results [l r]
  (fn [l r] (cond (number? l) (+    l r)
                  (vector? l) (into l r)
                  :else               r)))

(defn- merge-more
  "Enables auto paging for batch batch-get/write, query/scan requests - useful for throughput limitations."
  ([more-f span-reqs last-result]
     (merge-more more-f span-reqs last-result nil))
  ([more-f {max-reqs :max :keys [throttle-ms]} last-result limit]
     (loop [{:keys [items unprocessed last-prim-kvs scanned-count] :as last-result} last-result idx 1]
       (assert items "PROGRAMMING ERROR! - ASK REVERT BACK to PREVIOUS CODE")
       (let [more (or unprocessed last-prim-kvs)]
         (if (or (and limit (<= limit scanned-count)) ;; limit & query/scan
                 (empty? more) (nil? max-reqs) (>= idx max-reqs))
           (with-meta items (dissoc last-result :items))
           (do (when throttle-ms (Thread/sleep throttle-ms))
               (recur (merge-with %merge-results last-result (more-f more))
                      (inc idx))))))))

;;
;; Client
;;
(defn- %db-client ^AmazonDynamoDBClient [{:keys [access-key secret-key endpoint] :as opts}]
  (if (empty? opts)
    (AmazonDynamoDBClient.)
    (let [^AWSCredentials credentials (if access-key
                                        (BasicAWSCredentials. access-key secret-key)
                                        (DefaultAWSCredentialsProviderChain.))]
      (utils/doto-cond (AmazonDynamoDBClient. credentials)
                       endpoint (.setEndpoint endpoint)))))

(defonce db-client (utils/bounded-memoize %db-client 64)) ;; 64 different cachedoptions

;;
;; Managing Tables
;;

(defn describe-table
  "Returns a map describing a table, or nil if the table doesn't exist."
  [client-opts table]
  (try (java->clojure (.describeTable (db-client client-opts)
                                      (DescribeTableRequest. (name table))))
       (catch ResourceNotFoundException _ nil)))

(defn create-table
  "Creates a table with options:
    hash-keydef   - [<name> <#{:s :n :ss :ns :b :bs}>].
    :range-keydef - [<name> <#{:s :n :ss :ns :b :bs}>].
    :throughput   - {:read <units> :write <units>}.
    :lsindexes    - [{:name _ :range-keydef _
                      :projection #{:all :keys-only [<attr> ...]}}].
    :gsindexes    - [{:name _ :hash-keydef _ :range-keydef _
                      :projection #{:all :keys-only [<attr> ...]}
                      :throughput _}].
    :block?       - Block for table to actually be active?

  Additional examples (using dynolite) :

        (dl/create-table :employee1 [:site-uid :s]
				     :range-keydef [:uid :s]
				     :gsindexes [{:name :fname-index :hash-keydef [:emailAddress :s]
                                                 :range-keydef [:familyName :s]
                                                 :projection [:firstName :paymentSchedule :phoneNumber :dateEmployment]
                                                 :throughput {:read 1 :write 1}}])

        (dl/create-table :employee [:site-uid :s]
				     :range-keydef [:uid :s]
				     :lsindexes [{:name :family-name-index :range-keydef [:familyName :s]
                        			 :projection [:firstName :emailAddress :phoneNumber :dateEmployment]}])
"
  [client-opts table-name hash-keydef
   & {:keys [range-keydef throughput lsindexes gsindexes block?]
      :or   {throughput {:read 1 :write 1}} :as opts}]
  (let [table-name (name table-name)
        client (db-client client-opts)
        result (.createTable client
                  (utils/doto-cond (CreateTableRequest. (make-DynamoDB-parts :attribute-definitions
                                                                             hash-keydef range-keydef lsindexes gsindexes)
                                                        table-name
                                                        (make-DynamoDB-parts :key-schema-elements
                                                                             hash-keydef range-keydef)
                                                        (make-DynamoDB-parts :provisioned-throughput
                                                                             throughput))
                                   lsindexes (.setLocalSecondaryIndexes (make-DynamoDB-parts :local-secondary-indexes
                                                                                             hash-keydef lsindexes))
                                   gsindexes (.setGlobalSecondaryIndexes (make-DynamoDB-parts :global-secondary-indexes
                                                                                              gsindexes))))]
    (if block?
      (do (Tables/waitForTableToBecomeActive client table-name)
          (describe-table client-opts table-name))
      (java->clojure result))))

(defn- throughput-series [[r1 w1] [r2 w2 :as target]]
  (letfn [(next-n [n1 n2]
            (if (> n1 n2)
              n2
              (min n2 (* 2 n1))))]
    (loop [[r w :as r-w] [r1 w1] result []]
      (if (= r-w target)
        result
        (let [ns [(next-n r r2) (next-n w w2)]]
          (recur ns (conj result ns)))))))

(defn update-table
  [client-opts table throughput & {:keys [span-reqs block?]
                                   :or   {span-reqs {:max 5 :block? false}}}]

  (let [{arg-read :read arg-write :write} throughput
        {:keys [status throughput]} (describe-table client-opts table)
        _ (assert (= status :active) (str "Table status is not active but " status))
        {:keys [read write num-decreases-today]} throughput
        _ (assert (or (and (< read arg-read) (< write arg-write))
                      (< num-decreases-today 4))
                  (str "Max 4 decreases per 24hr period " (seq (interpose " " [arg-read read arg-write write num-decreases-today]))))
        throughput-series (throughput-series [read write] [arg-read arg-write])
        _ (assert (<= (count throughput-series) (:max span-reqs))
                  (str "Got max-reqs " (:max span-reqs) " needs at least " (count throughput-series)))
        client (db-client client-opts)
        table-name (name table)]
      (letfn [(request-update [throughput]
                (.updateTable client
                              (UpdateTableRequest. table-name
                                                   (make-DynamoDB-parts :provisioned-throughput throughput)))
                (Tables/waitForTableToBecomeActive client table-name))
              (update-repeatedly []
                (loop [[[r w] & series] throughput-series]
                  (let [result (request-update {:read r :write w})]
                    (if (empty? series)
                      (describe-table client-opts table-name)
                      (recur series)))))]
        (if block?
          (update-repeatedly)
          (future (update-repeatedly))))))

(defn list-tables "Returns a vector of table names."
  [client-opts]
  (->> (db-client client-opts) (.listTables) (.getTableNames) (mapv keyword)))

(defn delete-table [client-opts table]
  (java->clojure (.deleteTable (db-client client-opts) (DeleteTableRequest. (name table)))))

;;
;; Filter expression, etc
;;
(defn- exp-alias [n prefix]
  (let [n (name n)]
    (if (identical? (get n 0) prefix)
      n
      (str prefix n))))

(defn- alias-attr->map [aliases prefix val-conv]
  (utils/maphash (fn [[f s]]
                   [(exp-alias f prefix) (val-conv s)])
                 aliases))

(defn- alias-attr-value-map->exp-map [aliases]
  (alias-attr->map aliases \: identity))

(defn- alias-attr-name-map->exp-map [aliases]
  (alias-attr->map aliases \# name))

(defmulti op->filter-exp-str (fn [op & _]
                               (cond (contains? #{:exists :not-exists :begins-with :contains} op)       :function
                                     (contains? #{:= :<> :< :<= :> :>=} op)                             :comparator
                                     (contains? #{:and :or} op)                                         :and-or
                                     :else op)))

(def ^:dynamic *name-aliases*)

(def ^:dynamic *value-aliases*)

(defn filter-exp->filter-exp-str [exp & {:keys [name-aliases value-aliases]}]
  (when (and name-aliases value-aliases)
    (assert (empty? (select-keys name-aliases (keys value-aliases))) (str "Duplicated aliases in :alias-attr-name-map and :alias-attr-value-map - " (keys (select-keys name-aliases (keys value-aliases))))))
  (binding [*name-aliases* (if (bound? #'*name-aliases*)
                             *name-aliases*
                             name-aliases)
            *value-aliases* (if (bound? #'*value-aliases*)
                              *value-aliases*
                              value-aliases)]
    (if (coll? exp)
      (op->filter-exp-str (first exp)
                          (map (fn [x]
                                 (filter-exp->filter-exp-str x name-aliases value-aliases))
                               (rest exp)))
      exp)))

(defn- maybe-substitute-name-alias [n & not-found-value]
  (if (get *name-aliases* n)
    (exp-alias n \#)
    (first not-found-value)))

(defn- maybe-substitute-name-aliases [attrs alias-map]
  (map #(binding [*name-aliases* alias-map]
          (maybe-substitute-name-alias % (name %))) attrs))

(defn projection-attrs->exp [attrs alias-map]
  (->> (maybe-substitute-name-aliases attrs alias-map)
       (interpose ",")
       (apply str)))

(defn- maybe-substitute-value-alias [v & not-found-value]
  (if (get *value-aliases* v)
    (exp-alias v \:)
    (first not-found-value)))

(defn- substitute-value-alias [v]
  (let [result (maybe-substitute-value-alias v)]
    (assert result (str v " must be valid alias value"))
    result))

(defn- maybe-substitute-aliases [col]
  (for [nv col]
    (or (maybe-substitute-name-alias nv)
        (maybe-substitute-value-alias nv)
        (and (keyword? nv) (name nv))
        nv)))

(defmethod op->filter-exp-str :default [arg args]
  (cons arg args))

(defn- %comparator-and-or [op args & {:keys [subst-fn] :or {subst-fn identity}}]
  ;; all comparators, AND, OR
  (str "("
       (->> (-> (name op)
                clojure.string/upper-case
                (interpose (subst-fn (filter-exp->filter-exp-str args))))
            (interpose \space)
            (apply str))
       ")"))

(defmethod op->filter-exp-str :comparator [op args]
  (%comparator-and-or op args :subst-fn maybe-substitute-aliases))

(defmethod op->filter-exp-str :and-or [op args]
  (%comparator-and-or op args))

(defmethod op->filter-exp-str :not [op args]
  (assert (= (count args) 1))
  (str "(NOT " (first (filter-exp->filter-exp-str args)) ")"))

(defmethod op->filter-exp-str :between [op args]
  (assert (= (count args) 2))
  (let [[attr and-exp] args]
    (str "(" (maybe-substitute-name-alias attr (name attr)) " BETWEEN " and-exp ")")))

(defmethod op->filter-exp-str :in [op args]
  (assert (= (count args) 2))
  ;; FIXME: arg1 can be attribute name or alias
  ;;        arg2 are value aliases
  (let [[arg1 arg2] (filter-exp->filter-exp-str args)]
    (assert (coll? arg2))
    (str "(" (maybe-substitute-name-alias arg1 (name arg1)) " IN ("
         (apply str (interpose "," (map substitute-value-alias arg2)))
         "))")))

(defmethod op->filter-exp-str :function [op args]
  (let [[path & args] (filter-exp->filter-exp-str args)]
    (str (-> (name op) (str/replace "-" "_"))
         "("
         (apply str (interpose "," `(~(maybe-substitute-name-alias path (name path))
                                     ~@(map substitute-value-alias args))))
         ")")))


;;
;; Reading data
;;

(defn get-item
  "Retrieves an item from a table by its primary key with options:
    prim-kvs     - {<hash-key> <val>} or {<hash-key> <val> <range-key> <val>}.
    :attrs       - Attrs to return, [<attr> ...].
    :consistent? - Use strongly (rather than eventually) consistent reads?
    :alias-attr-name-map - {<alias> <attr>}, that can be used in :projection-attrs
    :projection-attrs - [<attr> ...]

  Additional examples (using dynolite) :

    (dl/get-item :employee {:site-id \"4w\", :id \"yVSEkgAb9dDWtA\"}
                 :alias-attr-name-map {:fn :firstName :sn :familyName :e :emailAddress :p :phoneNumber
                                     :d :dateEmployment :t :terminatedP :i :id}
                 :projection-attrs [:fn :sn :e :p :d :t :i])
"
  [client-opts table prim-kvs & {:keys [attrs consistent? return-cc? alias-attr-name-map projection-attrs]}]
  (java->clojure (.getItem (db-client client-opts)
                           (utils/doto-cond (GetItemRequest. (name table)
                                                             (make-DynamoDB-parts :attribute-values prim-kvs)
                                                             consistent?)
                                            attrs            (.setAttributesToGet (mapv name attrs))
                                            return-cc?       (.setReturnConsumedCapacity cc-total)
                                            alias-attr-name-map (.setExpressionAttributeNames (alias-attr-name-map->exp-map alias-attr-name-map))
                                            projection-attrs (.setProjectionExpression
                                                              (projection-attrs->exp projection-attrs alias-attr-name-map))))))

(defn batch-get-item
  "Retrieves a batch of items in a single request.
  Limits apply, Ref. http://goo.gl/Bj9TC.

  (batch-get-item client-opts
    {:users   {:prim-kvs {:name \"alice\"}}
     :posts   {:prim-kvs {:id [1 2 3]}
               :attrs    [:timestamp :subject]
               :consistent? true}
     :friends {:prim-kvs [{:catagory \"favorites\" :id [1 2 3]}
                          {:catagory \"recent\"    :id [7 8 9]}]}})

  :span-reqs - {:max _ :throttle-ms _} allows a number of requests to
  automatically be stitched together (to exceed throughput limits, for example)."
  [client-opts requests & {:keys [return-cc? span-reqs] :as opts
                           :or   {span-reqs {:max 5}}}]
  (letfn [(run1 [raw-req]
            (java->clojure
             (.batchGetItem (db-client client-opts)
                            (BatchGetItemRequest. raw-req cc-total))))]
    (when-not (empty? requests)
      (merge-more run1 span-reqs (run1 (make-DynamoDB-parts :keys-and-attributes requests))))))

(defn query
  "Retrieves items from a table (indexed) with options:
    prim-key-conds - {<key-attr> [<comparison-operator> <val-or-vals>] ...}.
    :last-prim-kvs - Primary key-val from which to eval, useful for paging.
    :query-filter  - {<key-attr> [<comparison-operator> <val-or-vals>] ...}.
    :logical-op    - A logical operator to apply to :query-filter, #{:and :or}
    :span-reqs     - {:max _ :throttle-ms _} controls automatic multi-request
                     stitching.
    :return        - e/o #{:all-attributes :all-projected-attributes :count
                           [<attr> ...]}.
    :index         - Name of a local or global secondary index to query.
    :order         - Index scaning order e/o #{:asc :desc}.
    :limit         - Max num >=1 of items to eval (≠ num of matching items).
                     Useful to prevent harmful sudden bursts of read activity.
    :consistent?   - Use strongly (rather than eventually) consistent reads?
    :alias-attr-name-map - {<alias> <attr>}, that can be used in filter-exp and projection-attrs.
    :alias-attr-value-map - {<alias> <val>}, that can be used in filter-exp.
    :projection-attrs - [<attr> ...]
    :filter-exp    - Prefixed expression with:
                        #{:= :<> :< :<= :> :>= :and :or :not :exists :not-exists :begins-with :contains :between :in}
                For available comparators and functions, see
http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference

  Note: the newly added :alias-attr-name-map, :alias-attr-value-map, :filter-exp, and :projection-attrs are
        Amazon's preferred way of doing filtering.
        Only one of query-filter or filter-exp has to exist in a query/scan.

  (create-table client-opts :my-table [:name :s]
    {:range-keydef [:age :n] :block? true})

  (do (put-item client-opts :my-table {:name \"Steve\" :age 24})
      (put-item client-opts :my-table {:name \"Susan\" :age 27}))
  (query client-opts :my-table {:name [:eq \"Steve\"]
                                :age  [:between [10 30]]})
  => [{:age 24, :name \"Steve\"}]

  The old style comparison-operators e/o #{:eq :le :lt :ge :gt :begins-with :between}.

  For unindexed item retrievel see `scan`.

  Ref. http://goo.gl/XfGKW for query+scan best practices.

  Additional examples (using dynolite) :

    (dl/query :employee {:site-uid [:eq \"4w\"]} :query-filter {:phoneNumber :not-null})
    (dl/query :employee {:site-uid [:eq \"4w\"]} :query-filter {:emailAddress [:contains \"Super\"]} :limit 10)
    (dl/query :employee {:site-id [:eq \"4w\"]}
              :exp-attr-name-map {\"#f\" \"firstName\" \"#s\" \"site-id\"}
              :exp-attr-val-map {\":fn\" \"Louis\" \":id\" \"4w\"}
              :filter-exp \"#f = :fn AND #s = :id\")
    (dl/query :employee {:site-id [:eq \"4w\"]}
              :index :family-name-index
              :alias-attr-name-map {:fn :firstName      :sn :familyName         :e :emailAddress
                                    :p :phoneNumber     :d :dateEmployment      :t :terminatedP         :a :active?}
              :alias-attr-value-map {:active true}
              :projection-attrs [:fn :sn :e :p :d :t :id]
              :filter-exp [:= :a :active])
"
  [client-opts table prim-key-conds
   & {:keys [last-prim-kvs query-filter logical-op span-reqs return index order limit consistent?
             return-cc? alias-attr-name-map alias-attr-value-map projection-attrs filter-exp] :as opts
      :or   {span-reqs {:max 5}
             order     :asc}}]
  (letfn [(run1 [last-prim-kvs]
            (java->clojure
             (.query (db-client client-opts)
               (utils/doto-cond (QueryRequest. (name table))
                                true (.setKeyConditions    (make-DynamoDB-parts :conditions prim-key-conds))
                                true (.setScanIndexForward (case order :asc true :desc false))
                                last-prim-kvs   (.setExclusiveStartKey (make-DynamoDB-parts :attribute-values last-prim-kvs))
                                query-filter    (.setQueryFilter (make-DynamoDB-parts :conditions query-filter))
                                logical-op      (.setConditionalOperator (keyword->DynamoDB-enum-str logical-op))

                                limit           (.setLimit     (int limit))
                                index           (.setIndexName      (name index))
                                consistent?     (.setConsistentRead consistent?)
                                (vector? return) (.setAttributesToGet (mapv name return))
                                (keyword? return) (.setSelect (keyword->DynamoDB-enum-str return))
                                return-cc?      (.setReturnConsumedCapacity cc-total)
                                alias-attr-name-map (.setExpressionAttributeNames (alias-attr-name-map->exp-map alias-attr-name-map))
                                alias-attr-value-map (.setExpressionAttributeValues
                                                      (make-DynamoDB-parts :attribute-values
                                                                           (alias-attr-value-map->exp-map alias-attr-value-map)))
                                filter-exp (.setFilterExpression (filter-exp->filter-exp-str filter-exp
                                                                                             :name-aliases alias-attr-name-map
                                                                                             :value-aliases alias-attr-value-map))
                                projection-attrs   (.setProjectionExpression
                                                    (projection-attrs->exp projection-attrs alias-attr-name-map))))))]
    (merge-more run1 span-reqs (run1 last-prim-kvs) limit)))

(defn scan
  "Retrieves items from a table (unindexed) with options:
    :attr-conds     - {<attr> [<comparison-operator> <val-or-vals>] ...}.
    :logical-op    - A logical operator to apply to :attr-conds, #{:and :or}
    :limit          - Max num >=1 of items to eval (≠ num of matching items).
                      Useful to prevent harmful sudden bursts of read activity.
    :last-prim-kvs  - Primary key-val from which to eval, useful for paging.
    :span-reqs      - {:max _ :throttle-ms _} controls automatic multi-request
                      stitching.
    :return         - e/o #{:all-attributes :all-projected-attributes :count
                            [<attr> ...]}.
    :total-segments - Total number of parallel scan segments.
    :segment        - Calling worker's segment number (>=0, <=total-segments).
    :alias-attr-name-map - {<alias> <attr>}, that can be used in filter-exp and projection-attrs.
    :alias-attr-value-map - {<alias> <val>}, that can be used in filter-exp.
    :projection-attrs - [<attr> ...]
    :filter-exp    - Prefixed expression with:
                        #{:= :<> :< :<= :> :>= :and :or :not :exists :not-exists :begins-with :contains :between :in}
                For available comparators and functions, see
http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference

  Note: the newly added :alias-attr-name-map, :alias-attr-value-map, :filter-exp, and :projection-attrs are
        Amazon's preferred way of doing filtering.
        Only one of query-filter or filter-exp has to exist in a query/scan.

  The old style comparison-operators e/o
         #{:eq :le :lt :ge :gt :begins-with :between :ne :not-null :null :contains :not-contains :in}.

  (create-table client-opts :my-table [:name :s]
    {:range-keydef [:age :n] :block? true})

  (do (put-item client-opts :my-table {:name \"Steve\" :age 24})
      (put-item client-opts :my-table {:name \"Susan\" :age 27}))
  (scan client-opts :my-table :attr-conds {:age [:in [24 27]]})
  => [{:age 24, :name \"Steve\"} {:age 27, :name \"Susan\"}]

  For automatic parallelization & segment control see `scan-parallel`.
  For indexed item retrievel see `query`.

  Ref. http://goo.gl/XfGKW for query+scan best practices.

  Additional examples (using dynolite) :

    (dl/scan :employee :attr-conds {:emailAddress [:contains \"Super\"]} :limit 100)
    (dl/scan :employee :attr-conds {:emailAddress :null})
    (dl/scan :employee
             :exp-attr-name-map {\"#f\" \"firstName\" \"#s\" \"site-id\"}
	     :exp-attr-val-map {\":fn\" \"Louis\" \":id\" \"4w\"}
	     :filter-exp \"#f = :fn AND #s = :id\")
    (dl/scan :employee
              :index :family-name-index
              :alias-attr-name-map {:fn :firstName      :sn :familyName         :e :emailAddress
                                    :p :phoneNumber     :d :dateEmployment      :t :terminatedP         :a :active?}
              :alias-attr-value-map {:active true}
              :projection-attrs [:fn :sn :e :p :d :t :id]
              :filter-exp [:= :a :active])
"
  [client-opts table
   & {:keys [attr-conds logical-op last-prim-kvs span-reqs return limit total-segments
             segment return-cc? alias-attr-name-map alias-attr-value-map projection-attrs filter-exp] :as opts
      :or   {span-reqs {:max 5}}}]
  (letfn [(run1 [last-prim-kvs]
            (java->clojure
             (.scan (db-client client-opts)
               (utils/doto-cond (ScanRequest. (name table))
                                attr-conds      (.setScanFilter        (make-DynamoDB-parts :conditions attr-conds))
                                logical-op      (.setConditionalOperator (keyword->DynamoDB-enum-str logical-op))

                                last-prim-kvs   (.setExclusiveStartKey (make-DynamoDB-parts :attribute-values last-prim-kvs))
                                limit           (.setLimit             (int limit))
                                total-segments  (.setTotalSegments     (int total-segments))
                                segment         (.setSegment           (int segment))
                                (vector? return) (.setAttributesToGet (mapv name return))
                                (keyword? return) (.setSelect (keyword->DynamoDB-enum-str return))
                                return-cc?     (.setReturnConsumedCapacity cc-total)
                                alias-attr-name-map (.setExpressionAttributeNames (alias-attr-name-map->exp-map alias-attr-name-map))
                                alias-attr-value-map (.setExpressionAttributeValues
                                                      (make-DynamoDB-parts :attribute-values
                                                                           (alias-attr-value-map->exp-map alias-attr-value-map)))
                                filter-exp (.setFilterExpression (filter-exp->filter-exp-str filter-exp
                                                                                             :name-aliases alias-attr-name-map
                                                                                             :value-aliases alias-attr-value-map))
                                projection-attrs   (.setProjectionExpression
                                                    (projection-attrs->exp projection-attrs alias-attr-name-map))))))]
    (merge-more run1 span-reqs (run1 last-prim-kvs) limit)))

;;
;; Modifying Data
;;

(defn put-item
  "Adds an item (Clojure map) to a table with options:
    :return   - e/o #{:none :all-old}.
    :expected - A map of item attribute/condition pairs, all of which must be
                met for the operation to succeed. e.g.:
                  {<attr> <expected-value> ...}
                  {<attr> false ...} ; Attribute must not exist"
  [client-opts table item & {:keys [return expected return-cc?]
                             :or   {return :none}}]
  (java->clojure
   (.putItem (db-client client-opts)
             (utils/doto-cond (PutItemRequest. (name table)
                                               (make-DynamoDB-parts :attribute-values item)
                                               (keyword->DynamoDB-enum-str return))
                              expected (.setExpected     (make-DynamoDB-parts :expected-attribute-values expected))
                              return-cc? (.setReturnConsumedCapacity cc-total)))))

(defn update-item
  "Updates an item in a table by its primary key with options:
    prim-kvs   - {<hash-key> <val>} or {<hash-key> <val> <range-key> <val>}.
    update-map - {<attr> [<#{:put :add :delete}> <optional value>]}.
    :return    - e/o #{:none :all-old :updated-old :all-new :updated-new}.
    :expected  - {<attr> <#{<expected-value> false}> ...}."
  [client-opts table prim-kvs update-map & {:keys [return expected return-cc?]
                                            :or   {return :none}}]
  (java->clojure
   (.updateItem (db-client client-opts)
     (utils/doto-cond (UpdateItemRequest. (name table)
                                          (make-DynamoDB-parts :attribute-values prim-kvs)
                                          (make-DynamoDB-parts :attribute-value-updates update-map)
                                          (keyword->DynamoDB-enum-str return))
                      expected (.setExpected      (make-DynamoDB-parts :expected-attribute-values expected))
                      return-cc? (.setReturnConsumedCapacity cc-total)))))

(defn delete-item
  "Deletes an item from a table by its primary key.
  See `put-item` for option docs."
  [client-opts table prim-kvs & {:keys [return expected return-cc?]
                                 :or   {return :none}}]
  (java->clojure
   (.deleteItem (db-client client-opts)
     (utils/doto-cond (DeleteItemRequest. (name table)
                                          (make-DynamoDB-parts :attribute-values prim-kvs)
                                          (keyword->DynamoDB-enum-str return))
                      expected (.setExpected     (make-DynamoDB-parts :expected-attribute-values expected))
                      return-cc? (.setReturnConsumedCapacity cc-total)))))

(defn batch-write-item
  "Executes a batch of Puts and/or Deletes in a single request.
   Limits apply, Ref. http://goo.gl/Bj9TC. No transaction guarantees are
   provided, nor conditional puts. Request execution order is undefined.

   (batch-write-item client-opts
     {:users {:put    [{:user-id 1 :username \"sally\"}
                       {:user-id 2 :username \"jane\"}]
              :delete [{:user-id [3 4 5]}]}})

  :span-reqs - {:max _ :throttle-ms _} allows a number of requests to
  automatically be stitched together (to exceed throughput limits, for example)."
  [client-opts requests & {:keys [return-cc? span-reqs] :as opts
                           :or   {span-reqs {:max 5}}}]
  (letfn [(run1 [raw-req]
            (java->clojure
             (.batchWriteItem (db-client client-opts)
               (utils/doto-cond (BatchWriteItemRequest. raw-req)
                                return-cc? (.setReturnConsumedCapacity cc-total)))))]
    (merge-more run1 span-reqs
      (run1
       (utils/maphash (fn [[table-name table-req]]
                  [(name table-name) (make-DynamoDB-parts :write-requests table-req)])
                requests)))))

;;; DYNOHUB.CLJ ends here
