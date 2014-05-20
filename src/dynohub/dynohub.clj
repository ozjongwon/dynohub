(ns pegotezzi.dynohub
  "Clojure DynamoDB client. This experimental project started from Faraday by Peter Taoussanis.
  Ref. https://github.com/ptaoussanis/faraday (Faraday),
       http://goo.gl/22QGA (DynamoDBv2 API)"
  {:author "Jong-won Choi"}
  (:require [clojure.string     :as str]
            [clojure.edn        :as edn]
            ;; [taoensso.encore        :as encore :refer (doto-cond)]
            ;; [taoensso.nippy.tools   :as nippy-tools]
            ;; [taoensso.faraday.utils :as utils :refer (coll?*)]
            )
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

;;;
;;; Utility functions/macros
;;;

;; LRU cache memoization

(defn lru-cache [max-size]
  ;;
  ;; With some google search, Java's LinkedHashMap sounds right.
  ;; And this one shows exactly what I want : http://books.google.com.au/books?id=I8KdEKceCHAC&pg=PA372&lpg=PA372&dq=linked+hash+map+clojure&source=bl&ots=wNnMP8U6j4&sig=8vJIPTMD9f4CwMUzKy5kxGLnzDU&hl=en&sa=X&ei=dVn_UubCKcqfkwWyoIHgCw&ved=0CG4Q6AEwCA#v=onepage&q=linked%20hash%20map%20clojure&f=false
  ;;
  ;; For size, http://www.javaranch.com/journal/2008/08/Journal200808.jsp#a3
  ;;
  (proxy [java.util.LinkedHashMap] [16 0.75 true]
    (removeEldestEntry [entry]
      (> (count this) max-size))))

(defn bounded-memoize
  "Return a bounded memoized version of fn 'f'
   that caches the last 'k' computed values"
  [f k]
  (assert (and (fn? f) (integer? k)))

  (let [lru-cache (lru-cache (dec k))]
    (letfn [(find-memo [arg]
              (-> lru-cache (.get arg)))
            (update-memo [arg new-value]
              (-> lru-cache (.put arg new-value)))]
      (fn [arg]
        (if-let [found (find-memo arg)]
          found
          (let [new-value (f arg)]
            (update-memo arg new-value)
            new-value))))))

(defmacro defmemofn [memo-size name [& args] & body]
  (let [internal-fn (symbol (str "%" name))]
    `(do (defn- ~internal-fn [~@args]
           ~@body)
         (def ~name (bounded-memoize ~internal-fn ~memo-size)))))

(defn error [s & more]
  (throw (Exception. (apply str more))))

(def mapset (comp set mapv))
(def maphash (comp (partial apply hash-map) mapcat))

(defn- get-bignum-precision [x]
  (.precision (if (string? x) (BigDecimal. ^String x) (bigdec x))))

(defn- dynamo-db-number? [x]
  (let [type (class x)]
    (or (contains? #{Long Double Integer Float} type)
        (and (contains? #{BigInt BigDecimal BigInteger} type)
             (<= (get-bignum-precision x) 38)))))

(defn- str->dynamo-db-num [^String s]
  (let [n (edn/read-string s)]
    (if (number? n)
      n
      (throw (Exception. (str "Invalid number type value " s " from DynamoDB!"))))))

(defmacro doto-cond "My own version of doto-cond"
  [exp & clauses]
  (assert (even? (count clauses)))
  (let [g (gensym)
        sexps (map (fn [[cond sexp]]
                     (if (symbol? cond)
                       `(when ~cond
                          (~(first sexp) ~g ~@(rest sexp)))
                       `(~(first sexp) ~g ~@(rest sexp))))
                   (partition 2 clauses))]
    `(let [~g ~exp]
       ~@sexps
       ~g)))

(defmacro clojure->java-using-mapv [sexp args]
  (if (vector? args)
     `(mapv #(clojure->java ~sexp %) ~args)
     `(let [args# ~args]
        (assert (vector? args#))
        (mapv #(clojure->java ~sexp %) args#))))


(defn- cartesian-product [& seqs]
  ;; From Pascal Costanza's CL code
  (letfn [(simple-cartesian-product [s1 s2]
            (for [x s1 y s2]
              (conj x y)))]
    (reduce simple-cartesian-product [[]] seqs)))

;;;;;;;;;;;;;;;;;;;;;
(def ^:private db-client*
  "Returns a new AmazonDynamoDBClient instance for the supplied client opts:
    (db-client* {:access-key \"<AWS_DYNAMODB_ACCESS_KEY>\"
                 :secret-key \"<AWS_DYNAMODB_SECRET_KEY>\"}),
    (db-client* {:creds my-AWSCredentials-instance}),
    etc."
  (memoize
   (fn [{:keys [provider creds access-key secret-key endpoint proxy-host proxy-port
               conn-timeout max-conns max-error-retry socket-timeout]
        :as client-opts}]
     (if (empty? client-opts) (AmazonDynamoDBClient.) ; Default client
       (let [creds (or creds (:credentials client-opts)) ; Deprecated opt
             _ (assert (or (nil? creds) (instance? AWSCredentials creds)))
             _ (assert (or (nil? provider) (instance? AWSCredentialsProvider provider)))
             ^AWSCredentials aws-creds
             (when-not provider
               (cond
                creds      creds ; Given explicit AWSCredentials
                access-key (BasicAWSCredentials. access-key secret-key)
                :else      (DefaultAWSCredentialsProviderChain.)))
             client-config
             (doto-cond (ClientConfiguration.)
               proxy-host      (.setProxyHost         proxy-host)
               proxy-port      (.setProxyPort         proxy-port)
               conn-timeout    (.setConnectionTimeout conn-timeout)
               max-conns       (.setMaxConnections    max-conns)
               max-error-retry (.setMaxErrorRetry     max-error-retry)
               socket-timeout  (.setSocketTimeout     socket-timeout))]
         (doto-cond (AmazonDynamoDBClient. (or provider aws-creds) client-config)
           endpoint (.setEndpoint endpoint)))))))

(defn- db-client ^AmazonDynamoDBClient [client-opts] (db-client* client-opts))

;;;
;;; Simple binary reader/writer
;;;
(defn- sexp->str [sexp]
  (-> (pr-str sexp)
      (.getBytes)
      (ByteBuffer/wrap)))

(defn- byte-buffer->sexp [^ByteBuffer buf]
  (-> (.array buf)
      (String.)
      (edn/read-string)))

(def ^:dynamic *binary-writer* sexp->str)
(def ^:dynamic *binary-reader* byte-buffer->sexp)

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
(defn- DynamoDB-enum-str->keyword [^String s]
  (-> s
      (str/replace "_" "-")
      (str/lower-case)
      (keyword)))

(defn- keyword->DynamoDB-enum-str [^clojure.lang.Keyword k]
  (-> k
      (name)
      (str/replace "-" "_")
      (str/upper-case)))

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

(defprotocol Java<->Coljure
  (java->clojure [x])
  (clojure->java [x args]))

(defmulti make-DynamoDB-parts (fn [elem & _] elem))

(defmethod make-DynamoDB-parts :key-schema-elements [_ [hname _] [rname _]]
  (clojure->java-using-mapv (KeySchemaElement.) `[[~hname :hash] ~@(when rname [[rname :range]])]))

(defmethod make-DynamoDB-parts :provisioned-throughput [_ {read-units :read write-units :write :as throughput}]
  (assert (and read-units write-units)
          (str "Malformed throughput: " throughput))
  (clojure->java (ProvisionedThroughput.) [read-units write-units]))

(defmethod make-DynamoDB-parts :attribute-definitions [_  hash-keydef range-keydef lsindexes gsindexes]
  (->> `[~@hash-keydef ~@range-keydef ~@(mapcat :range-keydef lsindexes)
         ~@(mapcat :hash-keydef gsindexes) ~@(mapcat :range-keydef gsindexes)]
       (apply hash-map)
       (mapv (fn [[n t :as def]]
               (assert (t #{:s :n :b :ss :ns :bs}) (str "Invalid keydef: " def))
               [n t]))
       (clojure->java-using-mapv (AttributeDefinition.))))

(defmethod make-DynamoDB-parts :local-secondary-indexes [_ hash-keydef lsindexes]
  (->> lsindexes
       (map (fn [{:keys [name range-keydef projection] :or {projection :all} :as index}]
              (assert (and name range-keydef projection)
                      (str "Malformed local secondary index (LSI): " index))
              [name
               (make-DynamoDB-parts :key-schema-elements
                                    hash-keydef range-keydef)
               (make-DynamoDB-parts :projection projection)]))
       (clojure->java-using-mapv (LocalSecondaryIndex.))))

(defmethod make-DynamoDB-parts :projection [_ projection]
  (clojure->java (Projection.) (cond (keyword? projection) [projection]
                                     (vector? projection) [:include (mapv name projection)]
                                     :else (error "Unknown projection type(and value): " projection))))

(defmethod make-DynamoDB-parts :global-secondary-indexes [_ gsindexes]
  (->> gsindexes
       (map (fn [{:keys [name hash-keydef range-keydef projection throughput] :or {projection :all} :as index}]
              (assert (and name hash-keydef range-keydef projection throughput)
                      (str "Malformed global secondary index (GSI): " index))
              [name
               (make-DynamoDB-parts :key-schema-elements
                                    hash-keydef range-keydef)
               (make-DynamoDB-parts :projection projection)
               (make-DynamoDB-parts :provisioned-throughput throughput)]))
       (clojure->java-using-mapv (GlobalSecondaryIndex.))))

(defmethod make-DynamoDB-parts :attribute-values [_ prim-kvs]
  (maphash (fn [[k v]]
             [(name k) (clojure->java (AttributeValue.) v)])
           prim-kvs))

(defmethod make-DynamoDB-parts :expected-attribute-values [_ expected]
  (maphash (fn [[k v]]
             [(name k) (ExpectedAttributeValue. (if (= v false)
                                                  false
                                                  (clojure->java (AttributeValue.) v)))])
           expected))

(defmethod make-DynamoDB-parts :attribute-value-updates [_ update-map]
  (when-not (empty? update-map)
    (maphash (fn [[k [action val]]]
               [(name k) (AttributeValueUpdate. (clojure->java (AttributeValue.) val)
                                                (keyword->DynamoDB-enum-str action))])
             update-map)))

(defmethod make-DynamoDB-parts :keys-and-attributes [_ requests]
  (maphash (fn [[k v]]
             [(name k) (clojure->java (KeysAndAttributes.) v)])
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

(defn- java-result->clojure-with-cc-units-meta [result map]
  (when map
    (with-meta (maphash (fn [[k v]]
                          [(keyword k) (java->clojure v)])
                        map)
      {:cc-units (java->clojure (.getConsumedCapacity result))})))

;;
;;FIXME:
;;      TODO make binary converter flexible
;;
(extend-protocol Java<->Coljure
  nil (java->clojure [_] nil)

;;  java.lang.Boolean (java->clojure [v] v)

  java.util.ArrayList (java->clojure [a] (mapv java->clojure a))

  java.util.HashMap   (java->clojure [m]
                        (maphash (fn [[k v]]
                                   [(keyword k) (java->clojure v)])
                                 m))

  CreateTableResult
  (java->clojure [r] (java->clojure (.getTableDescription r)))

  KeySchemaElement
  (java->clojure [e] {:name (keyword (.getAttributeName e))
                      :type (DynamoDB-enum-str->keyword (.getKeyType e))})
  (clojure->java [e [n t]] (doto e
                             (.setAttributeName (name n))
                             (.setKeyType (keyword->DynamoDB-enum-str t))))

  ProvisionedThroughput
  (clojure->java [pt [ru wu]] (doto pt
                                (.setReadCapacityUnits  (long ru))
                                (.setWriteCapacityUnits (long wu))))

  AttributeDefinition
  (java->clojure [d] {:name (keyword (.getAttributeName d))
                      :type (DynamoDB-enum-str->keyword (.getAttributeType d))})
  (clojure->java [ad [n t]] (doto ad
                              (.setAttributeName (name n))
                              (.setAttributeType (keyword->DynamoDB-enum-str t))))

  LocalSecondaryIndex
  (clojure->java [lsi [n ks p]] (doto lsi
                                  (.setAttributeName n)
                                  (.setKeySchema ks)
                                  (.setProjection p)))

  Projection
  (clojure->java [p [pt attrs]] (doto-cond p
                                   true  (.setProjectionType pt)
                                   (and (= pt :include) attrs) (.setNonKeyAttributes pr attrs)))

  GlobalSecondaryIndex
  (clojure->java [gsi [n ks proj prov]] (doto gsi
                                                (.setAttributeName n)
                                                (.setKeySchema ks)
                                                (.setProjection proj)
                                                (.setProvisionedThroughput prov)))

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

;;  AttributeDefinition

  LocalSecondaryIndexDescription
  (java->clojure [d] (inline-secondary-index-description-result d))

  GlobalSecondaryIndexDescription
  (java->clojure [d] (inline-secondary-index-description-result d :throughput? true))

  AttributeValue
  (clojure->java [a v]
    (cond
     ;; s
     (string? v) (do (assert (not (empty? v)) (str "Invalid DynamoDB value: empty string: " v))
                     (doto a (.setS v)))
     ;; n
     (dynamo-db-number? v) (doto a (.setN (str v)))

     (set? v) (do (assert (not (empty? v)) (str "Invalid DynamoDB value: empty set: " v))
                  (cond
                   ;; ss
                   (every? string? v) (doto a (.setSS (vec v)))
                   ;; ns
                   (every? dynamo-db-number? v) (doto a (.setNS (mapv str  v)))
                   ;; bs
                   :else (doto a (.setBS (mapv *binary-writer* v)))))
     ;;     (instance? AttributeValue v) v

     ;; b
     :else (doto a (.setB (*binary-writer* v)))))

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


  KeysAndAttributes
  (clojure->java [ka {:keys [prim-kvs attrs consistent?]}]
    (doto-cond (KeysAndAttributes.)
       attrs            (.setAttributesToGet (mapv name attrs))
       consistent?      (.setConsistentRead  consistent?)
       true             (.setKeys (make-DynamoDB-parts :key-attributes prim-kvs))))

  BatchGetItemResult
  (java->clojure [r]
    {:items       (java->clojure (.getResponses r))
     :unprocessed (.getUnprocessedKeys r)
     :cc-units    (java->clojure (.getConsumedCapacity r))})
  )

;;;
;;; Public API
;;;

(defn list-tables "Returns a vector of table names."
  [client-opts]
  (->> (db-client client-opts) (.listTables) (.getTableNames) (mapv keyword)))

(defn describe-table
  "Returns a map describing a table, or nil if the table doesn't exist."
  [client-opts table]
  (try (java->clojure (.describeTable (db-client client-opts)
                                      (doto (DescribeTableRequest.) (.setTableName (name table)))))
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
    :block?       - Block for table to actually be active?"
  [client-opts table-name hash-keydef
   & {:keys [range-keydef throughput lsindexes gsindexes block?]
      :or   {throughput {:read 1 :write 1}} :as opts}]
  (let [table-name (name table-name)
        client (db-client client-opts)
        result (.createTable client
                  (doto-cond (CreateTableRequest.)
                     true (.setTableName table-name)
                     true (.setKeySchema (make-DynamoDB-parts :key-schema-elements
                                                              hash-keydef range-keydef))
                     true (.setProvisionedThroughput (make-DynamoDB-parts :provisioned-throughput
                                                                          throughput))
                     true (.setAttributeDefinitions (make-DynamoDB-parts :attribute-definitions
                                                                         hash-keydef range-keydef lsindexes gsindexes))
                     lsindexes (.setLocalSecondaryIndexes (make-DynamoDB-parts :local-secondary-indexes)
                                                          hash-keydef lsindexes)
                     gsindexes (.setGlobalSecondaryIndexes (make-DynamoDB-parts :global-secondary-indexes
                                                                                gsindexes))))]
    (if block?
      (do (Tables/waitForTableToBecomeActive client table-name)
          ;; (describe-table table-name)
          )
      (java->clojure result))))

(defn ensure-table "Creates a table iff it doesn't already exist."
  [client-opts table-name hash-keydef & opts]
  (when-not (describe-table client-opts table-name)
    (create-table client-opts table-name hash-keydef opts)))

;;(defn update-table


(defn delete-table [client-opts table]
  (java->clojure (.deleteTable (db-client client-opts) (DeleteTableRequest. (name table)))))

(defn get-item
  "Retrieves an item from a table by its primary key with options:
    prim-kvs     - {<hash-key> <val>} or {<hash-key> <val> <range-key> <val>}.
    :attrs       - Attrs to return, [<attr> ...].
    :consistent? - Use strongly (rather than eventually) consistent reads?"
  [client-opts table prim-kvs & {:keys [attrs consistent? return-cc?]}]
  (java->clojure (.getItem (db-client client-opts)
                           (doto-cond (GetItemRequest.)
                                      true              (.setTableName (name table))
                                      true              (.setKey (make-DynamoDB-parts :attribute-values prim-kvs))
                                      consistent?       (.setConsistentRead consistent?)
                                      attrs             (.setAttributesToGet (mapv name attrs))
                                      return-cc?        (.setReturnConsumedCapacity
                                                         (keyword->DynamoDB-enum-str :total))))))

(defn put-item
  "Adds an item (Clojure map) to a table with options:
    :return   - e/o #{:none :all-old :updated-old :all-new :updated-new}.
    :expected - A map of item attribute/condition pairs, all of which must be
                met for the operation to succeed. e.g.:
                  {<attr> <expected-value> ...}
                  {<attr> false ...} ; Attribute must not exist"
  [client-opts table item & {:keys [return expected return-cc?]
                             :or   {return :none}}]
  (java->clojure
   (.putItem (db-client client-opts)
     (doto-cond (PutItemRequest.)
       true     (.setTableName    (name table))
       true     (.setItem         (make-DynamoDB-parts :attribute-values item))
       expected (.setExpected     (make-DynamoDB-parts :expected-attribute-values expected))
       return   (.setReturnValues (keyword->DynamoDB-enum-str return))
       return-cc? (.setReturnConsumedCapacity (keyword->DynamoDB-enum-str :total))))))

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
     (doto-cond (UpdateItemRequest.)
       true  (.setTableName        (name table))
       true  (.setKey              (make-DynamoDB-parts :attribute-values prim-kvs))
       true  (.setAttributeUpdates (make-DynamoDB-parts :attribute-value-updates update-map))
       expected (.setExpected      (make-DynamoDB-parts :expected-attribute-values expected))
       return   (.setReturnValues  (keyword->DynamoDB-enum-str return))
       return-cc? (.setReturnConsumedCapacity (keyword->DynamoDB-enum-str :total))))))

(defn delete-item
  "Deletes an item from a table by its primary key.
  See `put-item` for option docs."
  [client-opts table prim-kvs & {:keys [return expected return-cc?]
                                 :or   {return :none}}]
  (java->clojure
   (.deleteItem (db-client client-opts)
     (doto-cond (DeleteItemRequest.)
       true     (.setTableName    (name table))
       true     (.setKey          (make-DynamoDB-parts :attribute-values prim-kvs))
       expected (.setExpected     (make-DynamoDB-parts :expected-attribute-values expected))
       return   (.setReturnValues (keyword->DynamoDB-enum-str return))
       return-cc? (.setReturnConsumedCapacity (keyword->DynamoDB-enum-str :total))))))

(defn- merge-more
  "Enables auto paging for batch batch-get/write and query/scan requests.
  Particularly useful for throughput limitations."
  [more-f {max-reqs :max :keys [throttle-ms]} last-result]
  (loop [{:keys [unprocessed last-prim-kvs] :as last-result} last-result idx 1]
    (let [more (or unprocessed last-prim-kvs)]
      (if (or (empty? more) (nil? max-reqs) (>= idx max-reqs))
        (if-let [items (:items last-result)]
          (with-meta items (dissoc last-result :items))
          last-result)
        (let [merge-results (fn [l r] (cond (number? l) (+    l r)
                                           (vector? l) (into l r)
                                           :else               r))]
          (when throttle-ms (Thread/sleep throttle-ms))
          (recur (merge-with merge-results last-result (more-f more))
                 (inc idx)))))))

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
               (doto-cond (BatchGetItemRequest.)
                 true       (.setRequestItems raw-req)
                 return-cc? (.setReturnConsumedCapacity (keyword->DynamoDB-enum-str :total))))))]
    (when-not (empty? requests)
      (merge-more run1 span-reqs (run1 (make-DynamoDB-parts :keys-and-attributes requests))))))