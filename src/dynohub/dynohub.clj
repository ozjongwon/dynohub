(ns pegotezzi.dynohub
  "doc"
  {:author "Jong-won Choi"}
  (:require [clojure.string         :as str]
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

(defn parse-number [^String s]
  ;; This assumes the given s is a well formed passive number string
  ;; so no need   (binding [*read-eval* false]
  (let [n (read-string s)]
    (if (number? n)
      n
      (throw (Exception. (str "Invalid number type value " s "from DynamoDB!"))))))

(def mapset (comp set mapv))
(def maphash (comp (partial apply hash-map) mapcat))

(defn- get-bignum-precision [x]
  (.precision (if (string? x) (BigDecimal. ^String x) (bigdec x))))

(defn- dynamo-db-number? [x]
  (let [type (class x)]
    (or (contains? #{Long Double Integer Float} type)
        (and (contains? #{BigInt BigDecimal BigInteger} type)
             (<= (get-bignum-precision x) 38)))))

(defmacro doto-cond "My own version of doto-cond"
  [[name exp] & clauses]
  (assert (even? (count clauses)))
  (let [g (gensym)
        sexps (map (fn [[cond sexp]]
                     (if (symbol? cond)
                       `(when-let [~name ~cond]
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

(defn partial-right [f & args]
  (fn [& next-args]
    (apply f (concat next-args args))))

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
             (doto-cond [g (ClientConfiguration.)]
               proxy-host      (.setProxyHost         g)
               proxy-port      (.setProxyPort         g)
               conn-timeout    (.setConnectionTimeout g)
               max-conns       (.setMaxConnections    g)
               max-error-retry (.setMaxErrorRetry     g)
               socket-timeout  (.setSocketTimeout     g))]
         (doto-cond [g (AmazonDynamoDBClient. (or provider aws-creds) client-config)]
           endpoint (.setEndpoint g)))))))

(defn- db-client ^AmazonDynamoDBClient [client-opts] (db-client* client-opts))

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

(defmethod make-DynamoDB-parts :global-secondary-indexes [gsindexes]
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

;;
;;FIXME:
;;      DOTO make binary converter flexible
;;
(extend-protocol Java<->Coljure
  java.util.ArrayList (java->clojure [a] (mapv java->clojure a))

  CreateTableResult
  (java->clojure [r] (java->clojure (.getTableDescription r)))

  KeySchemaElement
  ;; (java->clojure [e] {:name (keyword (.getAttributeName e))
  ;;                     :type (DynamoDB-enum-str->keyword (.getKeyType e))})
  (clojure->java [e [n t]] (doto e
                             (.setAttributeName (name n))
                             (.setKeyType (keyword->DynamoDB-enum-str t))))

  ProvisionedThroughput
  (clojure->java [pt [ru wu]] (doto pt
                                (.setReadCapacityUnits  (long ru))
                                (.setWriteCapacityUnits (long wu))))

  AttributeDefinition
  (clojure->java [ad [n t]] (doto ad
                              (.setAttributeName (name n))
                              (.setAttributeType (keyword->DynamoDB-enum-str t))))

  LocalSecondaryIndex
  (clojure->java [lsi [n ks p]] (doto lsi
                                  (.setAttributeName n)
                                  (.setKeySchema ks)
                                  (.setProjection p)))

  Projection
  (clojure->java [p [pt attrs]] (doto-cond [_ p]
                                   true  (.setProjectionType pt)
                                   (and (= pt :include) attrs) (.setNonKeyAttributes pr attrs)))

  GlobalSecondaryIndex
  (clojure->java [gsi [n ks proj prov]] (doto gsi
                                                (.setAttributeName n)
                                                (.setKeySchema ks)
                                                (.setProjection proj)
                                                (.setProvisionedThroughput prov)))








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



  )

;;;
;;; Public API
;;;
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
                  (doto-cond [_ (CreateTableRequest.)]
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
