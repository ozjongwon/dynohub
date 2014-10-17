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
(ns ozjongwon.dynolite
  (:require [ozjongwon.dynohub  :as dh]
            [ozjongwon.utils    :as utils]))

;;
;; Copy paste from:
;; https://github.com/sritchie/jackknife/blob/master/src/jackknife/def.clj#L4
(defmacro defalias
  "Defines an alias for a var: a new var with the same root binding (if
  any) and similar metadata. The metadata of the alias is its initial
  metadata (as provided by def) merged into the metadata of the original."
  ([name orig]
     `(do
        (alter-meta!
         (if (.hasRoot (var ~orig))
           (def ~name (.getRawRoot (var ~orig)))
           (def ~name))
         ;; When copying metadata, disregard {:macro false}.
         ;; Workaround for http://www.assembla.com/spaces/clojure/tickets/273
         #(conj (dissoc % :macro)
                (apply dissoc (meta (var ~orig)) (remove #{:macro} (keys %)))))
        (var ~name)))
  ([name orig doc]
     (list `defalias (with-meta name (assoc (meta name) :doc doc)) orig)))

;; aliases

(defalias with-binary-reader-writer dh/with-binary-reader-writer)
(defalias without-binary-reader-writer dh/without-binary-reader-writer)

;;
;; DynamoDB connection
;;
(defonce default-client-opts (atom {:access-key "accesskey" :secret-key "secretkey"
                                    :endpoint "http://localhost:8000"    ; For DynamoDB Local
                                    }))

(defn- hub-args->lite-args [args]
  (loop [[arg & remains] (rest args) result []]
    (cond (= arg '&) [result [arg {:keys (:keys (first (first remains))) :as 'opts}]]
          (nil? arg) [result]
          :else  (recur remains (conj result arg)))))

(defmacro hub-fn->lite-fn [fn-name]
  (let [meta (meta (ns-resolve (find-ns 'ozjongwon.dynohub) fn-name))
        [args] (:arglists meta)
        doc  (:doc meta)]
    (when-not args
      (throw (Exception. (str "No functions " fn-name " found in ns 'ozjongwon.dynohub'"))))
    (let [[new-args extra] (hub-args->lite-args args)]
      `(defn ~fn-name ~@(when doc [doc]) [~@new-args ~@extra]
         (apply ~(symbol (str "ozjongwon.dynohub/" fn-name))
          @default-client-opts
          ~@(when new-args new-args)
          ~(when extra `(mapcat #(into [] %) ~'opts)))))))

(defmacro hub-fns->lite-fns [fn-name-list & args]
  `(do ~@(map (fn [fn-name]
                `(hub-fn->lite-fn ~fn-name ~@args))
              fn-name-list)))

;;;
;;; API
;;;

(defn set-default-client-opts [opts]
  (reset! default-client-opts opts))

(hub-fns->lite-fns [batch-get-item batch-write-item create-table delete-item delete-table describe-table get-item list-tables put-item query scan update-item update-table])

(defn ensure-table "Creates a table iff it doesn't already exist."
  [table-name hash-keydef & opts]
  (when-not (describe-table table-name)
    (apply create-table table-name hash-keydef opts)))

(defn pagination-query [table prim-key-conds start length & {:keys [projection-alias-attr-name-map filter-alias-attr-name-map] :as opts}]
  (let [;; DynamoDB does not like unused attr-name in alias-attr-name-map
        new-opts-map (as-> (dissoc opts :projection-alias-attr-name-map :filter-alias-attr-name-map) opts
                           (if (:filter-exp opts)
                             (assoc opts :alias-attr-name-map filter-alias-attr-name-map)
                             opts))

        count-opts (->> (dissoc new-opts-map :projection-attrs)
                        (mapcat seq)
                        (concat [:return :count]))

        {records-total :scanned-count records-filtered :count}
        (-> (apply query table prim-key-conds count-opts)
            meta
            (select-keys [:scanned-count :count]))

        item-query-opts (->> (if (:projection-attrs new-opts-map)
                               (update-in new-opts-map [:alias-attr-name-map] merge projection-alias-attr-name-map)
                               new-opts-map)
                             (mapcat seq))

        result (cond (zero? records-total) []
                     (zero? start) (apply query table prim-key-conds (concat item-query-opts [:limit length]))
                     :else (let [last-prim-kvs (-> (apply query table prim-key-conds
                                                          (concat count-opts [:limit (max start (- start length))]))
                                                   meta
                                                   :last-prim-kvs)]
                             (if (nil? last-prim-kvs)
                               []
                               (->> (vector :last-prim-kvs last-prim-kvs)
                                    (concat item-query-opts [:limit length])
                                    (apply query table prim-key-conds)))))]

    (with-meta result (assoc (meta result) :records-total records-total :records-filtered records-filtered))))

;;; DYNOLITE.CLJ ends here
