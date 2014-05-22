;;;;   -*- Mode: clojure; encoding: utf-8; -*-
;;
;; Copyright (C) 2014 Jong-won Choi
;; All rights reserved.
;;
;;;; Commentary:
;;
;;
;;
;;;; Code:
(ns pegotezzi.dynolite
  (:require [pegotezzi.dynohub  :as hub]))

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
  (let [meta (meta (ns-resolve (find-ns 'pegotezzi.dynohub) fn-name))
        [args] (:arglists meta)
        doc  (:doc meta)]
    (when-not args
      (throw (Exception. (str "No functions " fn-name " found in ns 'pegotezzi.dynohub'"))))
    (let [[new-args extra] (hub-args->lite-args args)]
      `(defn ~fn-name ~@(when doc [doc]) [~@new-args ~@extra]
         (apply ~(symbol (str "pegotezzi.dynohub/" fn-name))
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

(hub-fns->lite-fns [batch-get-item batch-write-item create-table delete-item delete-table describe-table get-item list-tables put-item query scan update-item ;;update-table
                    ])

(defn ensure-table "Creates a table iff it doesn't already exist."
  [client-opts table-name hash-keydef & opts]
  (when-not (describe-table client-opts table-name)
    (create-table client-opts table-name hash-keydef opts)))

;;; DYNOLITE.CLJ ends here
