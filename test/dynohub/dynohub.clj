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
(ns ozjongwon.dynohub.tests.dynohub
  (:require [expectations     :as test :refer :all]
            [ozjongwon.dynohub :as dh]
            [ozjongwon.dynolite :as dl]
;            [taoensso.encore  :as encore]
;            [taoensso.faraday :as far]
;            [taoensso.nippy   :as nippy]
            )
  (:import  [com.amazonaws.auth BasicAWSCredentials]
            [com.amazonaws.internal StaticCredentialsProvider]))

(comment (test/run-tests '[ozjongwon.dynohub.tests.dynohub]))

(defn- basic-table-tests []
  (let [new-opts {:access-key "test-accesskey" :secret-key "test-secretkey"
                  :endpoint "http://localhost:8000"}
        [table1 hash-keydef1]  [:test-table [:hash-keydef1 :s]]]

    (dl/set-default-client-opts new-opts)

    ;; default-client-opts updated?
    (expect new-opts @dl/default-client-opts)

    ;; Initially there are no tables
    (expect [] (dl/list-tables))

    ;; create a simple table with hash-keydef only
    (dl/create-table table1 hash-keydef1)

    ;; Do all expected keys exist?
    (let [table-description (dl/describe-table table1)]
      (expect  #{:prim-keys :creation-date :gsindexes :item-count :lsindexes :throughput :name :size :status}
               (set (keys table-description))))

))




(defn- before-run {:expectations-options :before-run} []
  (doseq [t (dl/list-tables)]
    (dl/delete-table t)))

(defn- after-run {:expectations-options :after-run} []
  (println "after-run")
  (basic-table-tests)
)

;;; DYNOHUB.CLJ ends here
