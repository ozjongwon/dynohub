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

(ns ozjongwon.utils
  "Clojure DynamoDB client. This experimental project started from Faraday by Peter Taoussanis.
  Ref. https://github.com/ptaoussanis/faraday (Faraday),
       http://goo.gl/22QGA (DynamoDBv2 API)"
  )

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
              ;;(println :find-memo arg)
              (-> lru-cache (.get arg)))
            (update-memo [arg new-value]
              ;;(println :update-memo arg)
              (-> lru-cache (.put arg new-value)))]
      (fn [args]
        (if-let [found (find-memo args)]
          found
          (let [new-value (f args)]
            (update-memo args new-value)
            new-value))))))

(def maphash (comp (partial apply hash-map) mapcat))

(defmacro tx-assert [x & message]
  `(when-not ~x
     (throw (ex-info (str "Transaction Assertion failed: " ~@(if message
                                                               `(~@(interpose " " message) "\n" (pr-str '~x))
                                                               `((pr-str '~x))))
                     {}))))
(defn error
  ([x] (if (instance? Exception x) (throw x) (error x {})))
  ([str map] (throw (ex-info str map ))))

(defmacro ignore-errors [exp]
  `(try ~exp
        (catch Exception _#
          nil)))

;;; UTILS.CLJ ends here
