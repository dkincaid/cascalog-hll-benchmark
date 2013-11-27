(ns cascalog-hll-benchmark.util
  (:require [cascalog.api :as c]
            [cascalog.logic.vars :as vars]
            [cascalog.logic.ops :as ops])
  (:import [com.clearspring.analytics.stream.cardinality HyperLogLog HyperLogLog$Builder]
           [org.apache.commons.codec.binary Base64])
  (:refer-clojure :exclude [read merge]))
 
; This first bit of code comes from the blog post at 
; http://screen6.github.io/blog/2013/11/13/hyperloglog-with-cascalog.html
(defn create
  "log2n — the number of bits to use as the basis for the HLL instance; accuracy = 1.04/sqrt(2^log2m)"
  ([] (HyperLogLog. 12))
  ([log2n] (HyperLogLog. log2n)))
 
(defn to-string [^HyperLogLog hll]
  (Base64/encodeBase64String (.getBytes hll)))
 
(defn read [string-hll]
  "Read HyperLogLog field from Base64 encoded string"
  (HyperLogLog$Builder/build
    (Base64/decodeBase64 string-hll)))
 
(defprotocol IHyperLogLogMerge 
  (hyperloglog-val [this])
  (merge [this other])
  (merge-with-hyperloglog [this other-hll]))
 
(extend-protocol IHyperLogLogMerge
  nil
  (hyperloglog-val [this] nil)
  (merge-with-hyperloglog [this other-hll] other-hll)
  (merge [this other] other)
 
  Object
  (hyperloglog-val [this] (doto (create) (.offer this)))
  (merge-with-hyperloglog [this other-hll] (.offer other-hll this) other-hll)
  (merge [this other]
    (merge (hyperloglog-val other) this))
 
  HyperLogLog
  (hyperloglog-val [this] this)
  (merge-with-hyperloglog [this other-hll] (.addAll this other-hll) this)
  (merge [this other]
    (merge-with-hyperloglog other this)))
 
;;
;; Cascalog glue
;;
(c/defaggregatefn sum*
  ([] (create))
  ([state val]
    (.offer state val)
    state)
  ([state] [state]))
  
(def sum
  (ops/each sum*))

(defn merge-n
   ([h1] h1)
   ([h1 h2] (merge h1 h2))
   ([h1 h2 & more]
    (reduce merge (merge h1 h2) more)))
 
(c/defparallelagg parallel-sum
                   :init-var #'identity
                   :combine-var #'merge-n)
 
(c/defmapfn stringify [hll-object]
            [(to-string hll-object)])


; This bit of code was adapted from an excellent post in the Cascalog Google
; Group by Jeroen van Dijk 
;(https://groups.google.com/forum/#!msg/cascalog-user/l3H456kmhhQ/-7gfgVZ2xaUJ)
(defn hll-construct
  "Construct a HyperLogLog and offer the given init-value. The accuracy (standard error) will be 1.04/sqrt(2^bits). So 12 bits (the default used if not specified) gives a little less than 2% standard error."
  ([init-value]
     (hll-construct 12 init-value))
  ([bits init-value]
     (doto (HyperLogLog. bits) (.offer init-value))))

(defn hll-merge
  "Merge two or more HyperLogLog's together."
  [^HyperLogLog first-hlog & hlogs]
  (.merge ^HyperLogLog first-hlog (into-array HyperLogLog hlogs)))

(defn hll-estimate-cardinality
  "Get the estimated cardinality from the HyperLogLog"
  [hll]
  (.cardinality hll))

(c/defparallelagg agg-hyperloglog
  :init-var #'hll-construct
  :combine-var #'hll-merge)

