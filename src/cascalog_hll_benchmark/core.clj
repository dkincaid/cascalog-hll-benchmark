(ns cascalog-hll-benchmark.core
  (:require [clojure.java.io :as io]
            [cascalog.api :as ap :refer [?- <-]]
            [cascalog.logic.ops :as ops]
            [cascalog.logic.vars :as vars]
            [clojure.data.csv :as csv]
            [clj-time.coerce :as time2]
            [cascalog-hll-benchmark.util :as hll]
            [taoensso.timbre :as timbre
             :refer (trace debug info warn error fatal spy with-log-level)]
            [taoensso.timbre.profiling :as profiling :refer (p profile)])
  (:gen-class))

; This first bit of code was adapted from the blog post at 
; http://screen6.github.io/blog/2013/11/13/hyperloglog-with-cascalog.html
(def tags-totalle 10000)

(def cities ["Amsterdam" "Groningen" "Haarlem" "Den-Haag" "Utrecht" "Delft" "Edam"])
(def tags (take 10000 (repeatedly #(str (java.util.UUID/randomUUID)))))

(defn rand-zipcode
  "Generates a random district zipcode" []
  (+ (+ 10 (rand-int 89)) (* (+ 10 (rand-int 89)) 100)))

(def year-ago 1351680873) ;; GMT: Wed, 31 Oct 2012 10:54:33 GMT
(def something-like-a-year 31536001)

(defn a-day-the-past-year []
  (+ year-ago (rand-int something-like-a-year)))

(defn line
  "Generates a single line:
    'Edam 5140 568261ad-b6be-49b5-8562-daf0f41caa23 1352498259'" []
  (let [city (->> cities
                  .size
                  rand-int
                  (nth cities))
        district (rand-zipcode)
        tag (nth tags (rand-int tags-totalle)) ;;pick a random tag
        timestamp (a-day-the-past-year)]
    (str city " " district " " tag " " timestamp "\n")))

(defn write-ov-chipkaart-accesslog-file [filepathname entries-count]
  (with-open [wrtr (io/writer filepathname)]
    (dotimes [_ entries-count] (.write wrtr (line)))))

(def test-data
  (into [] (repeatedly 100 (fn [] [(line)]))))
;;
;; Cascalog parts
;;

(def ov-fields ["?city" "?district" "?uuid" "?timestamp"])

(defn parse-csv-line
  "Parse the space separated data"
  [line]
  (first (csv/read-csv line :separator \space)))

(defn ov-source "" [source]
  (<- ov-fields
        (source :> ?line)
        (parse-csv-line ?line :>> ov-fields)))

(defn cardinality [hll]
  [(.cardinality hll)])

(defn get-day-n-year [epoch-time]
  (let [ epoch-time-long (Long/parseLong epoch-time)
         in-millis (* epoch-time-long 1000)
        date (time2/from-long in-millis)]
    [(.getDayOfYear date) (.getYear date)]))

(defn count-gvb-passengers
  ([source]
     (count-gvb-passengers source hll/sum))
  ([source aggfn]
     (let [ov-source (ov-source source)]
       (<- [?city ?year ?day ?cardinality]

           (:trap (ap/hfs-textline "/tmp/hll-demo-errors" :sinkmode :replace ))

           (ov-source :>> ov-fields)

           (aggfn ?uuid :> ?hll)
           (cardinality ?hll :> ?cardinality)
           (get-day-n-year ?timestamp :> ?day ?year)
                                        ;(hll/stringify ?hll :> ?base64-hll)
           ))))

(defn -main [& args]
  (info "HyperLogLog with Cascalog demo:")
  (let [in (first args)
        dest-agg (ap/hfs-textline (str (second args) "/aggregate-op") :sinkmode :replace)
        dest-par (ap/hfs-textline (str (second args) "/parallel") :sinkmode :replace)
        agg-query (count-gvb-passengers (ap/hfs-textline (first args)) hll/sum)
        par-query (count-gvb-passengers (ap/hfs-textline (first args)) hll/agg-hyperloglog)
        run-queries (fn []
                      (p :aggregate-op (?- dest-agg agg-query))
                      (p :parallel-agg (?- dest-par par-query)))]
    ;; creates .dot file representing execution graph for this query
    ; (.writeDOT (c/compile-flow (c/stdout) query) "execution-graph.dot")
    (profile :info :Queries (run-queries))
))

