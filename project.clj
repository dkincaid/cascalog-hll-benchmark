(defproject cascalog-hll-benchmark "0.1.0"
  :description "Some benchmarking of using HyperLogLog with Cascalog"
  :url "http://github.com/dkincaid/cascalog-hll-benchmark"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"conjars" "http://conjars.org/repo"}
  :aot [cascalog-hll-benchmark.core]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [cascalog/cascalog-core "2.0.0"]
                 [com.clearspring.analytics/stream "2.5.0"]
                 [commons-codec/commons-codec "1.8"]
                 [com.taoensso/timbre "2.7.1"]
                 [org.clojure/data.csv "0.1.2"]
                 [com.clearspring.analytics/stream "2.5.0"]
                 [clj-time "0.6.0" :exclusions [org.clojure/clojure]]]
  :profiles {:dev {:dependencies [[midje/midje "1.5.1"]
                                  [org.apache.hadoop/hadoop-core "1.1.2"]]}})
