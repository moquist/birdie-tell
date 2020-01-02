(defproject scamp "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha14"]
                 [org.clojure/math.numeric-tower "0.0.4"]
                 [com.taoensso/timbre "4.10.0"]
                 [prismatic/schema "1.1.1"]
                 ]
  :profiles {:dev {:dependencies [[org.clojure/test.check "0.10.0"]]}})
