(defproject com.pegotezzy/dynohub "0.0.1-SNAPSHOT"
  :author "Jong-won Choi"
  :description "Clojure DynamoDB client"
  :url "https://github.com/pegotezzi/dynohub"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "Same as Clojure"}
  :min-lein-version "2.3.3"
  :global-vars {*warn-on-reflection* true
                *assert* true}
  :dependencies
  [[org.clojure/clojure        "1.5.1"]
   [com.taoensso/encore        "1.6.0"]
   [com.taoensso/nippy         "2.6.3"]
   [com.amazonaws/aws-java-sdk "1.7.9" :exclusions [joda-time]]
   [joda-time                  "2.3"]]

  :test-paths ["test" "src"]
  :profiles
  {;; :default [:base :system :user :provided :dev]
   :1.6  {:dependencies [[org.clojure/clojure    "1.6.0"]]}
   :test {:dependencies [[expectations           "1.4.56"]
                         [org.clojure/test.check "0.5.7"]]
          :plugins [[lein-expectations "0.0.8"]
                    [lein-autoexpect   "1.2.2"]]}
   :dev* [:dev {:jvm-opts ^:replace ["-server"]
                ;; :hooks [cljx.hooks leiningen.cljsbuild] ; cljx
                }]
   :dev
   [:1.6 :test
    {:dependencies []
     :plugins [[lein-ancient "0.5.5"]
               [codox        "0.6.7"]
               [lein-pprint    "1.1.1"]
               [lein-swank     "1.4.5"]]}]}

  ;; :codox {:sources ["target/classes"]} ; cljx
  :aliases
  {"test-all"   ["with-profile" "default:+1.6" "expectations"]
   "test-auto"  ["with-profile" "+test" "autoexpect"]
   ;; "build-once" ["do" "cljx" "once," "cljsbuild" "once"] ; cljx
   ;; "deploy-lib" ["do" "build-once," "deploy" "clojars," "install"] ; cljx
   "deploy-lib" ["do" "deploy" "clojars," "install"]
   "start-dev"  ["with-profile" "+dev*" "repl" ":headless"]}

  :repositories
  {"sonatype"
   {:url "http://oss.sonatype.org/content/repositories/releases"
    :snapshots false
    :releases {:checksum :fail}}
   "sonatype-snapshots"
   {:url "http://oss.sonatype.org/content/repositories/snapshots"
    :snapshots true
    :releases {:checksum :fail :update :always}}}

  :jvm-opts ["-Xmx512m"
             "-XX:MaxPermSize=256m"
             "-XX:+UseParNewGC"
             "-XX:+UseConcMarkSweepGC"
             "-Dfile.encoding=UTF-8"
             "-Dsun.jnu.encoding=UTF-8"
             "-Dsun.io.useCanonCaches=false"]
  :encoding "utf-8"
  )
