(defproject state-export "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :test-paths ["test"]
  ; :plugins [[lein-protobuf "0.5.0"]]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.3.442"]

                 ; DB Access
                 [org.clojure/java.jdbc "0.7.0-alpha3"]
                 [org.postgresql/postgresql "42.1.1"]

                 ; For parsing
                 [com.google.protobuf/protobuf-java "3.3.0"]
                 [co.nstant.in/cbor "0.7"]

                 [sawtooth/sdk "1.0-SNAPSHOT"]

                 [digest "1.4.5"]
                 [com.stuartsierra/component "0.3.2"]
                 [org.zeromq/jeromq "0.4.0"]
                 [com.taoensso/timbre "4.10.0"]]
  ; :protoc "/usr/local/bin/protoc"
  ; :proto-path "../sawtooth-core/protos"
  ; :proto-path "/project/sawtooth-core/protos"
  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "0.2.11"]]
                   :source-paths ["dev"]}})
