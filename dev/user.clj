(ns user
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.namespace.repl :refer [refresh]]
            [state-export.app :as app]))

(def system nil)

(defn init []
  (alter-var-root #'system
    (constantly (app/state-export-system
                  {:url "tcp://127.0.0.1:4004"
                   :jdbc-url "jdbc:postgresql://localhost:5432/intkey?user=intkey_app&password=intkey_proto" }))))

(defn start []
  (alter-var-root #'system component/start))

(defn stop []
  (alter-var-root #'system
    (fn [s] (when s (component/stop s)))))

(defn go []
  (init)
  (start))


(defn reset []
  (stop)
  (refresh :after 'user/go))
