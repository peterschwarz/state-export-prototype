(ns state-export.app
  (:require [clojure.core.async :as async :refer [>!!]]
            [com.stuartsierra.component :as component]
            [state-export.network :as net]
            [state-export.core :as state-export]
            [taoensso.timbre :as timbre :refer [info]]))

(defrecord AsyncChannelLifecycle [ch]
  component/Lifecycle
  (start [this]
    (if-not (:ch this)
      (assoc this :ch (async/chan))
      this))

  (stop [this]
    (if (:ch this)
      (let [ch (:ch this)]
        (async/close! ch)
        (assoc this :ch nil))
      this)))

(defn lifecycle-chan []
  (map->AsyncChannelLifecycle {}))

(defrecord ZmqConnection [url message-ch connection]
  component/Lifecycle
  (start [this]
    (when-not (:connection this)
      (let [ch (:ch message-ch)
            conn (net/start url #(>!! ch %))]
        (info "Connected over ZMQ")
        (assoc this :connection conn))))

  (stop [this]
    (when-let [conn (:connection this)]
      (info "Stopping the ZMQ connection")
      (net/stop conn)
      (assoc this :connection nil))))

(defn zmq-connection
  [url]
  (component/using (map->ZmqConnection {:url url})
                   [:message-ch]))

(defrecord StateExportApp [zmq database message-ch started shutdown-hook-thread]
  component/Lifecycle

  (start [this]
    (if-not (:started this)
      (let [conn (:connection zmq)
            ch (:ch message-ch)
            shutdown-hook (Thread. #(when (:started this)
                                      (state-export/stop conn)))]
        (info "Starting State Export Application")
        (.addShutdownHook (Runtime/getRuntime) shutdown-hook)
        (state-export/start conn ch database)
        (assoc this
               :started true
               :shutdown-hook-thread shutdown-hook))
      this))

  (stop [this]
    (if (:started this)
      (if-let [zmq-conn (:connection zmq)]
        (do
          (info "Shutting down State Export Application")
          (.removeShutdownHook (Runtime/getRuntime) (:shutdown-hook-thread this))
          (state-export/stop zmq-conn)
          (assoc this
                 :started nil
                 :shutdown-hook-thread nil))
        this)
      this)))

(defn state-export-app []
  (component/using (map->StateExportApp {})
                   [:zmq :message-ch :database]))

(defn state-export-system [config-options]
  (component/system-map
    :message-ch (lifecycle-chan)
    :database {:connection-uri (:jdbc-url config-options)}
    :zmq (zmq-connection (:url config-options))
    :state-export-app (state-export-app)))
