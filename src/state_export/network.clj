(ns state-export.network
  (:refer-clojure :exclude [send])
  (:import [org.zeromq ZContext]
           [org.zeromq ZFrame]
           [org.zeromq ZLoop]
           [org.zeromq ZLoop$IZLoopHandler]
           [org.zeromq ZMQ]
           [org.zeromq ZMQ$PollItem]
           [org.zeromq ZMQ$Poller]
           [org.zeromq ZMsg]
           [java.util UUID]
           [java.io ByteArrayOutputStream]
           [com.google.protobuf ByteString]
           [sawtooth.sdk.protobuf Message]
           [sawtooth.sdk.protobuf Message$MessageType]))


(defrecord ZMQConnection [socket context promises])

(defn- new-zmq-connection [socket context]
  (ZMQConnection. socket context (atom {})))

(defn connect [url]
  "DOCSTRING"
  (let [context (ZContext.)
        socket (doto (.createSocket context ZMQ/DEALER)
                 (.setIdentity (-> (UUID/randomUUID)
                                   (str)
                                   (.getBytes ZMQ/CHARSET)))
                 (.connect url))]
    (new-zmq-connection socket context)))

(defn listen [^ZMQConnection conn f]
  "DOCSTRING"
  (let [event-loop (ZLoop. (:context conn))
        poll-item (ZMQ$PollItem. (:socket conn) ZMQ$Poller/POLLIN)]
    (.addPoller event-loop poll-item
      (reify ZLoop$IZLoopHandler
        (^int handle [_ ^ZLoop zloop ^ZMQ$PollItem item _]
          (let [^ZMsg msg (ZMsg/recvMsg (.getSocket item))

                multi-part-msg (.iterator msg)

                byte-stream
                (reduce
                  (fn [^ByteArrayOutputStream stream ^ZFrame frame]
                    (.write stream (.getData frame))
                    stream)
                  (ByteArrayOutputStream.)
                  (iterator-seq multi-part-msg))]

            (try
              (-> byte-stream
                  (.toByteArray)
                  (Message/parseFrom)
                  (f))
            (catch Exception e
              (print e)))
            ; Return zero, to satisfy the interface
            0)))
      nil)
    (future
      (.start event-loop))))

(defn ^ZMQConnection start [url f]
  "DOCSTRING"
  (let [conn (connect url)]
    (listen conn
      (fn [msg]
        (let [{:keys [promises]} conn]
          (if-let [receiver (get @promises (.getCorrelationId msg))]
            (let [[p p-xform] receiver]
              (deliver p (p-xform (.getContent msg)))
              (swap! promises dissoc (.getCorrelationId msg)))
            (f msg)))))
    conn))

(defn stop [^ZMQConnection conn]
  "DOCSTRING"
  (.close (:socket conn))
  (.destroy (:context conn)))

(defn- send-message [^ZMQConnection conn ^Message msg]
  (let [zmsg (ZMsg.)]
    (.add zmsg (.. msg toByteString toByteArray))
    (.send zmsg (:socket conn))))

(defn- make-message [^Message$MessageType destination ^String correlation-id ^ByteString msg-bytes]
  (-> (Message/newBuilder)
      (.setCorrelationId correlation-id)
      (.setMessageType destination)
      (.setContent msg-bytes)
      (.build)))

(defn send
  ([^ZMQConnection conn ^Message$MessageType destination ^ByteString msg-bytes]
   (send conn destination msg-bytes identity))
  ([^ZMQConnection conn ^Message$MessageType destination ^ByteString msg-bytes response-xform]
  "DOCSTRING"
  (let [promises (:promises conn)
        msg (make-message destination (str (UUID/randomUUID)) msg-bytes)
        result-promise (promise)]

    (swap! promises assoc (.getCorrelationId msg) [result-promise response-xform])
    (send-message conn msg)
    result-promise)))

(defn reply
  [^ZMQConnection conn ^Message$MessageType destination ^String correlation-id ^ByteString msg-bytes]
  "DOCSTRING"
  (send-message conn (make-message destination correlation-id msg-bytes)))
