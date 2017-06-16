(ns state-export.message
  (:import [sawtooth.sdk.protobuf RegisterStateDeltaSubscriberRequest]
           [sawtooth.sdk.protobuf RegisterStateDeltaSubscriberResponse]
           [sawtooth.sdk.protobuf RegisterStateDeltaSubscriberResponse$Status]
           [sawtooth.sdk.protobuf UnregisterStateDeltaSubscriberRequest]
           [sawtooth.sdk.protobuf UnregisterStateDeltaSubscriberResponse]
           [sawtooth.sdk.protobuf UnregisterStateDeltaSubscriberResponse$Status]
           [sawtooth.sdk.protobuf Message]
           [sawtooth.sdk.protobuf Message$MessageType]
           [sawtooth.sdk.protobuf StateDeltaEvent]
           [sawtooth.sdk.protobuf StateChange]
           [sawtooth.sdk.protobuf StateChange$Type]))

; Message Type convience mappings
(def REGISTER_SUBSCRIBER Message$MessageType/STATE_DELTA_SUBSCRIBE_REQUEST)
(def UNREGISTER_SUBSCRIBER Message$MessageType/STATE_DELTA_UNSUBSCRIBE_REQUEST)
(def DELTA_EVENT Message$MessageType/STATE_DELTA_EVENT)

(defn register-subscriber-request
  [block-id namespaces]
  (let [builder (cond-> (RegisterStateDeltaSubscriberRequest/newBuilder)
                  block-id (.addAllLastKnownBlockIds [block-id])
                  namespaces (.addAllAddressPrefixes namespaces))]
     (-> builder
        (.build)
        (.toByteString))))

(defn read-register-subscriber-response
  [bytes-or-bytes-str]
  (when [bytes-or-bytes-str]
    (let [response (RegisterStateDeltaSubscriberResponse/parseFrom bytes-or-bytes-str)]
      {:status (condp = (.getStatus response)
                 RegisterStateDeltaSubscriberResponse$Status/OK :ok
                 RegisterStateDeltaSubscriberResponse$Status/UNKNOWN_BLOCK :unknown-block
                 RegisterStateDeltaSubscriberResponse$Status/INTERNAL_ERROR :internal-error
                 ; Note: not a keyword, but we don't know
                 (.getStatus response))})))

(defn unregister-subscriber-request
  []
  (-> (UnregisterStateDeltaSubscriberRequest/newBuilder)
      (.build)
      (.toByteString)))

(defn read-unregister-subscriber-response
  [bytes-or-bytes-str]
  (when bytes-or-bytes-str
    (let [response (UnregisterStateDeltaSubscriberResponse/parseFrom bytes-or-bytes-str)]
      {:status (condp = (.getStatus response)
                 UnregisterStateDeltaSubscriberResponse$Status/OK :ok
                 UnregisterStateDeltaSubscriberResponse$Status/INTERNAL_ERROR :internal-error
                 ; Note: not a keyword, but we don't know
                 (.getStatus response))})))

(defn is-state-delta-event? [^Message msg]
  (= (.getMessageType msg) DELTA_EVENT))

(defn read-state-delta-event [^Message msg address-to-data-parsing]
  (let [^StateDeltaEvent event (StateDeltaEvent/parseFrom (.getContent msg))]
    {:block-id (.getBlockId event)
     :state-root-hash (.getStateRootHash event)
     :block-num (.getBlockNum event)
     :changes (->> (.getStateChangesList event)
                   (map (fn [^StateChange change]
                       (when-let [address-parser (get address-to-data-parsing (subs (.getAddress change) 0 6))]
                         (let [parsed-data (address-parser (.getValue change))
                               meta-data {:address (.getAddress change)
                                          :operation (condp = (.getType change)
                                                       StateChange$Type/SET :set
                                                       StateChange$Type/DELETE :delete)}]
                         (if (map? parsed-data)
                           (merge  parsed-data meta-data)
                           (map #(merge % meta-data) parsed-data))))))
                   flatten
                   (remove nil?))}))
