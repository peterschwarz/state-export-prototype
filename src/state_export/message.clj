(ns state-export.message
  (:require [clojure.string :refer [join]])
  (:import [sawtooth.sdk.protobuf ClientEventsSubscribeRequest]
           [sawtooth.sdk.protobuf ClientEventsSubscribeResponse]
           [sawtooth.sdk.protobuf ClientEventsSubscribeResponse$Status]
           [sawtooth.sdk.protobuf ClientEventsUnsubscribeRequest]
           [sawtooth.sdk.protobuf ClientEventsUnsubscribeResponse]
           [sawtooth.sdk.protobuf ClientEventsUnsubscribeResponse$Status]
           [sawtooth.sdk.protobuf Message]
           [sawtooth.sdk.protobuf Message$MessageType]
           [sawtooth.sdk.protobuf Event]
           [sawtooth.sdk.protobuf Event$Attribute]
           [sawtooth.sdk.protobuf EventList]
           [sawtooth.sdk.protobuf EventSubscription]
           [sawtooth.sdk.protobuf EventFilter]
           [sawtooth.sdk.protobuf EventFilter$FilterType]
           [sawtooth.sdk.protobuf StateChange]
           [sawtooth.sdk.protobuf StateChange$Type]
           [sawtooth.sdk.protobuf StateChangeList]))

; Message Type convience mappings
(def REGISTER_SUBSCRIBER Message$MessageType/CLIENT_EVENTS_SUBSCRIBE_REQUEST)
(def UNREGISTER_SUBSCRIBER Message$MessageType/CLIENT_EVENTS_UNSUBSCRIBE_REQUEST)
(def CLIENT_EVENTS Message$MessageType/CLIENT_EVENTS)

(def NULL_BLOCK_ID "0000000000000000")

(defn- namespace-regex
  "Builds a regex string for namespace matches"
  [namespaces]
  (str "^(" (join "|" namespaces) ").*$"))

(defn- make-block-sub
  []
  (-> (EventSubscription/newBuilder)
      (.setEventType "sawtooth/block-commit")
      (.build)))

(defn- make-delta-sub
  [namespaces]
  (let [event-filter (if (empty? namespaces)
                       (-> (EventFilter/newBuilder)
                           (.setFilterType EventFilter$FilterType/SIMPLE_ALL)
                           (.setKey "address")
                           (.build))
                       (-> (EventFilter/newBuilder)
                           (.setFilterType EventFilter$FilterType/REGEX_ANY)
                           (.setKey "address")
                           (.setMatchString (namespace-regex namespaces))
                           (.build)))]
    (-> (EventSubscription/newBuilder)
        (.setEventType "sawtooth/state-delta")
        (.addAllFilters [event-filter])
        (.build))))

(defn register-subscriber-request
  [block-id namespaces]
  (let [block-sub (make-block-sub)
        delta-sub (make-delta-sub namespaces)]
     (-> (ClientEventsSubscribeRequest/newBuilder)
         (.addAllLastKnownBlockIds [(or block-id NULL_BLOCK_ID)])
         (.addAllSubscriptions [block-sub delta-sub])
         (.build)
         (.toByteString))))

(defn read-register-subscriber-response
  [bytes-or-bytes-str]
  (when [bytes-or-bytes-str]
    (let [response (ClientEventsSubscribeResponse/parseFrom bytes-or-bytes-str)]
      {:status (condp = (.getStatus response)
                 ClientEventsSubscribeResponse$Status/OK :ok
                 ClientEventsSubscribeResponse$Status/INVALID_FILTER  :invalid-filter
                 ClientEventsSubscribeResponse$Status/UNKNOWN_BLOCK :unknown-block
                 ; Note: not a keyword, but we don't know
                 (.getStatus response))})))

(defn unregister-subscriber-request
  []
  (-> (ClientEventsUnsubscribeRequest/newBuilder)
      (.build)
      (.toByteString)))

(defn read-unregister-subscriber-response
  [bytes-or-bytes-str]
  (when bytes-or-bytes-str
    (let [response (ClientEventsUnsubscribeResponse/parseFrom bytes-or-bytes-str)]
      {:status (condp = (.getStatus response)
                 ClientEventsUnsubscribeResponse$Status/OK :ok
                 ClientEventsUnsubscribeResponse$Status/INTERNAL_ERROR :internal-error
                 ; Note: not a keyword, but we don't know
                 (.getStatus response))})))

(defn is-event-message-event? [^Message msg]
  (= (.getMessageType msg) CLIENT_EVENTS))

(defn- extract-block-info [^Event event]
  (when event
    (letfn [(get-attr
              ([attr-name] (get-attr attr-name nil))
              ([attr-name default-value]
               (if-let [^Event$Attribute attr
                        (->> (.getAttributesList event)
                             (filter (fn [^Event$Attribute attr]
                                       (= (.getKey attr) attr-name)))
                             first)]
                 (.getValue attr)
                 default-value)))]
      {:block-id (get-attr "block_id")
       :block-num (Integer/parseInt (get-attr "block_num" "0"))
       :state-root-hash (get-attr "state_root_hash")})))

(defn- extract-state-changes [^Event event address-to-data-parsing]
  (when event
    (->> (.getData event)
         (StateChangeList/parseFrom)
         (.getStateChangesList)
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
         (remove nil?))))

(defn read-event-message [^Message msg address-to-data-parsing]
  (let [^EventList event-list (EventList/parseFrom (.getContent msg))
        get-event (fn [event-type]
                    (->> event-list
                         (.getEventsList)
                         (filter #(= (.getEventType %) event-type))
                         first))]
    (merge (extract-block-info (get-event "sawtooth/block-commit"))
           {:changes (extract-state-changes (get-event "sawtooth/state-delta")
                                            address-to-data-parsing)})))
