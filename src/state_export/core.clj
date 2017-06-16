(ns state-export.core
  (:import [sawtooth.sdk.protobuf Setting]
           [sawtooth.sdk.protobuf Setting$Entry])
  (:require [clojure.core.async :as async
             :refer [go <!]]
            [clojure.java.jdbc :as jdbc]
            [digest]
            [state-export.cbor :as cbor]
            [state-export.network :as net]
            [state-export.message :as msg]
            [taoensso.timbre :as log]))

(def TIMEOUT_MILLIS 10000)

(defmulti create-db-ops
  "Returns a vector of
  [<table_key> <update_old_record> <insert record or nil for deletes>]"
  :type)

(defmethod create-db-ops ::setting [{:keys [address key value operation]}]
  [:setting
   ["name = ? AND end_block_num IS NULL" key]
   (when (= operation :set)
     {:name key :value value})])

(defmethod create-db-ops ::intkey [{:keys [key value operation]}]
  [:intkey
   ["name = ? AND end_block_num IS NULL" key]
   (when (= operation :set)
     {:name key :value value})])

(defn- process-event
  "Consumes an event, and inserts the necessary entries into the database"
  [database {:keys [block-id block-num state-root-hash changes] :as event}]
  (log/infof "Event received: %s" event)
  (jdbc/with-db-transaction [t-conn database]
    (jdbc/insert! t-conn :block
                  {:block_id block-id
                   :block_num block-num
                   :state_root_hash state-root-hash})
    (doseq [[table update-op insert-op]
            (map create-db-ops changes)]
      (jdbc/update! t-conn table {:end_block_num block-num} update-op)
            (when insert-op
              (jdbc/insert! t-conn table (assoc insert-op :start_block_num block-num))))))

(defn- process-events
  "Processes incoming state delta events from the given channel."
  [ch database]
  (go
    (loop []
      (when-let [event (<! ch)]
        (try
          (process-event database event)
        (catch Exception e
          (log/error e "Exception while processing event")))
        (recur)))))

(defn- parse-setting
  "Parses the state value for Settings entries from the bytes provided."
  [bytes-or-byte-str]
  (let [setting (Setting/parseFrom bytes-or-byte-str)]
    (map (fn [^Setting$Entry entry]
           {:type ::setting
            :key (.getKey entry)
            :value (.getValue entry)})
         (.getEntriesList setting))))

(defn- parse-intkey
  "Parses the state value for Intkey entries from the bytes provided."
  [bytes-or-byte-str]
  (let [decoded-data (cbor/decode (.toByteArray bytes-or-byte-str))]
    (map (fn [[k v]]
           {:type ::intkey
            :key k
            :value v})
      decoded-data)))

(defn- init-db
  "Performs any database operations needed to initialize a postgres DB instance"
  [db-spec]
  (jdbc/with-db-connection [db-conn db-spec]
    (jdbc/execute! db-conn
      ["CREATE TABLE IF NOT EXISTS block (
        block_id varchar(128) CONSTRAINT pk_block_id PRIMARY KEY,
        block_num integer,
        state_root_hash varchar(64))"])

    (jdbc/execute! db-conn
      ["CREATE INDEX IF NOT EXISTS block_num_idx
        ON block (block_num)"])

    (jdbc/execute! db-conn
      ["CREATE TABLE IF NOT EXISTS setting (
        id BIGSERIAL CONSTRAINT pk_setting PRIMARY KEY,
        name varchar(1024),
        value text,
        start_block_num integer,
        end_block_num integer)"])

    (jdbc/execute! db-conn
      ["CREATE INDEX IF NOT EXISTS setting_key_block_num
        ON setting (name, end_block_num NULLS FIRST);"])

    (jdbc/execute! db-conn
      ["CREATE TABLE IF NOT EXISTS intkey (
        id BIGSERIAL CONSTRAINT pk_intkey PRIMARY KEY,
        name varchar(20),
        value integer,
        start_block_num integer,
        end_block_num integer)"])

    (jdbc/execute! db-conn
      ["CREATE INDEX IF NOT EXISTS intkey_name_block_num
        ON intkey (name, end_block_num NULLS FIRST);"])))

(defn current-block
  "Fetch the current block stored in the DB."
  [database]
  (first (jdbc/query database ["select * from block order by block_num desc limit 1"])))

(defn current-settings
  "Fetches all of the setting values, as of the current block."
  [database]
  (jdbc/query database ["select name, value from setting where end_block_num IS NULL"]))

(defn settings-at
  "Selects the settings values at a given block number (i.e a slice in chain-time)."
  [database block-num]
  (->>
    (jdbc/query database
                ["SELECT DISTINCT ON (s.name)
                       s.name, s.value, s.end_block_num
                  FROM setting s
                  WHERE s.start_block_num <= ?
                    AND (s.end_block_num IS NULL OR s.end_block_num >= ?)
                  ORDER BY s.name, s.end_block_num desc NULLS FIRST" block-num block-num])
    (map #(dissoc % :end_block_num))))

(defn current-intkeys
  "Fetches all of the intkey values, as of the current block."
  [database]
  (jdbc/query database
              ["SELECT name, value from intkey where end_block_num IS NULL"]))

(defn intkeys-at
  "Selects the intkey values at a given block number (i.e a slice in chain-time)."
  [database block-num]
  (->>
    (jdbc/query database
                ["SELECT DISTINCT ON (t.name)
                       t.name, t.value, t.end_block_num
                  FROM intkey t
                  WHERE t.start_block_num <= ?
                    AND (t.end_block_num IS NULL OR t.end_block_num >= ?)
                  ORDER BY t.name, t.end_block_num desc NULLS FIRST" block-num block-num])
    (map #(dissoc % :end_block_num))))

(defn start
  "Starts the application"
  [zmq-conn message-ch database]
  (init-db database)
  (let [namespace-parsers {"000000" #'parse-setting
                           ; The IntegerKey Transaction family namespace
                           (subs (digest/sha-512 "intkey") 0 6) #'parse-intkey }
        delta-ch (async/chan 1
                             (comp
                               (filter #'msg/is-state-delta-event?)
                               (map #(msg/read-state-delta-event % namespace-parsers))))

        _ (async/pipe message-ch delta-ch)

        current-block (current-block database)

        _ (log/infof "Latest Known Block: %s" current-block)

        ack-promise
        (net/send zmq-conn msg/REGISTER_SUBSCRIBER
                  (msg/register-subscriber-request (:block_id current-block) (keys namespace-parsers))
                  #'msg/read-register-subscriber-response)

        response (deref ack-promise TIMEOUT_MILLIS {:status :timeout})]
    (if (= (:status response) :ok)
      (process-events delta-ch database)
      (log/errorf "Failed: %s" (:status response)))))

(defn stop
  "Stops the application"
  [zmq-conn]
   (let [ack-promise
        (net/send zmq-conn msg/UNREGISTER_SUBSCRIBER
                  (msg/unregister-subscriber-request)
                  #'msg/read-unregister-subscriber-response)

        response (deref ack-promise TIMEOUT_MILLIS {:status :timeout})]))
