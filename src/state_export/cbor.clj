(ns state-export.cbor
  (:require [taoensso.timbre :as log :refer [spy]])
  (:import [co.nstant.in.cbor CborDecoder]
           [co.nstant.in.cbor.model DataItem]
           [co.nstant.in.cbor.model MajorType]
           [java.io ByteArrayInputStream]))

(defprotocol Decodable
  (do-decode [self]))

(extend-protocol Decodable
  co.nstant.in.cbor.model.Map
  (do-decode [map-data-item]
    (into
      {}
      (for [k (.getKeys map-data-item)]
        [(do-decode k) (do-decode (.get map-data-item k))])))

  co.nstant.in.cbor.model.Array
  (do-decode [array-data-item]
    (mapv do-decode (.getDataItems array-data-item)))

  co.nstant.in.cbor.model.ByteString
  (do-decode [byte-string-data-item]
    (.getBytes byte-string-data-item))

  co.nstant.in.cbor.model.UnicodeString
  (do-decode [byte-string-data-item]
    (str byte-string-data-item))

  co.nstant.in.cbor.model.Number
  (do-decode [num-data-item]
    (.longValue (.getValue num-data-item)))

  String
  (do-decode [s] s))

(defn decode
  "This is a very naive cbor parser, as it handles some strangeness of the
  underlying library, but can easily run into issues."
  [bytes-value]
  (let [bais (ByteArrayInputStream. bytes-value)
        decoded-data (map #(do-decode %)
                          (.decode (CborDecoder. bais)))]
    (if (= 1 (count decoded-data))
      (first decoded-data)
      (if (= 0 (first decoded-data))
        (second decoded-data)
        decoded-data))))
