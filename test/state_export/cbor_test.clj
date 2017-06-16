(ns state-export.cbor-test
  (:require [clojure.test :refer :all]
            [state-export.cbor :as cbor]))

(defn- hex->bytes [i]
  (.toByteArray (biginteger i)))

(deftest test-decode-cbor
  (testing "Testing basic decoding"
    (is (= 123 (cbor/decode (hex->bytes 0x187B)))))

  (testing "String decoding"
    (is (= "Hello World!" (cbor/decode (hex->bytes 0x6C48656C6C6F20576F726C6421)))))

  (testing "Array decoding"
    (is (= ["Hello World!" 12]
           (cbor/decode (hex->bytes 0x826C48656C6C6F20576F726C64210C)))))

  (testing "Map decoding"
    (is (= {"x" 1
            "y" 2}
           (cbor/decode (hex->bytes 0xA2617801617902))))))

