(ns exchange.hyperliquid-historical
  (:require [byte-streams :as bs]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [amazonica.aws.s3 :as s3]
            [amazonica.core :as core]
            [exchange.binance-historical :refer [is-dir? create-dir]])
  (:import (java.io BufferedWriter ByteArrayInputStream DataInputStream)
           (java.nio ByteBuffer ByteOrder CharBuffer)
           (java.nio.channels FileChannel)
           (java.nio.charset StandardCharsets)
           (java.nio.file Files LinkOption OpenOption Path Paths StandardOpenOption StandardCopyOption)
           (java.time LocalDate LocalDateTime)
           (java.time.format DateTimeFormatter)
           (java.time.temporal ChronoUnit)
           (java.util.concurrent Executors)
           (net.jpountz.lz4 LZ4Factory LZ4SafeDecompressor LZ4FrameInputStream)
           (com.amazonaws.services.s3.model S3ObjectInputStream)))

(def bucket-name "hyperliquid-archive")

(defonce executor (Executors/newFixedThreadPool 10))

(defonce aws-access-key (System/getenv "AWS_ACCESS_KEY"))
(defonce aws-secret-access-key (System/getenv "AWS_SECRET_ACCESS_KEY"))

(defonce yyyyMMDD-formatter (DateTimeFormatter/ofPattern "yyyyMMdd"))

(defn ->datatype
  "Reference: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions"
  [v]
  (case v
    :all-mids "allMids"
    :notification "notification"
    :web-data2 "webData2"
    :candle "candle"
    :l2-book "l2Book"
    :trades "trades"
    :order-updates "orderUpdates"
    :user-events "userEvents"
    :user-fills "userFills"
    :user-fundings "userFundings"
    :user-non-funding-ledger-updates "userNonFundingLedgerUpdates"
    nil))

(defn hour-delta [^LocalDateTime d1 d2]
  (.until d1 d2 (ChronoUnit/HOURS)))

(defn make-date-time-list
  "Defaults the start/end-date to start of day (hour/minute 0).
  Increment from start-date until the last hour prior to end-date."
  [^String start-date end-date]
  (let [d1 (.atStartOfDay (LocalDate/parse start-date))
        d2 (.atStartOfDay (LocalDate/parse end-date))
        delta (hour-delta d1 d2)]
    (mapv #(.plusHours d1 %) (range 1 delta))))

(defn date-time->components
  [date-time]
  {:date   (.format date-time yyyyMMDD-formatter)
   :hour   (.getHour date-time)
   :minute (.getMinute date-time)})

(defn date-time->object-name
  [date-time datatype asset]
  (let [{:keys [date hour]} (date-time->components date-time)
        datatype (->datatype datatype)
        asset (str asset ".lz4")]
    (str/join "/" ["market_data" date hour datatype asset])))

(defn get-s3-object [bucket-name key]
  (let [result (core/with-credential [aws-access-key aws-secret-access-key]
                                     (s3/get-object
                                       :bucket-name bucket-name
                                       :key key))]
    result))

(defn s3-object->input-stream
  [obj]
  (io/input-stream (:input-stream obj)))

(defn lz4-input-stream->write
  [input-stream output-path]
  (let [fis (LZ4FrameInputStream. input-stream)
        path (Paths/get output-path (into-array String []))
        writer (Files/newBufferedWriter path StandardCharsets/UTF_8
                                        (into-array StandardOpenOption
                                                    [StandardOpenOption/CREATE
                                                     StandardOpenOption/WRITE
                                                     StandardOpenOption/TRUNCATE_EXISTING]))]
    (try
      (loop []
        (let [buffer (ByteBuffer/allocate (* 64 1024))
              bytes-read (.read fis (.array buffer) 0 (* 64 1024))]
          (when (pos? bytes-read)
            (let [actual-bytes (byte-array bytes-read)
                  _ (System/arraycopy (.array buffer)
                                      0 actual-bytes
                                      0 bytes-read)
                  s (bs/convert actual-bytes String)]
              (.write writer s 0 (.length s))
              (.flush writer)
              (recur)))))
      (finally
        (.close writer)
        (.close fis)))))

(comment
  (def start-date (LocalDateTime/of 2024 1 1 0 0))
  (def end-date (LocalDateTime/of 2024 1 15 0 0))

  (let [dates (make-date-time-list "2024-01-01" "2024-04-01")
        dates-stream (s/->source dates)
        dir "src/data/"
        asset "SOL"
        datatype :l2-book]
    (s/consume-async
      (fn [d]
        (let [key (date-time->object-name d datatype asset)
              {:keys [date hour]} (date-time->components d)
              out-file (str dir date "/" (str/join "-" [asset hour]))]
          (try
            (do
              (if-not (is-dir? (str dir date))
                (create-dir (str dir date)))
              (.submit executor
                       (fn []
                         (let [s3-obj (get-s3-object bucket-name key)
                               input-stream (s3-object->input-stream s3-obj)]
                           (lz4-input-stream->write input-stream out-file))))
              true)
            (catch Exception _
              false))))
      dates-stream)))