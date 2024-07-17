(ns exchange.binance-historical
  (:require [clojure.data.xml :as xml]
            [clojure.string :as str]
            [clojure.zip :as zip]
            [org.httpkit.client :as http]
            [manifold.stream :as s]
            [manifold.deferred :as d])
  (:import (java.io InputStream)
           (java.nio.file Files LinkOption Path StandardCopyOption)
           (java.nio.file.attribute FileAttribute)
           (java.time LocalDate)
           (java.time.format DateTimeFormatter)
           (java.util.concurrent Executors)
           (java.util.zip ZipInputStream)))

(def bucket-url "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision")
(def data-url "https://data.binance.vision")

(defonce executor (Executors/newFixedThreadPool 10))

(def asset-kind
  {:futures "futures"
   :option  "option"
   :spot    "spot"})

(def coin-kind
  {:um "um"
   :cm "cm"})

(def time-frame
  {:daily   "daily"
   :monthly "monthly"})

(def granularity
  {:none ""
   :2m   "2m"
   :4m   "4m"
   :6m   "6m"
   :16m  "16m"
   :31m  "31m"
   :2h   "2h"
   :3h   "3h"
   :5h   "5h"
   :7h   "7h"
   :9h   "9h"
   :13h  "13h"
   :2d   "2d"
   :4d   "4d"
   :2w   "2w"})

(def market-data-kind
  {:unknown              "unknown"
   :agg-trades           "aggTrades"
   :book-depth           "bookDepth"
   :book-ticker          "bookTicker"
   :index-price-klines   "indexPriceKlines"
   :klines               "klines"
   :liquidation-snapshot "liquidationSnapshot"
   :mark-price-klines    "markPriceKlines"
   :metrics              "metrics"
   :premium-index-klines "premiumIndexKlines"
   :trades               "trades"})

(defn create-data-string
  "Return an unvalidated string of the available data:
  https://www.binance.com/en/support/faq/how-to-download-historical-market-data-on-binance-5810ae42176b4770b880ce1f14932262"
  [asset coin time gran market ticker]
  (let [validate-key (fn [m k]
                       (if-let [v (get m k)]
                         v
                         (throw (ex-info "Invalid key" {:map m :key k}))))
        data-str (->> ["data"
                       (validate-key asset-kind asset)
                       (validate-key coin-kind coin)
                       (validate-key time-frame time)
                       (validate-key granularity gran)
                       (validate-key market-data-kind market)
                       ticker
                       ""]                                  ; require / at end of data-str
                      (str/join "/")
                      )]
    (str/replace data-str "//" "/")))

(defn create-download-link [suffix]
  (str/join "/" [data-url suffix]))

(defn prefix-marker-params
  [prefix & marker]
  {:delimiter "/"
   :prefix    prefix
   :marker    marker})

(defn get-data-with-params
  [params]
  (http/get bucket-url
            {:query-params params}))

(defn get-as-byte-input-stream
  [url]
  (http/get url {:as :stream}))

(defn str->date
  ([date] (LocalDate/parse date))
  ([date fmt] (LocalDate/parse date (DateTimeFormatter/ofPattern fmt))))

(defn parse-name->yyyy-mm-dd
  "Return a map consisting of the file-name and the date found in the name."
  [^String file-name]
  (when file-name
    (let [[_ date] (re-find #"(\d{4}-\d{2}-\d{2})" file-name)]
      (when date
        {:file-name file-name
         :date      (str->date date)}))))

(defn tag-ends-with?
  "Helper function to parse the tags of Binance's XML responses."
  [suffix]
  (fn [node]
    (str/ends-with? (name (:tag node)) suffix)))

(defn next-marker
  "Get the content of the NextMarker, returns nil otherwise."
  [nodes]
  (let [marker (filter (tag-ends-with? "NextMarker") nodes)]
    (first (:content (first marker)))))

;; TODO this can more robustly determine the value of interest
(defn walk-xml
  "An awkward way of just snagging the Key content within the
  Contents (ListBucketResult -> Contents -> Key (the first content
  is the value of interest)."
  [root links & ignore-checksums]
  (if-let [content (:content (first root))]
    (let [marker (next-marker content)
          children (filter (tag-ends-with? "Contents") content)
          links (reduce (fn [links n]
                          (let [key-content (first (filter (tag-ends-with? "Key") (:content n)))
                                content-str (:content key-content)
                                filtered-content (if ignore-checksums
                                                   (filter #(not (str/ends-with? % ".CHECKSUM")) content-str)
                                                   content-str)]
                            (into links filtered-content)))
                        links
                        children)]
      {:next-marker marker
       :links       links})))


(defn get-download-links [prefix & ignore-checksums]
  "Parses Binance's S3 bucket for some given prefix and determines
  the download url (suffix) for every available data."
  (loop [links []
         resp @(get-data-with-params
                 (prefix-marker-params prefix))]
    (let [parsed (-> (:body resp)
                     xml/parse-str
                     zip/xml-zip)
          {:keys [next-marker links]} (walk-xml parsed links ignore-checksums)]
      (if (nil? next-marker)
        links
        (recur links @(get-data-with-params
                        (prefix-marker-params prefix next-marker)))))))

(defn is-dir? [dir]
  (Files/isDirectory
    (Path/of dir (into-array String []))
    (into-array LinkOption [])))

(defn create-dir [dir]
  (let [path (Path/of dir (into-array String []))
        attr-arr (into-array FileAttribute [])]
    (try
      (Files/createDirectory path attr-arr)
      (catch Exception e
        (println "Couldn't make a single directory for" dir ". Attempting to create all parent directories.")
        (Files/createDirectories path attr-arr)
        ))))

;; TODO add predicate(s) to determine which ZipEntry to save/copy
(defn save-byte-input-stream
  "Blindly save files from the zip to some directory."
  [byte-stream dir]
  (with-open [stream (ZipInputStream. byte-stream)]
    (loop []
      (when-let [entry (.getNextEntry stream)]
        (let [name (last (str/split (.getName entry) #"/"))
              dir (or dir (System/getProperty "user.dir"))
              path (Path/of dir (into-array String [name]))]
          (if-not (is-dir? dir)
            (create-dir dir))
          (Files/copy stream path (into-array StandardCopyOption [StandardCopyOption/REPLACE_EXISTING]))
          (recur))))))

(defn process-download-request [{:keys [status body error]} dir]
  (if error
    (println "Failed, exception:" error)
    (if (= status 200)
      (when (instance? InputStream body)
        (save-byte-input-stream body dir))
      (println "HTTP request failed, status:" status))))

(comment
  (def sol-info
    (get-download-links (create-data-string :futures :cm :daily :none :book-ticker "SOLUSD_PERP") true))

  (let [link-stream (s/->source sol-info)
        dir "src/data/solusd-perp/tickers"]
    (s/consume-async
      (fn [link]
        (d/chain'
          link
          create-download-link
          (fn [url]
            (try
              (do
                (.submit executor
                         (fn []
                           (let [resp @(get-as-byte-input-stream url)]
                             (process-download-request resp dir))))
                true)
              (catch Exception e
                false)))))
      link-stream)))