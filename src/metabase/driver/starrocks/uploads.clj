(ns metabase.driver.starrocks.uploads
  "CSV upload writes. Uses the host's connection scope and pool, including SSH and writable
   credentials. Each statement commits independently; truncate and insert are separate calls."
  (:require
   [clojure.string :as str]
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute])
  (:import
   (java.math BigInteger)
   (java.nio.charset StandardCharsets)
   (java.sql Connection)
   (java.time LocalDate LocalDateTime OffsetDateTime)
   (java.time.format DateTimeFormatter)))

(set! *warn-on-reflection* true)

(def identifier-limit
  "Conservative limit shared with old Metabase application metadata. StarRocks allows more."
  255)

;; STRING resolves to VARCHAR(65533) in StarRocks DDL.
(def ^:private text-limit 65533)
(def ^:private auto-pk "_mb_row_id")
(def ^:private auto-pk-type [:bigint :not-null :auto-increment])

(defn- fail! [message]
  (throw (ex-info (str "StarRocks uploads: " message) {:status-code 400})))

(defn supported?
  "Advertise uploads only for the internal catalog. The acquired connection is checked again,
   since writable credentials or JDBC session options can change its effective catalog."
  [database]
  (let [catalog (some-> (get-in database [:details :catalog]) str/trim)]
    (or (str/blank? catalog) (= catalog "default_catalog"))))

(defn database-type
  "The public upload keywords are stable even where Metabase moved its uploads namespace."
  [upload-type]
  (case upload-type
    (:metabase.upload/varchar-255 :metabase.upload/text) [:string]
    :metabase.upload/int                             [:bigint]
    :metabase.upload/auto-incrementing-int-pk         auto-pk-type
    :metabase.upload/float                           [:double]
    :metabase.upload/boolean                         [:boolean]
    :metabase.upload/date                            [:date]
    :metabase.upload/datetime                        [:datetime]
    :metabase.upload/offset-datetime
    (fail! "Datetime values with a timezone offset are not supported. Convert the CSV to UTC without an offset before uploading, including subsequent appends and replacements.")
    (fail! "Unsupported CSV column type.")))

(defn- identifier-name [identifier]
  (when-not (or (string? identifier)
                (and (keyword? identifier) (nil? (namespace identifier))))
    (fail! "Expected an unqualified identifier."))
  (let [s (name identifier)]
    (when (or (str/blank? s)
              (str/includes? s "\u0000")
              (> (alength (.getBytes s StandardCharsets/UTF_8)) identifier-limit))
      (fail! "Identifiers must be nonblank and at most 255 UTF-8 bytes, without NUL characters."))
    s))

(defn- quote-identifier [identifier]
  (str "`" (str/replace (identifier-name identifier) "`" "``") "`"))

(defn- quote-table [table-name]
  ;; Uploads passes schema.table as a string. Requiring both avoids accidentally writing to
  ;; the connection's fallback information_schema, and rejects catalog.schema.table.
  (when-not (string? table-name)
    (fail! "Select an upload schema and table."))
  (let [parts (str/split table-name #"\." -1)]
    (when-not (= 2 (count parts))
      (fail! "Expected schema.table in default_catalog; catalog-qualified names are not supported."))
    (str/join "." (map quote-identifier parts))))

(def ^:private column-types
  {[:bigint]               "BIGINT NULL"
   [:double]               "DOUBLE NULL"
   [:boolean]              "BOOLEAN NULL"
   [:date]                 "DATE NULL"
   [:datetime]             "DATETIME NULL"
   [:string]               "STRING NULL"})

(defn create-table-sql
  "Only the upload table shape is supported. Keep the generated key first, as required by
   StarRocks 3.2, regardless of the input map's iteration order. Never lower replication_num."
  [table-name column-definitions primary-key]
  (let [table   (quote-table table-name)
        columns (mapv (fn [[column type]] [(identifier-name column) type]) column-definitions)]
    (when-not (and (= [auto-pk] (mapv identifier-name primary-key))
                   (= auto-pk-type (some #(when (= auto-pk (first %)) (second %)) columns)))
      (fail! "Uploads require the generated _mb_row_id BIGINT AUTO_INCREMENT primary key."))
    (when-not (= (count columns) (count (distinct (map (comp str/lower-case first) columns))))
      (fail! "Duplicate column names are not supported."))
    (let [definitions (into [(str (quote-identifier auto-pk) " BIGINT NOT NULL AUTO_INCREMENT")]
                            (for [[column type] columns :when (not= column auto-pk)]
                              (str (quote-identifier column) " "
                                   (or (get column-types type)
                                       (fail! "Unsupported column definition.")))))]
      (str "CREATE TABLE " table " (" (str/join ", " definitions) ") "
           "ENGINE=OLAP PRIMARY KEY (`_mb_row_id`) DISTRIBUTED BY HASH (`_mb_row_id`) "
           "PROPERTIES (\"replicated_storage\" = \"true\")"))))

(defn- scalar-string [^Connection conn sql]
  (with-open [stmt (.createStatement conn)
              rs   (.executeQuery stmt sql)]
    (when (.next rs)
      (.getString rs 1))))

(defn- with-upload-connection [driver db-id f]
  ;; Do not rebuild connection details: on newer hosts the caller has already established
  ;; with-write-connection, which the standard pool lookup below must inherit.
  (sql-jdbc.execute/do-with-connection-with-options
   driver db-id {:write? true}
   (fn [^Connection conn]
     (when-not (= "default_catalog" (scalar-string conn "SELECT catalog()"))
       (fail! "Writing is supported only in default_catalog."))
     (.setAutoCommit conn true)
     (f conn))))

(defn- execute! [driver db-id ^String sql]
  (with-upload-connection driver db-id
    (fn [^Connection conn]
      (with-open [stmt (.createStatement conn)]
        (.execute stmt sql))))
  nil)

(defn create-table!
  "Create a new upload table. A failed CREATE must never cause an existing table to be dropped."
  [driver db-id table-name column-definitions primary-key]
  (execute! driver db-id (create-table-sql table-name column-definitions primary-key)))

(defn drop-table! [driver db-id table-name]
  (execute! driver db-id (str "DROP TABLE IF EXISTS " (quote-table table-name))))

(defn truncate! [driver db-id table-name]
  (execute! driver db-id (str "TRUNCATE TABLE " (quote-table table-name))))

(defn reject-schema-change!
  "Reject new columns and legacy type promotions before any asynchronous ALTER."
  [& _]
  (fail! "Adding CSV columns or changing their types is not supported. Create a new upload table instead."))

(defn- check-date! [^LocalDate date]
  (when-not (<= 0 (.getYear date) 9999)
    (fail! "Dates must have a year between 0000 and 9999."))
  ;; StarRocks 4.1.3 corrupts this ISO leap day on INSERT, for both DATE and DATETIME.
  (when (= date (LocalDate/of 0 2 29))
    (fail! "The date 0000-02-29 is not supported by StarRocks.")))

(defn- upload-value [value]
  ;; Bind dates and doubles as strings for conversion by the target column. Legacy JDBC
  ;; calendar conversion changes historical dates, and StarRocks can round tiny numeric
  ;; literals to zero as DECIMAL before assigning them to DOUBLE. Keep this local to uploads.
  (cond
    (instance? OffsetDateTime value)
    (fail! "Datetime values with a timezone offset are not supported; convert the CSV to UTC without an offset.")

    (instance? BigInteger value)
    (try (.longValueExact ^BigInteger value)
         (catch ArithmeticException _ (fail! "Integer is outside the signed BIGINT range.")))

    (instance? LocalDateTime value)
    (do (check-date! (.toLocalDate ^LocalDateTime value))
        (when-not (zero? (rem (.getNano ^LocalDateTime value) 1000))
          (fail! "Datetime precision beyond microseconds is not supported."))
        (str/replace (.format ^LocalDateTime value DateTimeFormatter/ISO_LOCAL_DATE_TIME) \T \space))

    (instance? LocalDate value)
    (do (check-date! value) (str value))

    (string? value)
    (do (when (> (alength (.getBytes ^String value StandardCharsets/UTF_8)) text-limit)
          (fail! (str "Text exceeds " text-limit " UTF-8 bytes.")))
        value)

    (instance? Double value)
    (do (when-not (Double/isFinite value)
          (fail! "Non-finite floating-point values are not supported."))
        (str value))

    (or (nil? value) (instance? Boolean value) (instance? Long value)) value
    :else (fail! "Unsupported CSV value type.")))

(defn insert-into!
  "Insert parameterized chunks without retries or a cross-statement transaction. Successful
   earlier chunks remain committed if a later chunk fails, including during replace."
  [driver db-id table-name column-names values chunk-rows]
  (let [table   (quote-table table-name)
        columns (mapv identifier-name column-names)
        n       (count columns)
        size    (or chunk-rows 10000)]
    (when (or (zero? n) (not= n (count (distinct (map str/lower-case columns))))
              (some #(= auto-pk (str/lower-case %)) columns))
      (fail! "Insert columns must be unique and must omit the generated _mb_row_id."))
    (when-not (pos-int? size)
      (fail! "Insert chunk size must be a positive integer."))
    ;; INSERT supports SET_VAR since StarRocks 3.2. It applies to this statement only, so
    ;; strict mode cannot leak to the next borrower of the pooled connection.
    ;; https://github.com/StarRocks/starrocks/blob/3.2.0/fe/fe-core/src/main/java/com/starrocks/sql/parser/AstBuilder.java#L1595
    (let [prefix (str "INSERT /*+ SET_VAR(enable_insert_strict = true) */ INTO " table
                      " (" (str/join ", " (map quote-identifier columns)) ") VALUES ")
          row-sql (str "(" (str/join ", " (repeat n "?")) ")")]
      (with-upload-connection
        driver db-id
        (fn [^Connection conn]
          (doseq [chunk (partition-all size values)]
            (when-not (every? #(= n (count %)) chunk)
              (fail! "CSV row width does not match the upload columns."))
            (let [params (into [] (comp cat (map upload-value)) chunk)
                  sql    (str prefix (str/join ", " (repeat (count chunk) row-sql)))]
              (with-open [stmt (.prepareStatement conn sql)]
                (sql-jdbc.execute/set-parameters! driver stmt params)
                (when-not (= (count chunk) (.executeUpdate stmt))
                  (fail! "Inserted row count differs from the CSV chunk size. Data may already be committed; do not retry automatically."))))))))))
