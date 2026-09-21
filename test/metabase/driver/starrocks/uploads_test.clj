(ns metabase.driver.starrocks.uploads-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [metabase.driver.sql-jdbc.execute :as execute]
   [metabase.driver.starrocks.uploads :as uploads]
   [stubs.fake-jdbc :as fake])
  (:import
   (java.math BigInteger)
   (java.sql Connection PreparedStatement SQLException Statement)
   (java.time LocalDate LocalDateTime OffsetDateTime)))

(defn- recording-write
  "Only fake JDBC; unimplemented transaction or batch calls deliberately fail. Preserve the
   call trace even on failure so partial writes, cleanup, and absence of retries are testable."
  [options f]
  (let [events (atom [])
        insert-count (atom 0)
        conn (reify Connection
               (setAutoCommit [_ value] (swap! events conj [:autocommit value]))
               (createStatement [_]
                 (reify Statement
                   (executeQuery [_ sql]
                     (swap! events conj [:query sql])
                     (fake/result-set
                      ["value"]
                      [{"value" (case sql
                                  "SELECT catalog()" (get options :catalog "default_catalog"))}]))
                   (^boolean execute [_ ^String sql]
                     (swap! events conj [:execute sql])
                     (when (= sql (:fail-sql options))
                       (throw (SQLException. "statement failed")))
                     false)
                   (close [_] (swap! events conj [:statement-closed]))))
               (^PreparedStatement prepareStatement [_ ^String sql]
                 (let [params (atom (sorted-map))]
                   (swap! events conj [:prepare sql])
                   (reify PreparedStatement
                     (^void setObject [_ ^int i ^Object value]
                       (swap! params assoc i value)
                       nil)
                     (^int executeUpdate [_]
                       (let [i (swap! insert-count inc)
                             values (vec (vals @params))]
                         (swap! events conj [:insert sql values])
                         (when (= i (:fail-insert options))
                           (throw (SQLException. "insert failed")))
                         (int (get options :affected (count (re-seq #"\(" (last (str/split sql #" VALUES "))))))))
                     (close [_] (swap! events conj [:prepared-closed])))))
               (close [_] (swap! events conj [:connection-closed])))]
    (with-redefs [execute/do-with-connection-with-options
                  (fn [driver db options callback]
                    (swap! events conj [:connection driver db options])
                    (try (callback conn) (finally (.close conn))))]
      (try
        {:result (f) :events @events}
        (catch Throwable t
          {:error t :events @events})))))

(defn- events-of [result kind]
  (filterv #(= kind (first %)) (:events result)))

(def ^:private definitions
  (array-map :name [:string]
             :count [:bigint]
             :_mb_row_id [:bigint :not-null :auto-increment]))

(deftest create-table-preserves-upload-contract
  (let [result (recording-write {} #(uploads/create-table! :starrocks 7 "sales.upload" definitions [:_mb_row_id]))
        sql (second (first (events-of result :execute)))]
    (is (nil? (:error result)))
    (is (= "CREATE TABLE `sales`.`upload` (`_mb_row_id` BIGINT NOT NULL AUTO_INCREMENT, `name` STRING NULL, `count` BIGINT NULL) ENGINE=OLAP PRIMARY KEY (`_mb_row_id`) DISTRIBUTED BY HASH (`_mb_row_id`) PROPERTIES (\"replicated_storage\" = \"true\")"
           sql))
    (is (= [[:connection :starrocks 7 {:write? true}]] (events-of result :connection)))
    (is (= [[:autocommit true]] (events-of result :autocommit)))
    (is (= [:connection-closed] (last (:events result))))
    (is (not (str/includes? sql "replication_num")))
    (is (not (str/includes? sql "IF NOT EXISTS")))))

(deftest all-upload-types-have-explicit-ddl
  (is (= [:string] (uploads/database-type :metabase.upload/varchar-255)))
  (is (= [:string] (uploads/database-type :metabase.upload/text)))
  (doseq [[type expected] {:int [:bigint], :float [:double], :boolean [:boolean],
                         :date [:date], :datetime [:datetime]}]
    (is (= expected (uploads/database-type (keyword "metabase.upload" (name type)))))))

(deftest identifiers-are-quoted-as-components
  (let [sql (uploads/create-table-sql "s.ch`ar" (assoc definitions :a.b [:date] (keyword "a`b") [:boolean]) [:_mb_row_id])]
    (is (str/starts-with? sql "CREATE TABLE `s`.`ch``ar`"))
    (is (str/includes? sql "`a.b` DATE NULL"))
    (is (str/includes? sql "`a``b` BOOLEAN NULL")))
  (doseq [table ["upload" "hive.s.t" ".t" "s." "s.bad\u0000name" (str "s." (apply str (repeat 256 "a")))]]
    (let [result (recording-write {} #(uploads/create-table! :starrocks 1 table definitions [:_mb_row_id]))]
      (is (some? (:error result)) table)
      (is (empty? (:events result)) "Invalid DDL must fail before acquiring a connection")))
  (doseq [columns [(assoc definitions :bad "VARCHAR(1); DROP TABLE t")
                   (assoc definitions :Name [:bigint])
                   (assoc definitions (keyword (apply str (repeat 128 "é"))) [:bigint])
                   (dissoc definitions :_mb_row_id)]]
    (is (thrown? clojure.lang.ExceptionInfo
                 (uploads/create-table-sql "s.t" columns [:_mb_row_id])))))

(deftest external-catalogs-never-reach-a-write
  (is (false? (uploads/supported? {:details {:catalog "hive"}})))
  (doseq [f [#(uploads/create-table! :starrocks 1 "s.t" definitions [:_mb_row_id])
             #(uploads/insert-into! :starrocks 1 "s.t" [:n] [[1]] nil)
             #(uploads/truncate! :starrocks 1 "s.t")
             #(uploads/drop-table! :starrocks 1 "s.t")]]
    (let [result (recording-write {:catalog "hive"} f)]
      (is (re-find #"only in default_catalog" (ex-message (:error result))))
      (is (empty? (events-of result :execute)))
      (is (empty? (events-of result :prepare)))
      (is (= [:connection-closed] (last (:events result)))))))

(deftest insert-uses-parameters-and-preserves-duplicates
  (let [text "'); DROP TABLE t; -- 'é😀\\\n"
        row [nil true (BigInteger. "9223372036854775807") 1.25 text
             (LocalDate/parse "2024-02-29") (LocalDateTime/parse "2024-02-29T12:34:56.123456")]
        result (recording-write {} #(uploads/insert-into! :starrocks 7 "s.t"
                                                         [:null :bool :int :float :text :date :datetime]
                                                         [row row row] 2))
        inserts (events-of result :insert)
        bound-row [nil true 9223372036854775807 "1.25" text
                   "2024-02-29" "2024-02-29 12:34:56.123456"]]
    (is (nil? (:error result)))
    (is (= 2 (count inserts)))
    (is (= (vec (concat bound-row bound-row)) (nth (first inserts) 2)))
    (is (= bound-row (nth (second inserts) 2)))
    (is (instance? Long (nth (nth (first inserts) 2) 2)))
    (is (every? #(not (str/includes? (second %) text)) inserts))
    (is (every? #(not (str/includes? (second %) "_mb_row_id")) inserts))
    (is (every? #(str/starts-with? (second %) "INSERT /*+ SET_VAR(enable_insert_strict = true) */ INTO ") inserts))
    (is (empty? (events-of result :execute)) "No session state to restore")
    (is (= 2 (count (events-of result :prepared-closed))))))

(deftest default-chunk-size-and-empty-upload
  (let [result (recording-write {} #(uploads/insert-into! :starrocks 1 "s.t" [:n] (repeat 10001 [1]) nil))]
    (is (nil? (:error result)))
    (is (= [10000 1] (mapv #(count (nth % 2)) (events-of result :insert)))))
  (let [result (recording-write {} #(uploads/insert-into! :starrocks 1 "s.t" [:n] [] nil))]
    (is (nil? (:error result)))
    (is (empty? (events-of result :prepare)))))

(deftest historical-dates-and-small-doubles-use-lossless-parameters
  (doseq [[value expected] [[(LocalDate/parse "0000-02-28") "0000-02-28"]
                           [(LocalDate/parse "1582-10-05") "1582-10-05"]
                           [(LocalDateTime/parse "0000-01-01T00:00") "0000-01-01 00:00:00"]
                           [(LocalDateTime/parse "1582-10-10T12:34:56.123456") "1582-10-10 12:34:56.123456"]
                           [Double/MIN_NORMAL "2.2250738585072014E-308"]
                           [Double/MIN_VALUE "4.9E-324"]
                           [Double/MAX_VALUE "1.7976931348623157E308"]]]
    (let [result (recording-write {} #(uploads/insert-into! :starrocks 1 "s.t" [:n] [[value]] nil))]
      (is (nil? (:error result)))
      (is (= [expected] (nth (first (events-of result :insert)) 2))))))

(deftest invalid-values-are-not-silently-coerced
  (doseq [value [(BigInteger. "9223372036854775808")
                 (BigInteger. "-9223372036854775809")
                 Double/NaN Double/POSITIVE_INFINITY
                 (LocalDate/of 10000 1 1) (LocalDate/of -1 1 1)
                 (LocalDate/of 0 2 29) (LocalDateTime/parse "0000-02-29T12:34:56")
                 (LocalDateTime/parse "2024-01-01T01:02:03.123456789")
                 (OffsetDateTime/parse "2024-01-01T01:02:03+03:00")
                 (apply str (repeat 32767 "é"))
                 (apply str (repeat 65534 "a"))]]
    (let [result (recording-write {} #(uploads/insert-into! :starrocks 1 "s.t" [:n] [[value]] nil))]
      (is (some? (:error result)))
      (is (empty? (events-of result :insert)))
      (is (= [:connection-closed] (last (:events result))))))
  (doseq [value [(BigInteger. "-9223372036854775808")
                 (str (apply str (repeat 16383 "😀")) "a")
                 (apply str (repeat 65533 "a"))]]
    (is (nil? (:error (recording-write {} #(uploads/insert-into! :starrocks 1 "s.t" [:n] [[value]] nil)))))))

(deftest invalid-insert-shapes-and-offset-type-fail-early
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"timezone offset"
                        (uploads/database-type :metabase.upload/offset-datetime)))
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Changing CSV column types"
                        (uploads/reject-schema-change! :starrocks 1 "s.t" {:n [:double]})))
  (doseq [columns [[:_mb_row_id] [:_MB_ROW_ID] [:n :N] []]]
    (let [result (recording-write {} #(uploads/insert-into! :starrocks 1 "s.t" columns [[1]] nil))]
      (is (some? (:error result)))
      (is (empty? (:events result)))))
  (doseq [size [0 -1 1.5]]
    (is (some? (:error (recording-write {} #(uploads/insert-into! :starrocks 1 "s.t" [:n] [[1]] size))))))
  (let [result (recording-write {} #(uploads/insert-into! :starrocks 1 "s.t" [:n] [[1 2]] nil))]
    (is (re-find #"row width" (ex-message (:error result))))
    (is (empty? (events-of result :prepare)))))

(deftest failure-closes-resources-without-retry-or-session-mutation
  (let [result (recording-write {:fail-insert 2}
                                #(uploads/insert-into! :starrocks 1 "s.t" [:n] [[1] [2] [3]] 1))]
    (is (= "insert failed" (ex-message (:error result))))
    (is (= 2 (count (events-of result :insert))))
    (is (= 2 (count (events-of result :prepared-closed))))
    (is (empty? (events-of result :execute)))
    (is (= [:connection-closed] (last (:events result)))))
  (let [result (recording-write {:affected 0}
                                #(uploads/insert-into! :starrocks 1 "s.t" [:n] [[1]] 1))]
    (is (re-find #"row count differs" (ex-message (:error result))))
    (is (empty? (events-of result :execute)))))

(deftest truncate-and-drop-are-separate-autocommit-operations
  (let [result (recording-write {} #(do (uploads/truncate! :starrocks 1 "s.t")
                                        (uploads/drop-table! :starrocks 1 "s.t")))]
    (is (nil? (:error result)))
    (is (= [[:execute "TRUNCATE TABLE `s`.`t`"] [:execute "DROP TABLE IF EXISTS `s`.`t`"]]
           (events-of result :execute)))
    (is (= 2 (count (events-of result :connection)))))
  (testing "A failed CREATE never blindly drops a potentially pre-existing table"
    (let [sql (uploads/create-table-sql "s.t" definitions [:_mb_row_id])
          result (recording-write {:fail-sql sql} #(uploads/create-table! :starrocks 1 "s.t" definitions [:_mb_row_id]))]
      (is (some? (:error result)))
      (is (= [[:execute sql]] (events-of result :execute))))))
