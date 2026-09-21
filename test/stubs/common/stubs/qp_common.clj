;;; Shared surface of `metabase.driver.sql.query-processor`, loaded into that namespace by each
;;; shape stub with `(load "/stubs/qp_common")`. No `ns` form on purpose -- see driver_common.clj.
;;;
;;; Everything here exists in every Metabase version the driver targets.

(defmulti ->honeysql
  (fn [driver expr]
    [driver (if (sequential? expr) (first expr) (class expr))]))

;; Only the operand shapes used by aggregation probes. Deliberately no fallback for
;; aggregates: the real driver must supply those methods. Numbers are inlined just as
;; in Metabase, so percentile constants cannot accidentally become bind parameters.
(defmethod ->honeysql :default
  [driver expr]
  (if (number? expr)
    [:inline expr]
    (case (first expr)
      :field (keyword (second expr))
      :value (->honeysql driver (second expr))
      :+     (into [:+] (map (partial ->honeysql driver)) (rest expr))
      (throw (ex-info "Unsupported stub expression" {:driver driver :expr expr})))))

(defmulti quote-style
  (fn [driver] driver))

(defmulti unix-timestamp->honeysql
  (fn [driver unit _expr] [driver unit]))

(defmulti current-datetime-honeysql-form
  (fn [driver] driver))

(defmulti date
  (fn [driver unit _expr] [driver unit]))

(defmulti add-interval-honeysql-form
  (fn [driver _hsql-form _amount _unit] driver))

(defmulti datetime-diff
  (fn [driver unit _x _y] [driver unit]))

(defmulti cast-temporal-string
  (fn [driver coercion _expr] [driver coercion]))

(defmulti cast-temporal-byte
  (fn [driver coercion _expr] [driver coercion]))
