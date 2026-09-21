(ns metabase.driver)

(load "/stubs/driver_common")
(load "/stubs/uploads_v54")

;;; Shape: 0.54 - 0.56, after uploads gained driver-specific type promotions.
(defmulti describe-table-fks (fn [driver _database _table] driver))
