(ns metabase.driver)

(load "/stubs/driver_common")
(load "/stubs/uploads_v54")

;;; Shape: Metabase 0.57 - 0.58
;;;   describe-table-fks  present (removed in 0.63)
;;;   describe-database*  present (first ships in release-x.57.x)

(defmulti describe-table-fks
  (fn [driver _database _table] driver))

(defmulti describe-database*
  (fn [driver _database] driver))
