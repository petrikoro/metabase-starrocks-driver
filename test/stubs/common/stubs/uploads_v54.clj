;;; Loaded into metabase.driver only for 0.54+ shapes.
;;; The column hook first appeared in 0.50.8; early v50 deliberately omits it.
(defmulti column-name-length-limit identity)
(defmethod column-name-length-limit :default [driver]
  (table-name-length-limit driver))

(defmulti allowed-promotions identity)
(defmethod allowed-promotions :default [_]
  {:metabase.upload/int #{:metabase.upload/float}})

(defmulti alter-table-columns! (fn [driver & _] driver))

;; The public legacy method remains present, while uploads calls the newer hook.
(defmethod alter-columns! :default [& _] :inherited-alter)
