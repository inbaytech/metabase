(ns metabase.test.data.elasticsearch
  (:require [clojure
              [string :as s]]
            [metabase.driver.elasticsearch.util :refer [with-elasticsearch-connection]]
            [metabase.test.data.interface :as i]
            [metabase.util :as u]
            [clojurewerkz.elastisch.rest :as rest]
            [clojurewerkz.elastisch.rest
              [index :as idx]
              [document :as doc]])
  (:import metabase.driver.elasticsearch.ElasticSearchDriver))

(defn- database->connection-details
  ([dbdef]
   {:dbname (i/escaped-name dbdef)
    :host   "localhost"})
  ([_ _ dbdef]
   (database->connection-details dbdef)))

(defn- destroy-db! [dbdef]
  (with-elasticsearch-connection [conn (database->connection-details dbdef)]
    (let [dbname (i/escaped-name dbdef)
          database (if (s/blank? dbname) "testdb" dbname)]
      (idx/delete conn database))))

(defn- create-db! [{:keys [table-definitions], :as dbdef}]
  (destroy-db! dbdef)
  (with-elasticsearch-connection [conn (database->connection-details dbdef)]
    (let [dbname (i/escaped-name dbdef)
          database (if (s/blank? dbname) "testdb" dbname)]
      (doseq [{:keys [field-definitions table-name rows]} table-definitions]
        ;; Create new index (a.k.a table)
        (rest/put conn (rest/index-url conn
                          table-name)
          {:body {:settings {}}
           :content-type  :json})
        ;; Add the new index to the alias (a.k.a database)
        ;(idx/update-aliases conn {:add {:index table-name :alias database}})
        (rest/post conn (rest/index-aliases-batch-url conn)
          {:body {:actions {:add {:index table-name :alias database}}}
           :content-type  :json})
        (let [field-names (for [field-definition field-definitions]
                            (keyword (:field-name field-definition)))]
          ;; Use map-indexed so we can get an ID for each row (index + 1)
          (doseq [[i row] (map-indexed (partial vector) rows)]
            (try
              ;; Insert each row
              (let [rowid (inc i)
                    document (merge (zipmap field-names row) {:id rowid})]
                (rest/post conn (rest/mapping-type-url conn
                                  (name table-name) (name table-name))
                  {:body document
                   :query-params {:id rowid}
                   :content-type  :json}))

              ;; If row already exists then nothing to do
              (catch Exception _))))))))

(u/strict-extend ElasticSearchDriver
  i/IDriverTestExtensions
  (merge i/IDriverTestExtensionsDefaultsMixin
    {:create-db!                   (u/drop-first-arg create-db!)
     :database->connection-details database->connection-details
     :engine                       (constantly :elasticsearch)}))

;(u/strict-extend ElasticSearchDriver
;                 i/IDriverTestExtensions
;                 (merge i/IDriverTestExtensionsDefaultsMixin
;                        {:create-db!                   (u/drop-first-arg create-db!)
;                         :database->connection-details database->connection-details
;                         :engine                       (constantly :elasticsearch)
;                         :format-name                  (fn [_ table-or-field-name]
;                                                         (if (= table-or-field-name "id")
;                                                           "_id"
;                                                           table-or-field-name))}))
