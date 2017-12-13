(ns metabase.driver.elasticsearch
  "ElasticSearch Driver."
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metabase
              [driver :as driver]
              [util :as u]]
            [metabase.driver.elasticsearch
              [query-processor :as qp]
              [util :refer [*elasticsearch-connection* with-elasticsearch-connection]]]
            [metabase.models
              [database :refer [Database]]
              [field :as field]]
            [metabase.sync.interface :as si]
            [metabase.util.ssh :as ssh]
            [clojurewerkz.elastisch.rest
              [index :as idx]
              [document :as doc]
              [admin :as adm]
              [response :as resp]]
            [toucan.db :as db])
  (:import clojurewerkz.elastisch.rest.Connection))

;;; ## ElasticSearchDriver

(defn- can-connect? [details]
  (with-elasticsearch-connection [^Connection conn, details]
    (let [status (-> (adm/cluster-health conn)
                   (:status))]
      (or (= status "yellow") (= status "green")))))

(defn- describe-database [database]
  (with-elasticsearch-connection [^Connection conn, database]
    (let [dbname  (:alias database)
          tables  (flatten (into '() (for [index (keys (if (str/blank? dbname)
                                                         (idx/get-settings conn)
                                                         (idx/get-aliases conn dbname)))]
                                       (let [collection  (name index)
                                             mappings    (keys (get-in (idx/get-mapping conn collection) [index :mappings]))
                                             tabs        {:schema nil, :name collection}
                                             tabs        (if (empty? mappings)
                                                           tabs
                                                           (cons tabs (for [mapping mappings]
                                                                        {:schema nil, :name (str collection "___" (name mapping))})))]
                                         ;(println (set tabs))
                                         ;(println (str "type: " (type (set tabs))))
                                         ;{:schema nil, :name (name index)}
                                         tabs)
                                       )))]
      {:tables (set tables)})))

(defn- describe-table [database table]
  (with-elasticsearch-connection [^Connection conn, database]
    (let [table-name    (:name table)
          names         (str/split table-name  #"___")
          table-name    (nth names 0)
          type-name     (nth names 1 nil)
          table-name-kw (keyword table-name)
          properties    (if (nil? type-name)
                          (:properties (table-name-kw (:mappings (table-name-kw (idx/get-mapping conn table-name)))))
                          (:properties ((keyword type-name) (:mappings (table-name-kw (idx/get-mapping conn table-name type-name))))))]
      {:schema nil
       :name   (:name table)
       :fields (set (remove nil? (for [[field-name field-info] properties]
                                   (let [data-type (:type field-info)]
                                     (when-let [base-type (cond
                                                            (= data-type "integer")    :type/Integer
                                                            (= data-type "long")       :type/Integer
                                                            (= data-type "float")      :type/Float
                                                            (= data-type "text")       :type/Text
                                                            (= data-type "keyword")    :type/Text
                                                            (= data-type "date")       :type/Date
                                                            )]
                                       {:name          (name field-name)
                                        :base-type     (keyword base-type)
                                        :database-type data-type}
                                       )))))})))


(defn- process-query-in-context [qp]
  (fn [{database-id :database, :as query}]
    (with-elasticsearch-connection [^Connection conn, (db/select-one [Database :details], :id database-id)]
      (qp query))))

(defrecord ElasticSearchDriver []
  clojure.lang.Named
  (getName [_] "ElasticSearch"))

(u/strict-extend ElasticSearchDriver
  driver/IDriver
  (merge driver/IDriverDefaultsMixin
         {:can-connect?               (u/drop-first-arg can-connect?)
          :describe-database          (u/drop-first-arg describe-database)
          :describe-table             (u/drop-first-arg describe-table)
          :details-fields             (constantly (ssh/with-tunnel-config
                                                    [{:name           "host"
                                                      :display-name   "Host"
                                                      :placeholder    "localhost"}
                                                     {:name           "port"
                                                      :display-name   "Port"
                                                      :type           :integer
                                                      :default        9200}
                                                     {:name           "alias"
                                                      :display-name   "Alias name"
                                                      :placeholder    ""
                                                      :required       false}]))
          :execute-query              (u/drop-first-arg qp/execute-query)
          ;:features                   (constantly #{:basic-aggregations :dynamic-schema :standard-deviation-aggregations})
          :features                   (constantly #{:dynamic-schema :native-parameters})
          :mbql->native               (u/drop-first-arg qp/mbql->native)
          :process-query-in-context   (u/drop-first-arg process-query-in-context)
        }
    ))

(defn -init-driver
  "Register the ElasticSearch driver"
  []
  (driver/register-driver! :elasticsearch (ElasticSearchDriver.)))
