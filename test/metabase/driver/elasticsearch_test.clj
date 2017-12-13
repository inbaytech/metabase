(ns metabase.driver.elasticsearch-test
  "Tests for ElasticSearch driver."
  (:require [expectations :refer :all]
            [metabase
              [query-processor-test :as pt]
              [driver :as driver]]
            [metabase.query-processor.middleware.expand :as ql]
            [metabase.models
             [field :refer [Field]]
             [table :as table :refer [Table]]]
            [metabase.test.data :as data]
            [metabase.test.data
             [datasets :as datasets]
             [interface :as i]]
            [metabase.driver :as driver])
  (:import metabase.driver.elasticsearch.ElasticSearchDriver))


;; ## Tests for connection functions

(datasets/expect-with-engine :elasticsearch
  false
  (driver/can-connect-with-details? :elasticsearch {:host "123.4.5.6"
                                                    :port 9200
                                                    :dbname "wrong-host-ip"}))

(datasets/expect-with-engine :elasticsearch
  false
  (driver/can-connect-with-details? :elasticsearch {}))

(datasets/expect-with-engine :elasticsearch
  true
  (driver/can-connect-with-details? :elasticsearch {:host "localhost"
                                                    :port 9200
                                                    :dbname "dbtest"}))

;; should use default port 9200 if not specified
(datasets/expect-with-engine :elasticsearch
  true
  (driver/can-connect-with-details? :elasticsearch {:host "localhost"}))


;; dbname is not mandatory for elasticsearch
(datasets/expect-with-engine :elasticsearch
  true
  (driver/can-connect-with-details? :elasticsearch {:host "localhost"
                                                    :port 9200}))

;; ## Tests for individual syncing functions

;; DESCRIBE-DATABASE
(datasets/expect-with-engine :elasticsearch
  {:tables #{{:schema nil, :name "checkins"}
             {:schema nil, :name "checkins___checkins"}
             {:schema nil, :name "categories"}
             {:schema nil, :name "categories___categories"}
             {:schema nil, :name "users"}
             {:schema nil, :name "users___users"}
             {:schema nil, :name "venues"}
             {:schema nil, :name "venues___venues"}}}
  (driver/describe-database (ElasticSearchDriver.) (data/db)))

;; DESCRIBE-TABLE
(datasets/expect-with-engine :elasticsearch
  {:schema nil
   :name   "venues"
   :fields #{{:name "name"
              :base-type :type/Text
              :database-type "text"}
             {:name "latitude"
              :base-type :type/Float
              :database-type "float"}
             {:name "longitude"
              :base-type :type/Float
              :database-type "float"}
             {:name "price"
              :base-type :type/Integer
              :database-type "long"}
             {:name "category_id"
              :base-type :type/Integer
              :database-type "long"}
             {:name "id",
              :base-type :type/Integer
              :database-type "long"}}}
  (driver/describe-table (ElasticSearchDriver.) (data/db) (Table (data/id :venues))))

;(pt/expect-with-non-timeseries-dbs
;  [[1]]
;  (->> (data/dataset places-cam-likes
;         (data/run-query places
;           (ql/aggregation (ql/count))
;           (ql/filter (ql/= $liked false))))
;    pt/rows (pt/format-rows-by [int])))
