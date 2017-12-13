(ns metabase.driver.elasticsearch.query-processor
  (:require [cheshire.core :as json]
            [clojure
             [set :as set]
             [string :as str]
             [walk :as walk]]
            [metabase.driver.elasticsearch.util :refer [*elasticsearch-connection*]]
            [clojurewerkz.elastisch.rest :as es]
            [clojurewerkz.elastisch.rest.response :as resp]
            [clojurewerkz.elastisch.rest.utils :refer [join-names]]
            [clojurewerkz.elastisch.rest
             [index :as idx]]
            [clojurewerkz.elastisch
             [query :as qry]
             [aggregation :as agg]]
            [metabase.util :as u])
  (:import [metabase.query_processor.interface AgFieldRef DateTimeField DateTimeValue Field RelativeDateTimeValue Value]))


(defprotocol ^:private IRValue
  (^:private ->rvalue [this]))

(extend-protocol IRValue
  nil           (->rvalue [_] nil)
  Object        (->rvalue [this] this)
  Field         (->rvalue [this] (:field-name this))
  DateTimeField (->rvalue [this] (->rvalue (:field this)))
  Value         (->rvalue [this] (:value this))
  DateTimeValue (->rvalue [{{unit :unit} :field, value :value}] (u/format-date "yyyy-MM-dd" (u/date-trunc unit value))))

(defn- flatten-row [row]
  ;(println (str "row: " row))
  (reduce-kv (fn [m k v]
               (if (map? v)
                 (if (map? (:meta v))
                   (assoc m (keyword k) ((first (keys (:meta v))) v))
                   (assoc m (keyword k) (first (vals v))))
                 (assoc m (keyword k) v))) {} row))


(defn- rename-aggregations-map-keys [results meta]
  ;(println (str "meta: " meta))
  ;(println (str "results: " results))
  (if (seq meta)
    (for [row results]
      (set/rename-keys (select-keys row (keys meta)) meta))
    results))

(defn- extract-aggregations-map [aggregations]
  (let [meta    (into {} (for [agg-kw (keys aggregations)]
                           (let [agg    (agg-kw aggregations)
                                 meta   (:meta agg)
                                 value  (:value agg)]
                             (when (some? value)
                               meta))))
        results (cons (into {} (for [agg-kw (keys aggregations)]
                                 (let [agg    (agg-kw aggregations)
                                       value  (:value agg)]
                                   (when (some? value)
                                     {agg-kw value}))))
                  '())
        results (rename-aggregations-map-keys results meta)]
    (if (seq (first results))
      results
      nil)))

(defn- extract-aggregations-stats-map [aggregations]
  (let [meta    (into {} (for [agg-kw (keys aggregations)]
                           (let [agg    (agg-kw aggregations)
                                 meta   (:meta agg)
                                 count  (:count agg)]
                             (when (some? count)
                               meta))))
        results (cons (into {} (for [agg-kw (keys aggregations)]
                                 (let [agg    (agg-kw aggregations)
                                       count  (:count agg)]
                                   (when (some? count)
                                     (dissoc agg :std_deviation_bounds)))))
                  '())
        results (rename-aggregations-map-keys results meta)]
    (if (seq (first results))
      results
      nil)))

(defn- extract-aggregations-buckets [aggregations]
  (into {} (for [agg-kw (keys aggregations)]
             (let [agg      (agg-kw aggregations)
                   meta     (:meta agg)
                   buckets  (:buckets agg)
                   buckets  (if (map? buckets)
                              (let [key-values    (keys buckets)]
                                (for [key-value key-values]
                                  (let [map-entry       (key-value buckets)]
                                    (merge map-entry {agg-kw (name key-value)}))))
                              buckets)]
               (when (some? buckets)
                 {agg-kw (rename-aggregations-map-keys (seq buckets) meta)})))))

(defn- extract-aggregations-results [aggregations]
  (let [aggs-map       (extract-aggregations-map aggregations)
        aggs-buckets   (first (vals (extract-aggregations-buckets aggregations)))
        aggs-stats     (extract-aggregations-stats-map aggregations)]
    ;(println (str "map" aggs-map))
    ;(println (str "buckets" aggs-buckets))
    ;(println (str "stats: " aggs-stats))
    ;(println (str "buckets: " (extract-aggregations-buckets aggregations)))
    (cond
      (seq aggs-map)     aggs-map
      (seq aggs-stats)   aggs-stats
      (seq aggs-buckets) aggs-buckets)))

(defn- aggregations-filter-results? [aggregations]
  (let [aggs-buckets   (first (vals aggregations))
        buckets        (:buckets aggs-buckets)]
    (map? buckets)))

(defn- extract-aggregations-filter-columns [aggregations]
  (let [meta           (into {} (for [agg-kw (keys aggregations)]
                                  (let [agg    (agg-kw aggregations)
                                        meta   (:meta agg)]
                                    meta)))
        aggs-buckets   (first (vals aggregations))
        buckets        (:buckets aggs-buckets)
        key-column     (first (keys aggregations))
        value-column   (first (keys (first (vals buckets))))
        key-column     (keyword (get meta key-column (name key-column)))
        value-column   (keyword (get meta value-column (name value-column)))]
    (list key-column value-column)))

(defn- extract-search-results [response]
  (let [sources       (resp/sources-from response)
        sources?      (seq sources)
        aggregations  (resp/aggregations-from response)
        aggregations? (seq aggregations)
        filter?       (aggregations-filter-results? aggregations)
        aggs-results  (if aggregations?
                        (extract-aggregations-results aggregations)
                        '())
        total-hits    (resp/total-hits response)
        columns       []
        columns       (if sources?
                        (concat columns (vec (keys (first sources))))
                        columns)
        first-aggs    (first aggs-results)
        columns       (if aggregations?
                        (if (map-entry? first-aggs)
                          [(str (key first-aggs))]
                          (concat columns (vec (keys (walk/keywordize-keys first-aggs)))))
                        columns)
        columns       (if filter?
                        (extract-aggregations-filter-columns aggregations)
                        columns)
        columns       (if (empty? columns)
                        '(:count)
                        columns)
        results       '()
        results       (if sources?
                        (concat results sources)
                        results)
        results       (if aggregations?
                        (concat results aggs-results)
                        results)
        results       (if (empty? results)
                        (cons {:count total-hits} '())
                        results)]
    {:columns   columns
     :rows      (for [row results]
                  (mapv (flatten-row row) columns))
     :annotate? true}))

;;; ### initial search
(defn- add-initial-search [query search-ctx]
  {:query {:query {:bool {:filter   []}}
           :size 0}})

;;; ### limit
(defn- handle-limit [{:keys [limit]} search-ctx]
  (if-not limit
    search-ctx
    (update search-ctx :query conj {:size limit})))

;;; ### page
(defn- handle-page [{{page-num :page items-per-page :items, :as page-clause} :page} search-ctx]
  (if-not page-clause
    search-ctx
    (update search-ctx :query into {:size items-per-page
                                    :from (* items-per-page (dec page-num))})))
;;; ### handle-order-by
(defn- handle-order-by [{:keys [order-by]} search-ctx]
  (if-not (seq order-by)
    search-ctx
    (update search-ctx :query conj {:sort (into (vector)
                                            (for [{:keys [field direction]} order-by]
                                              {(->rvalue field) (case direction
                                                                  :ascending  "asc"
                                                                  :descending "desc")}))})))

;;; ### fields
(defn- handle-fields [{:keys [fields]} search-ctx]
  (if-not (seq fields)
    search-ctx
    (update search-ctx :query conj {:_source (into (vector)
                                               (for [field fields]
                                                 (->rvalue field)))})))

;;; ### filter


;(defn- parse-filter-subclause [{:keys [filter-type field value] :as filter} & [negate?]]
;  (let [field (when field (->))]))
;
;(defn- parse-filter-clause [{:keys [compound-type subclause subclauses], :as clause}]
;  (case compound-type
;    :and  {:filter (mapv parse-filter-clause subclauses)}
;    :or   {:should (mapv parse-filter-clause subclauses)}
;    :not  (parse-filter-subclause subclause :negate)
;    nil   (parse-filter-subclause clause)))

(defn- handle-filter [{filter-clause :filter} search-ctx]
  (if-not (seq filter-clause)
    search-ctx
    search-ctx))

;;; ### aggregation

;(defn- ag-type->field-name [ag-type]
;  (when ag-type
;    (if (= ag-type :distinct)
;      "count"
;      (name ag-type))))
;
;(defn- aggregation->rvalue [{:keys [aggregation-type field]}]
;  {:pre [(keyword? aggregation-type)]}
;  (if-not field
;    (case aggregation-type
;      :count  {})
;    (case aggregation-type
;      :avg    {:field-kw  (agg/avg (->lvalue field))})))

;(defn- handle-breakout+aggregation [{breakout-fields :breakout, aggregations :aggregation} search-ctx]
;  (let [aggregations? (seq aggregations)
;        breakout?     (seq breakout-fields)]
;    (if-not (or aggregations? breakout?)
;      search-ctx
;      (update search-ctx :query conj {:aggregations (into (hash-map)
;                                                      (for [{:keys [ag-type field custom-name]} aggregations]
;                                                        (let [field-name (:file-name field)
;                                                              field-kw  (keyword (str field "_count")])))}))))

(defn- handle-breakout+aggregation [{breakout-fields :breakout, aggregations :aggregation} search-ctx]
  (let [aggregations? (seq aggregations)
        breakout?     (seq breakout-fields)]
    (if-not (or aggregations? breakout?)
      search-ctx
      search-ctx)))

(defn- debug-mbql-query [mbql-query search-ctx]
  (let [result  (println (str "mbql: " mbql-query))]
    search-ctx))


(defn- generate-search-query
  "Generate the search command."
  [query]
  (:query (reduce (fn [search-ctx f]
                    (f query search-ctx))
            {:query {}}
            ;[debug-mbql-query
            [add-initial-search
             handle-filter
             handle-breakout+aggregation
             handle-fields
             handle-order-by
             handle-limit
             handle-page])))

;;; ------------------------------------------------------------ Query Execution ------------------------------------------------------------

(defn mbql->native
  "Process and run an MBOL query."
  [{database :database, {{source-table-name :name} :source-table} :query, :as query}]
  {:pre [(map? database)
         (string? source-table-name)]}
  {:query (generate-search-query (:query query))
   :collection source-table-name
   :mbql? true})

(defn execute-query
  "Process and run a native ElasticSearch query."
  [{{:keys [collection query mbql?]} :native,  database :database :as raw}]
  {:pre [query
         (string? collection)
         (map? database)]}
  (let [conn          *elasticsearch-connection*
        query         (if-not (string? query)
                        (json/encode query)
                        query)
        names         (str/split collection  #"___")
        collection    (nth names 0)
        mapping-type  (nth names 1 nil)
        collection-kw (keyword collection)
        ;results       (println (str "query: " query))
        results       (es/post-string conn  (if (nil? mapping-type)
                                              (es/search-url conn (join-names collection))
                                              (es/search-url conn (join-names collection)(join-names mapping-type)))
                        {:content-type    :json
                         :body            query})]
                         ;:debug           true
                         ;:debug-body      true})]
    ;(println (str "results: " results))
    (extract-search-results results)))
