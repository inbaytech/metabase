(ns metabase.driver.elasticsearch.util
  "`*elasticsearch-connection*`, `with-elasticsearch-connection`, and other functions shared between several ElasticSearch driver namespaces."
  (:require [clojure.tools.logging :as log]
            [metabase.util :as u]
            [metabase.util.ssh :as ssh]
            [clojurewerkz.elastisch.rest :as es])
  (:import clojurewerkz.elastisch.rest.Connection))


(def ^:dynamic ^clojurewerkz.elastisch.rest.Connection *elasticsearch-connection*
  "Connection to a ElasticSearch server.
   Bound by top-level `with-elasticsearch-connection` so it may be reused within its body."
  nil)

(defn- database->details
  "Make sure DATABASE is in a standard db details format. This is done so we can accept several different types of
   values for DATABASE, such as plain strings or the usual MB details map."
  [database]
  (cond
    (string? database)            {:dbname database}
    (:dbname (:details database)) (:details database)
    (:dbname database)            database
    (:host (:details database))   (:details database)
    (:host database)              database
    :else                         (throw (Exception. (str "with-elasticsearch-connection failed: bad connection details:")))))

(defn- details->uri
  [{:keys [host port]
    :or { port 9200 }}]
  (str "http://" host ":" port))

(defn -with-elasticsearch-connection
  "Run F with a new connection (bound to `*elasticsearch-connection` to DATABASE.
   Don't use this directly; use `with-elasticsearch-connection`."
  [f database]
  (let [details (database->details database)]
    (ssh/with-ssh-tunnel [details-with-tunnel details]
      (let [elasticsearch-connection (es/connect (details->uri details))]
        (log/debug (u/format-color 'cyan "<< PREPARED NEW ELASTICSEARCH REQUEST(S) >>"))
        (try
          (binding [*elasticsearch-connection* elasticsearch-connection]
            (f *elasticsearch-connection*))
          (finally (log/debug (u/format-color 'cyan "<< COMPLETED ELASTICSEARCH REQUEST(S)"))))))))

(defmacro with-elasticsearch-connection
  [[binding database] & body]
  `(let [f# (fn [~binding]
              ~@body)]
     (if *elasticsearch-connection*
       (f# *elasticsearch-connection*)
       (-with-elasticsearch-connection f# ~database))))
