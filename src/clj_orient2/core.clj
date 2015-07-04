(ns clj-orient2.core
  (:import (com.tinkerpop.blueprints.impls.orient OrientGraph
                                                  OrientGraphFactory))
  (:import (com.orientechnologies.orient.core.db ODatabaseRecordThreadLocal))
  (:import (com.orientechnologies.orient.core.sql OCommandSQL)))

(defn get-graph [url user pass]
  "Return a transaction graph from the current graph factory."
  (.getTx @graph-factory)
  (OrientGraph. url user pass))

(defn commit [g]
  (.commit g))

(defn rollback [g]
  (.rollback g))

(defn close-graph [g]
  (.shutdown g))

(defmacro with-orient [g & body]
  `(let [~g (orient-tx)]
     (try ~@body
          (finally (close-graph ~g)))))

(defn sql [g & c]
  "Return a sequence of results from running sql command c on graph g.
Applies str to c."
  (seq (.execute (.command g (OCommandSQL. (apply str c))) nil)))

(defn sql' [g & c]
  "Return a result from running sql command c on graph g.  Does not run seq on its output."
  (.execute (.command g (OCommandSQL. (apply str c))) nil))

(defn get-vertices-of-class [g c]
  (.getVerticesOfClass g c))

(defn make-vertex [g class kv-map]
  (let [s (.addVertex g (str "class:" class))]
    (doseq [[k v] kv-map] (when v (.setProperty s (name k) v)))
    s))

(defn make-edge [g class from to label kv-map]
  (let [e (.addEdge g (str "class:" (or class "E")) from to (name label))]
    (doseq [[k v] kv-map] (when v (.setProperty e (name k) v)))
    e))

(defn remove-edge [g e]
  (.removeEdge g e))

(defn remove-edges [g es]
  (doseq [e es]
    (.removeEdge g e)))

(defn remove-vertex [g v]
  (.removeVertex g v))

(defn get-property [v p]
  (.getProperty v (name p)))

(defn set-property [v key value]
  (.setProperty v (name key) value))

(defn set-properties [v kv-map]
  (doseq [[key value] kv-map]
    (.setProperty v (name key) value)))

(defn get-id [v]
  (.toString (.getId v)))

(defn vertex-to-map [v]
  (reduce (fn [h p]
            (if (re-matches #"(in|out)_.*" p) h
                (assoc h (keyword p) (.getProperty v p))))
          {(keyword "@rid") (get-id v)} (.getPropertyKeys v)))

(defn edge-to-map [v]
  (reduce (fn [h p]
            (if (re-matches #"(in|out)_.*" p) h
                (assoc h (keyword p) (.getProperty v p))))
          {(keyword "@rid") (get-id v)} (.getPropertyKeys v)))
