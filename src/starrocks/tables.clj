(ns starrocks.tables
  (:refer-clojure :exclude [test])
  (:require [clojure.string :as str]
            [jepsen [client :as client]
             [checker :as checker]
             [generator :as gen]]
            [knossos.op :as op]
            [clojure.tools.logging :refer [info warn]]
            [starrocks.sql :as c :refer :all]
            [starrocks.nemesis :as nemesis]))

(defrecord SetClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (c/execute! conn ["create database if not exists test"])

    (c/execute! conn ["use test"]))

  (invoke! [this test op]
    (case (:f op)
      :create  (do (c/execute! conn [(str "create table test.t" (:value op) " (a int) distributed by hash(a) properties(\"replication_num\" = \"1\")")])
                   (assoc op :type :ok))

      :show    (do (c/query conn ["show tables from test"])
                   (assoc op :type :ok, :value []))))

  (teardown! [_ test])

  (close! [_ test]
    (c/close! conn)))

(defn creates
  []
  (->> (range)
       (map (fn [x] {:type :invoke, :f :create, :value x}))
       (gen/seq)))

(defn shows
  []
  {:type :invoke, :f :show, :value nil})

(defn workload
  [opts]
  (info "workload called")
  (let [c (:concurrency opts)]
    {:client    (SetClient. nil)
     :nemesis   (nemesis/process-nemesis)
     :generator (->> (gen/reserve (- c 1) (creates) (shows))
                     (gen/stagger 1/10)
                     (gen/nemesis (nemesis/kill-gen))
                     (gen/time-limit 600))
     :checker   (checker/set-full)}))
