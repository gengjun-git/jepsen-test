(defproject starrocks "0.1.1-SNAPSHOT"
  :description "Jepsen testing for StarRocks"
  :url "https://starrocks.com/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main starrocks.core
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [clj-http "3.10.0"]
                 [cheshire "5.8.1"]
                 [jepsen "0.1.15-SNAPSHOT"]
                 [org.clojure/java.jdbc "0.7.9"]
                 [org.mariadb.jdbc/mariadb-java-client "2.4.1"]]
  :aot [starrocks.core clojure.tools.logging.impl])
