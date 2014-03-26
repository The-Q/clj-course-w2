(ns task02.query
  (:use [task02 helpers db]
        [clojure.core.match :only (match)]))


(defn- resolve-func [fn-name]
  (if (= fn-name "!=")
    @(resolve 'not=)
    @(resolve (symbol fn-name))))

(defn make-where-function [& args]
  (let [column  (keyword (nth args 0))
        comp-op (resolve-func (nth args 1))
        value   (cond
                   (re-matches #"\d+" (nth args 2)) (parse-int (nth args 2))
                   (re-matches #"('(\w+)')" (nth args 2)) (last (re-matches #"('(\w*)')" (nth args 2))))]
    #(comp-op (column %) value)))
;; Функция выполняющая парсинг запроса переданного пользователем
;;
;; Синтаксис запроса:
;; SELECT table_name [WHERE column comp-op value] [ORDER BY column] [LIMIT N] [JOIN other_table ON left_column = right_column]
;;
;; - Имена колонок указываются в запросе как обычные имена, а не в виде keywords. В
;;   результате необходимо преобразовать в keywords
;; - Поддерживаемые операторы WHERE: =, !=, <, >, <=, >=
;; - Имя таблицы в JOIN указывается в виде строки - оно будет передано функции get-table для получения объекта
;; - Значение value может быть либо числом, либо строкой в одинарных кавычках ('test').
;;   Необходимо вернуть данные соответствующего типа, удалив одинарные кавычки для строки.
;;
;; - Ключевые слова --> case-insensitive
;;
;; Функция должна вернуть последовательность со следующей структурой:
;;  - имя таблицы в виде строки
;;  - остальные параметры которые будут переданы select
;;
;; Если запрос нельзя распарсить, то вернуть nil

;; Примеры вызова:
;; > (parse-select "select student")
;; ("student")
;; > (parse-select "select student where id = 10")
;; ("student" :where #<function>)
;; > (parse-select "select student where id = 10 limit 2")
;; ("student" :where #<function> :limit 2)
;; > (parse-select "select student where id = 10 order by id limit 2")
;; ("student" :where #<function> :order-by :id :limit 2)
;; > (parse-select "select student where id = 10 order by id limit 2 join subject on id = sid")
;; ("student" :where #<function> :order-by :id :limit 2 :joins [[:id "subject" :sid]])
;; > (parse-select "werfwefw")
;; nil

(defn parse-select [^String sel-string]
  (let [possible-where-fns #{"=" "!=" "<" ">" "<=" ">="}
        parse-select-hpr (fn [coll acc tbl]
                           (match coll
                                  [] (apply concat [tbl] acc)
                                  ["select" tbl-name & r] (recur (vec r) acc tbl-name)
                                  ["limit" lim & r] (recur (vec r) (assoc acc :limit (parse-int lim)) tbl)
                                  ["order" "by" column & r] (recur (vec r) (assoc acc :order-by (keyword column)) tbl)
                                  ["where" column (comp-op :guard #(possible-where-fns %)) value & r] (recur
                                                                                                       (vec r)
                                                                                                       (assoc acc
                                                                                                             :where
                                                                                                             (make-where-function column comp-op value))
                                                                                                       tbl)
                                  ["join" table "on" left "=" right & r] (recur (vec r) (update-in acc
                                                                                                   [:joins]
                                                                                                   (fnil #(conj % [(keyword left)
                                                                                                                   table
                                                                                                                   (keyword right)])
                                                                                                         [])) tbl)
                                  :else nil))
        query-vec (vec (.split sel-string " "))]
    (parse-select-hpr query-vec {} "")))

;; Выполняет запрос переданный в строке.  Бросает исключение если не удалось распарсить запрос

;; Примеры вызова:
;; > (perform-query "select student")
;; ({:id 1, :year 1998, :surname "Ivanov"} {:id 2, :year 1997, :surname "Petrov"} {:id 3, :year 1996, :surname "Sidorov"})
;; > (perform-query "select student order by year")
;; ({:id 3, :year 1996, :surname "Sidorov"} {:id 2, :year 1997, :surname "Petrov"} {:id 1, :year 1998, :surname "Ivanov"})
;; > (perform-query "select student where id > 1")
;; ({:id 2, :year 1997, :surname "Petrov"} {:id 3, :year 1996, :surname "Sidorov"})
;; > (perform-query "not valid")
;; exception...
(defn perform-query [^String sel-string]
  (if-let [query (parse-select sel-string)]
    (apply select (get-table (first query)) (rest query))
    (throw (IllegalArgumentException. (str "Can't parse query: " sel-string)))))
