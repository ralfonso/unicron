(ns unicron.core-test
  (:use clojure.test
        unicron.core))

(def backoff-intervals (-> default-config :backoff :intervals))

(deftest test-handle-error
  (testing "handle-error"
    (let [errors (atom {:intervals backoff-intervals :counts {:network-error 0 :http-error 0}})]
      (handle-error errors :network-error)
      (is (= (-> @errors :counts :network-error) 1)))
    (let [errors (atom {:intervals backoff-intervals :counts {:network-error 0 :http-error 0}})]
      (handle-error errors :http-error)
      (is (= (-> @errors :counts :http-error) 1)))
    (let [errors (atom {:intervals backoff-intervals :counts {:network-error 5 :http-error 3}})]
      (handle-error errors :disconnect)
      (is (= (-> @errors :counts) {:network-error 0 :http-error 0})))))
