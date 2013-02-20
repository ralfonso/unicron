(ns unicron.core-test
  (:use clojure.test
        unicron.core))

(def backoff-intervals (-> default-config :backoff :intervals))

(deftest test-backoff
  (testing "http-backoff"
    (is (= 5000 (dispatch-backoff {:http-error 2} :http-error backoff-intervals)))
    (is (= 20000 (dispatch-backoff {:http-error 4} :http-error backoff-intervals))))
  (testing "network-backoff"
    (is (= 250 (dispatch-backoff {:network-error 2} :network-error backoff-intervals)))
    (is (= 500 (dispatch-backoff {:network-error 3} :network-error backoff-intervals)))
    (is (= 1500 (dispatch-backoff {:network-error 7} :network-error backoff-intervals)))))

(deftest test-handle-error
  (testing "handle-error"
    (let [base-control {:network-error 0 :http-error 0}
          errors-in-control {:network-error 5 :http-error 1}]
      (is (= (handle-error base-control :network-error) {:network-error 1 :http-error 0}))
      (is (= (handle-error base-control :http-error) {:network-error 0 :http-error 1}))
      (is (= (handle-error errors-in-control :disconnect) {:network-error 0 :http-error 0})))))

(deftest test-twitter-stream-connect
  (testing "network-error, connection refused"
    (let [bad-url "http://NONEXISTENTHOSTNAME.nontld"]
      (is (= :network-error (twitter-stream-connect (fn) bad-url {:user "none" :password "none"} { }))))))
