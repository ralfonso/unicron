(ns unicron.core
  (:use     [clojure.tools.logging :only (debug debugf info infof error errorf enabled?)])
  (:require [http.async.client :as client]))

(defn create-handler
   "creates a handler for http.async.client that accepts a response state (status/headers) and a
   vec of bytes that represents the body checks the status, converts the body to string, and
   passes it along to the user-defined handler"
  [user-handler]
  (fn [state body]
    (let [status (client/status state)
          headers (client/headers state)]
      ; we track different types of disconnects because we have to
      ; back off in different manners for each
      (debugf "status: %s" status)
      (debugf "headers %s" headers)
      (if status
        (if (= (:code status) 200)
          (let [tweet-str (client/string headers body)]
            (user-handler status headers body tweet-str))))
      [body :continue])))

(defn dispatch-backoff
  "controls the sleep/backoff. linearly for network errors and exponentially for HTTP errors/rate limiting
  returns the number of seconds that we slept"
  [errors error]
  (let [error-count (- (-> @errors :counts error) 2)] ; the intervals are zero-indexed, but we only care about errors after the first one
    (if (>= error-count 0)
      (let [backoff-interval (deref (-> @errors :intervals error))
            backoff-in-ms (nth backoff-interval error-count)]
        (if (> backoff-in-ms 0) ; no backoff for the first error
          (do
            (infof "Backoff: sleeping for %sms" backoff-in-ms)
            (Thread/sleep backoff-in-ms)))))))

(defn handle-error
  "takes a map that represents the current control status (consectutive error counts)
   and the last-received error.  Handles incrementing the counts or resetting them
   if we encounter a legitimate disconnect"
  [errors current-error]
  (condp = current-error
    :disconnect
      ; if we get a simple disconnect, that means we had at least one good connection, so reset the control map
      (swap! errors assoc :counts {:http-error 0 :network-error 0})
    (do
      (swap! errors update-in [:counts current-error] inc)
      (dispatch-backoff errors current-error))))

(defn twitter-stream-connect
  "creates an HTTP client and attempts to connect to the Streaming API.  Uses an async callback system to
   handle the streamed response"
  [http-client run-control & {:as options}]
  (let [url (-> options :config :api-url)
        handler-base (-> options :config :handler)
        handler (create-handler handler-base)
        backoff-intervals (-> options :config :backoff :intervals)
        query (-> options :config :query)]
    (loop [errors (atom {:intervals backoff-intervals :counts {:http-error 0 :network-error 0}})]
      (if (= @run-control :stop)
        (Thread/sleep 1000) ;FIXME: how to do this without using a spinloop?
        (do
          (try
            (infof "Connecting to the Twitter Streaming API: %s. Params %s" url query)
            (let [resp (client/request-stream http-client :get url handler :query query)]
              (loop []
                (if (= @run-control :stop)
                  (client/cancel resp) ; if someone stopped the worker then cancel the request
                  (if (client/done? resp)
                    (let [err (client/error resp)
                          status (client/status resp)]
                      (if err
                        (do
                          (infof "Connection error: %s" err)
                          (handle-error errors :network-error))
                        (if status
                          (if (not= (:code status) 200)
                            (do
                              (infof "Non-200 status received: %s" (:code status))
                              (handle-error errors :http-error))
                            (do
                              (infof "Disconnected")
                              (handle-error errors :disconnect)))
                          ; no status, no error? blame the network!
                          (handle-error errors :network-error))))
                    (recur)))))
            (catch Exception e
              (infof "Received network error on connect: %s" e)
              (handle-error errors :network-error)))))
            (recur errors))))

(defprotocol WorkerBase
  (start-worker [worker])
  (stop-worker [worker]))

(defrecord Worker [run-control config client]
  WorkerBase
  (start-worker [worker]
    (reset! (:run-control worker) :start))
  (stop-worker [worker]
    (reset! (:run-control worker) :stop)))

(defn default-handler
  [status headers body tweet-string]
  (infof "Tweet received. len %s" (count tweet-string))
  [body :continue])

(def default-config
  {:backoff {:intervals {:network-error (delay (iterate #(min 16000 (+ 250 %)) 250))
                         :http-error (delay (iterate #(min 320000 (* 2 %)) 5000))}}
   :handler default-handler})

(defn create-worker
  [config]
  ;track-hashtags (if filters (apply str (interpose "," filters)))
  (let [config (merge default-config config)
        run-control (atom :stop)
        auth (:auth config)
        auth (if auth (assoc auth :preemptive true))
        http-client (client/create-client :request-timeout -1
                                          :connection-timeout 10000
                                          :keepalive false
                                          :auth auth)
        client (future (twitter-stream-connect http-client run-control :config config))]
    (Worker. run-control config client)))
