(ns unicron.core
  (:use     [clojure.tools.logging :only (debug debugf info infof error errorf enabled?)])
  (:require [http.async.client :as client]))

(defn create-handler
   "creates a handler for http.async.client that accepts a response state (status/headers) and a
   vec of bytes that represents the body checks the status, converts the body to string, and
   passes it along to the subscribers"
  [config]
  (let [user-handler (-> config :handler)]
    (fn handle-body
      [state body]
      (let [status (client/status state)
            headers (client/headers state)]
        ; we track different types of disconnects because we have to
        ; back off in different manners for each
        (debugf "status: %s" status)
        (debugf "headers %s" headers)
        (if status
          (if (= (:code status) 200)
            (let [tweet-str (client/string headers body)]
              (debugf "body: %s" tweet-str)
              (user-handler status headers body tweet-str))))))))


(defn twitter-stream-connect
  "creates an HTTP client and attempts to connect to the Streaming API.  Uses an async callback system
   to handle incoming data.  Returns one of three keywords to the worker loop:
    :network-error (represents a connection timeout or other TCP error)
    :http-error (we connected to the host, but received a non-200 HTTP status)
    :disconnect (this is a normal disconnect, it occurs if the stream is broken, we don't back off for disconnects
                 of this type)"
  [handler twitter-api-url auth query]
  (let [http-client (client/create-client :request-timeout -1
                                          :connection-timeout 10000
                                          :keepalive false
                                          :auth auth)]
    (try
      (infof "Connecting to the Twitter Streaming API: %s. Params %s" twitter-api-url query)
      (let [response (client/await (client/request-stream http-client :get twitter-api-url handler :query query))
            err (client/error response)
            status (client/status response)]
          (if err
            (do
              (if (enabled? :debug) (.printStackTrace err))
              (infof "Connection error: %s" err)
              :network-error)
            (if status
              (if (> (:code status) 200)
                (do
                  (infof "Non-200 status received: %s" (:code status))
                  :http-error)
                (do
                  (infof "Disconnected")
                  :disconnect))
              ; no status, no error? NETWORK
              :network-error)))
    (catch Exception e
      (infof "Received network error on connect: %s" e)
      (if (enabled? :debug) (.printStackTrace e))
      :network-error))))

(defn handle-error
  "takes a map that represents the current control status (consectutive error counts)
   and the last-received error.  Handles incrementing the counts or resetting them
   if we encounter a legitimate disconnect"
  [control error]
  (condp = error
    :disconnect
      ; if we get a simple disconnect, that means we had at least one good connection, so reset the control map
      {:http-error 0 :network-error 0}
    (update-in control [error] inc)))

(defn dispatch-backoff
  "controls the sleep/backoff. linearly for network errors and exponentially for HTTP errors/rate limiting
  returns the number of seconds that we slept"
  [control error backoff-intervals]
  (let [error-count (- (error control) 2) ; the intervals are zero-indexed, but we only care about errors after the first one
        backoff-interval (error backoff-intervals)]
    (nth backoff-interval error-count)))

(defn worker
  "an infinite loop that connects to the Streaming API and hands off tweets to our processor"
  [config handler]
  (let [auth {:user (-> config :auth :username)
              :password (-> config :auth :password)
              :preemptive true}
        track-hashtags (apply str (interpose "," (-> config :filters)))
        backoff-intervals (-> config :backoff :intervals)]
    (loop [control {:http-error 0
                    :network-error 0}]
      ;; twitter-stream-connect blocks until is disconnects, where it will return a keyword
      ;; specifying the error
      (let [error (twitter-stream-connect handler (-> config :api-url) auth {:track track-hashtags})
            loop-control (handle-error control error)
            backoff-in-ms (dispatch-backoff loop-control error backoff-intervals)]
        (infof "Error received: %s" error)
        (if (> backoff-in-ms 0) ; no backoff for the first error
          (do
            (infof "Backoff: sleeping for %sms" backoff-in-ms)
            (Thread/sleep backoff-in-ms)))
        (recur loop-control)))))

(defn default-handler
  [status headers body tweet-string]
  (infof "Tweet received %s" tweet-string))

(def default-config
  {:backoff {:intervals {:network-error (iterate #(min 16000 (+ 250 %)) 250)
                         :http-error (iterate #(min 320000 (* 2 %)) 5000)}}
   :handler default-handler})

(defn create-worker
  [config]
  (let [config (merge default-config config)
        handler (create-handler config)]
    (worker config handler)))
