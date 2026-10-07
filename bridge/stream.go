package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	es "github.com/launchdarkly/eventsource"
)

// Streaming flag delivery.
//
// Instead of polling /sdk/latest-all on an interval, the bridge holds one long-lived
// connection to LaunchDarkly's streaming service and is told about each change as it
// happens. The protocol is the same one the other LaunchDarkly server SDKs speak: a "put"
// event carrying every flag and segment opens each connection, and "patch" and "delete"
// events follow, one per changed item.
//
// The org cannot take an item at a time. Its store endpoint replaces a scope's flag data
// wholesale, so the bridge keeps its own copy of the environment in flagStore, applies each
// event to that, and pushes the whole set -- the same payload a poll would have pushed.
// The Apex side needs no change to be fed by the stream, which keeps either upgrade order
// safe.
//
// The push runs on its own goroutine, apart from the goroutine reading the stream, and it
// retries on its own schedule. That is what makes a Salesforce outage survivable under
// streaming. A poll has a next cycle to try again on, and a stream does not: with no
// retry, a change that arrived during the outage would be lost until the flag changed
// again.

const (
	// The event names the stream uses. Anything else is ignored, so a new event type the
	// service starts sending costs a log line rather than a reconnect.
	PUT_EVENT    = "put"
	PATCH_EVENT  = "patch"
	DELETE_EVENT = "delete"
	// STREAM_PATH is appended to LD_STREAM_URI. /all is the server-side stream, which
	// carries flags and segments together.
	STREAM_PATH = "/all"
	// STREAM_READ_TIMEOUT is how long the stream can stay silent before the bridge
	// drops the connection and dials again. LaunchDarkly sends a heartbeat comment every
	// three minutes or so, so five minutes of silence means the connection is dead in a
	// way TCP has not noticed yet. It matches the other LaunchDarkly server SDKs.
	//
	// It seeds a Bridge field rather than being read where the stream is opened, so
	// tests can shorten it.
	STREAM_READ_TIMEOUT = 5 * time.Minute
	// STREAM_MAX_RETRY_DELAY caps the exponential backoff between reconnection attempts,
	// STREAM_RETRY_RESET_INTERVAL is how long a connection has to stay up before the
	// backoff starts over at the initial delay, and STREAM_JITTER_RATIO is how much of
	// each delay is randomly shaved off so a fleet of bridges does not reconnect in step
	// after an outage. All three match the other LaunchDarkly server SDKs.
	STREAM_MAX_RETRY_DELAY      = 30 * time.Second
	STREAM_RETRY_RESET_INTERVAL = 60 * time.Second
	STREAM_JITTER_RATIO         = 0.5
	// FLAG_PUSH_RETRY_DELAY is how long flagPushLoop waits before it retries a push the
	// org refused or that never arrived, and FLAG_PUSH_MAX_RETRY_DELAY is where the
	// doubling stops. The cap equals the default poll interval, so a bridge retrying
	// against a Salesforce outage costs the org no more API calls than polling did.
	//
	// The initial delay seeds a Bridge field rather than being read where the push
	// happens, so tests can shorten it.
	FLAG_PUSH_RETRY_DELAY     = 1 * time.Second
	FLAG_PUSH_MAX_RETRY_DELAY = 30 * time.Second
)

// flagStore is the bridge's copy of the environment's flag data while streaming.
//
// It exists because the org's store endpoint has no way to change one item. The stream
// delivers items one at a time, so the bridge applies each change here and pushes the
// whole set.
type flagStore struct {
	mu sync.Mutex
	// kinds maps a kind -- "flags" or "segments" -- to its items by key. Kinds are kept
	// under the names LaunchDarkly uses, so the payload snapshot builds has the same
	// shape as the poll response the org already accepts.
	kinds map[string]map[string]flagItem
	// initialized reports whether a put has arrived. A patch or delete that somehow
	// precedes one is ignored, because a set assembled from patches alone is incomplete,
	// and pushing it would replace whatever complete set the org holds.
	initialized bool
}

// flagItem is one flag or segment, or the record of one having been deleted.
type flagItem struct {
	version int
	// raw is the item exactly as LaunchDarkly sent it, forwarded without being decoded
	// into a model the bridge would then have to keep up to date. It is nil for a
	// deleted item, whose version is kept so an older update that arrives after the
	// deletion cannot bring the item back.
	raw json.RawMessage
}

// replace discards everything held and takes the contents of a put.
//
// Every item has to parse as a JSON object with a version, because a patch that follows
// is ordered against that version. An item that does not is reported as an error, which
// the caller treats as a malformed event.
func (store *flagStore) replace(kinds map[string]map[string]json.RawMessage) error {
	replacement := make(map[string]map[string]flagItem, len(kinds))

	for kind, items := range kinds {
		replacement[kind] = make(map[string]flagItem, len(items))

		for key, raw := range items {
			version, err := versionOf(raw)
			if err != nil {
				return fmt.Errorf("%s %q: %w", kind, key, err)
			}
			replacement[kind][key] = flagItem{version: version, raw: raw}
		}
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	store.kinds = replacement
	store.initialized = true

	return nil
}

// upsert applies one item -- or, when item.raw is nil, one deletion -- and reports
// whether anything changed.
//
// An item only replaces one that is older. The stream can deliver events out of their
// natural order after a reconnect, and the versions are what put them back in order:
// an update carrying a version the store has already passed is one the store has
// already accounted for. Equal versions are also declined, which is what makes a
// redelivered event harmless. This matches the versioned upsert in every other
// LaunchDarkly server SDK store.
func (store *flagStore) upsert(kind, key string, item flagItem) bool {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.initialized {
		return false
	}

	items, ok := store.kinds[kind]
	if !ok {
		items = make(map[string]flagItem)
		store.kinds[kind] = items
	}

	if existing, ok := items[key]; ok && existing.version >= item.version {
		return false
	}

	items[key] = item

	return true
}

// snapshot serializes what the store holds into the payload the org's store endpoint
// accepts: each kind's live items by key, with deleted items left out. Each item is
// written as LaunchDarkly sent it.
func (store *flagStore) snapshot() ([]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	payload := make(map[string]map[string]json.RawMessage, len(store.kinds))

	for kind, items := range store.kinds {
		live := make(map[string]json.RawMessage, len(items))

		for key, item := range items {
			if item.raw != nil {
				live[key] = item.raw
			}
		}

		payload[kind] = live
	}

	return json.Marshal(payload)
}

// canonicalJSON rewrites a JSON document so that two documents with the same content
// come out byte-for-byte equal, whatever order their object keys arrived in.
//
// flagPushLoop compares snapshots through this rather than directly, because LaunchDarkly
// does not serialize an item the same way on every connection: the same flag arrives with
// its nested fields in a different order from one put to the next, so the bytes of two
// snapshots differ when the content does not. The payload pushed to the org is still the
// snapshot itself; only the comparison goes through here.
//
// Numbers are kept as the text they arrived as rather than decoded, so no precision is
// lost and no two distinct values can collapse into one.
func canonicalJSON(document []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.UseNumber()

	var value interface{}
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}

	// json.Marshal writes map keys in sorted order at every level.
	return json.Marshal(value)
}

// count reports how many live items of one kind the store holds. It is for log lines.
func (store *flagStore) count(kind string) int {
	store.mu.Lock()
	defer store.mu.Unlock()

	live := 0
	for _, item := range store.kinds[kind] {
		if item.raw != nil {
			live++
		}
	}

	return live
}

// versionOf reads the version out of one flag or segment without decoding the rest.
//
// The item has to be a JSON object. json.Unmarshal would accept null and leave the
// version at zero, and an item that is not an object would be forwarded to the org as
// one, where VersionedData's casts fail inside the push.
func versionOf(raw json.RawMessage) (int, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return 0, errors.New("item is not a JSON object")
	}

	var item struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(trimmed, &item); err != nil {
		return 0, err
	}

	return item.Version, nil
}

// parseStreamPath splits the path a patch or delete names -- "/flags/key" or
// "/segments/key" -- into its kind and key. A path naming anything else reports false,
// and the event is ignored rather than treated as malformed, which is how the other
// LaunchDarkly server SDKs leave room for item kinds they do not know about.
func parseStreamPath(path string) (kind, key string, ok bool) {
	for _, known := range []string{"flags", "segments"} {
		prefix := "/" + known + "/"
		if strings.HasPrefix(path, prefix) {
			return known, strings.TrimPrefix(path, prefix), true
		}
	}

	return "", "", false
}

// newStreamClient builds the HTTP client the stream connection uses.
//
// It is a separate client from the one every other request uses, because
// http.Client.Timeout bounds the whole exchange, body included, and a stream's body
// never ends: HTTP_TIMEOUT would cut every connection off after thirty seconds. What a
// stream needs bounded is the connection and the wait for response headers, so
// HTTP_TIMEOUT is applied to those instead. Silence after the headers is
// STREAM_READ_TIMEOUT's job.
//
// The transport is a copy of the default one, so proxy settings from the environment
// and the dial and TLS handshake timeouts carry over.
func newStreamClient(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if timeout > 0 {
		transport.ResponseHeaderTimeout = timeout
	}

	return &http.Client{Transport: transport}
}

// initFlagStream prepares the state streaming needs. newBridge and the test helper both
// call it, so the channel's capacity has one definition.
func (bridge *Bridge) initFlagStream(httpTimeout time.Duration) {
	// Capacity one. A signal that is already pending says everything a second one
	// would, because the push reads the store rather than the signal.
	bridge.flagsChanged = make(chan struct{}, 1)
	bridge.streamClient = newStreamClient(httpTimeout)
}

// notifyFlagsChanged wakes flagPushLoop. It never blocks: a signal already waiting
// covers this change as well, since the push snapshots the store when it runs.
func (bridge *Bridge) notifyFlagsChanged() {
	select {
	case bridge.flagsChanged <- struct{}{}:
	default:
	}
}

// streamLoop is featureLoop's streaming form. It returns when the bridge shuts down, with
// nil, or when something a retry cannot fix stops it, with that error.
//
// The eventsource library owns the connection: it reconnects with backoff, honors a
// retry delay the server sends, and gives up the connection after STREAM_READ_TIMEOUT
// of silence. What is decided here is which failures are worth retrying, and what each
// event does to the store.
func (bridge *Bridge) streamLoop() error {
	streamURI := bridge.launchDarklyStreamURI + STREAM_PATH

	// The bridge context rides on the request, which is how shutdown reaches the
	// stream: the library closes itself when the context ends, whether it is reading,
	// dialing, or waiting out a retry delay.
	request, err := http.NewRequestWithContext(bridge.context, "GET", streamURI, nil)
	if err != nil {
		// Logged as well as returned, for the reason given in pushFlags: run() only
		// reports the first error to arrive.
		log.Print("failed constructing stream request: ", err)
		return err
	}

	request.Header.Set("Authorization", bridge.launchDarklyKey)
	request.Header.Set("User-Agent", USER_AGENT)
	request.Header.Set(INSTANCE_ID_HEADER, bridge.instanceID)

	// The push runs apart from the stream so that a slow or failing org never holds up
	// reading events, and so that changes arriving during a push fold into the next one.
	// It stops when the context ends, which every exit below arranges.
	pushDone := make(chan error, 1)
	go func() { pushDone <- bridge.flagPushLoop() }()

	// fatal is set by the error handler, which the library calls on its own goroutine,
	// and read once the stream has closed.
	var fatalLock sync.Mutex
	var fatal error
	setFatal := func(err error) {
		fatalLock.Lock()
		defer fatalLock.Unlock()
		if fatal == nil {
			fatal = err
		}
	}
	getFatal := func() error {
		fatalLock.Lock()
		defer fatalLock.Unlock()
		return fatal
	}

	// The handler decides whether a failed connection is retried. The division matches
	// isHTTPErrorRecoverable, so the stream gives up on exactly the statuses the event
	// push gives up on: a rejected SDK key or a path the service does not serve answers
	// the same on every attempt, while a 5xx, a rate limit, or a dropped connection may
	// not.
	errorHandler := func(err error) es.StreamErrorHandlerResult {
		var refused es.SubscriptionError
		if errors.As(err, &refused) {
			if !isHTTPErrorRecoverable(refused.Code) {
				if refused.Code == 401 || refused.Code == 403 {
					log.Printf("the LaunchDarkly stream refused the SDK key with HTTP %d, giving up", refused.Code)
					setFatal(errors.New("requesting flags unauthorized"))
				} else {
					log.Printf("the LaunchDarkly stream answered HTTP %d, which a retry cannot fix; "+
						"check LD_STREAM_URI: %s", refused.Code, streamURI)
					setFatal(fmt.Errorf("the LaunchDarkly stream answered HTTP %d", refused.Code))
				}
				return es.StreamErrorHandlerResult{CloseNow: true}
			}

			log.Printf("the LaunchDarkly stream answered HTTP %d, will retry", refused.Code)
			return es.StreamErrorHandlerResult{}
		}

		log.Print("the LaunchDarkly stream connection failed: ", err, ", will retry")
		return es.StreamErrorHandlerResult{}
	}

	log.Print("connecting to the LaunchDarkly stream at: " + streamURI)

	stream, err := es.SubscribeWithRequestAndOptions(request,
		es.StreamOptionHTTPClient(bridge.streamClient),
		es.StreamOptionReadTimeout(bridge.streamReadTimeout),
		es.StreamOptionInitialRetry(bridge.streamInitialReconnectDelay),
		es.StreamOptionUseBackoff(STREAM_MAX_RETRY_DELAY),
		es.StreamOptionUseJitter(STREAM_JITTER_RATIO),
		es.StreamOptionRetryResetInterval(STREAM_RETRY_RESET_INTERVAL),
		es.StreamOptionErrorHandler(errorHandler),
		// A negative timeout retries the first connection for as long as it takes. A
		// bridge started during a LaunchDarkly outage then connects when the outage ends,
		// instead of exiting and leaving a supervisor to restart it into the same outage.
		es.StreamOptionCanRetryFirstConnection(-1),
		es.StreamOptionLogger(log.Default()),
	)
	if err != nil {
		// With the first connection retried indefinitely, only two things end the
		// attempt: the handler giving up, which set fatal, and the context ending, which
		// did not.
		bridge.cancel()
		return firstError(getFatal(), <-pushDone)
	}
	defer stream.Close()

	for {
		select {
		case event, ok := <-stream.Events:
			if !ok {
				// The library closes the channel once the stream is over, which it is
				// for one of two reasons: the handler gave up, or the context ended. A
				// third reason would be a defect, and is named rather than taken for a
				// clean stop, because featureLoop returning nil ends flag delivery
				// quietly.
				closed := getFatal()
				if closed == nil && bridge.context.Err() == nil {
					closed = errors.New("the LaunchDarkly stream closed unexpectedly")
				}
				bridge.cancel()
				return firstError(closed, <-pushDone)
			}

			bridge.handleStreamEvent(stream, event)

		case <-bridge.context.Done():
			stream.Close()
			// The stream's goroutine may be holding an event it was about to hand over,
			// and it closes the channel once it has stopped.
			for range stream.Events {
			}
			return <-pushDone
		}
	}
}

// firstError returns the first of its arguments that is not nil.
func firstError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

// handleStreamEvent applies one event to the store and wakes the push if it changed
// anything.
//
// A malformed event restarts the stream. The bridge cannot tell what the event would
// have changed, so it can no longer trust its copy of the data, and the connection that
// replaces this one opens with a put carrying everything. The other LaunchDarkly server
// SDKs do the same.
func (bridge *Bridge) handleStreamEvent(stream *es.Stream, event es.Event) {
	var err error

	switch event.Event() {
	case PUT_EVENT:
		err = bridge.applyPut([]byte(event.Data()))
	case PATCH_EVENT:
		err = bridge.applyPatch([]byte(event.Data()))
	case DELETE_EVENT:
		err = bridge.applyDelete([]byte(event.Data()))
	default:
		log.Printf("ignoring an unexpected %q event from the LaunchDarkly stream", event.Event())
		return
	}

	if err != nil {
		log.Printf("received a %q event with malformed data (%s), restarting the stream",
			event.Event(), err)
		stream.Restart()
	}
}

// applyPut replaces the store with the contents of a put event.
//
// The event looks like {"path":"/","data":{"flags":{...},"segments":{...}}}. The path is
// not checked: some versions of the Relay Proxy leave it out.
func (bridge *Bridge) applyPut(data []byte) error {
	var put struct {
		Data map[string]map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(data, &put); err != nil {
		return err
	}
	if put.Data == nil {
		return errors.New("put event has no data")
	}

	if err := bridge.flags.replace(put.Data); err != nil {
		return err
	}

	log.Printf("received %d flags and %d segments from the LaunchDarkly stream",
		bridge.flags.count("flags"), bridge.flags.count("segments"))
	bridge.notifyFlagsChanged()

	return nil
}

// applyPatch applies one changed item.
//
// The event looks like {"path":"/flags/key","data":{"key":"key","version":2,...}}.
func (bridge *Bridge) applyPatch(data []byte) error {
	var patch struct {
		Path string          `json:"path"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(data, &patch); err != nil {
		return err
	}
	if patch.Path == "" {
		return errors.New("patch event has no path")
	}
	if patch.Data == nil {
		return errors.New("patch event has no data")
	}

	kind, key, ok := parseStreamPath(patch.Path)
	if !ok {
		log.Printf("ignoring a patch to %q, which names no kind of item the bridge knows", patch.Path)
		return nil
	}

	version, err := versionOf(patch.Data)
	if err != nil {
		return err
	}

	if !bridge.flags.upsert(kind, key, flagItem{version: version, raw: patch.Data}) {
		log.Printf("ignoring a patch to %s %q at version %d, which is not newer than what is held",
			kind, key, version)
		return nil
	}

	log.Printf("the LaunchDarkly stream updated %s %q to version %d", kind, key, version)
	bridge.notifyFlagsChanged()

	return nil
}

// applyDelete records one deleted item.
//
// The event looks like {"path":"/flags/key","version":3}. The version is kept as a
// tombstone, so a patch to the same key that was already in flight when the item was
// deleted cannot bring it back.
func (bridge *Bridge) applyDelete(data []byte) error {
	var deletion struct {
		Path    string `json:"path"`
		Version *int   `json:"version"`
	}
	if err := json.Unmarshal(data, &deletion); err != nil {
		return err
	}
	if deletion.Path == "" {
		return errors.New("delete event has no path")
	}
	if deletion.Version == nil {
		return errors.New("delete event has no version")
	}

	kind, key, ok := parseStreamPath(deletion.Path)
	if !ok {
		log.Printf("ignoring a delete of %q, which names no kind of item the bridge knows", deletion.Path)
		return nil
	}

	if !bridge.flags.upsert(kind, key, flagItem{version: *deletion.Version}) {
		log.Printf("ignoring a delete of %s %q at version %d, which is not newer than what is held",
			kind, key, *deletion.Version)
		return nil
	}

	log.Printf("the LaunchDarkly stream deleted %s %q at version %d", kind, key, *deletion.Version)
	bridge.notifyFlagsChanged()

	return nil
}

// flagPushLoop delivers the store to the org whenever it changes, and keeps trying until
// it has.
//
// It coalesces. The push sends a snapshot of the whole store, so ten patches that arrive
// during one push cost one more push, not ten, and a change that lands while a failed
// push is waiting to be retried goes out in the retry instead of after it.
//
// It also declines to push what the org already has. Every reconnect opens with a put,
// and after a brief network interruption that put carries what the last push carried. A
// snapshot whose content matches the last successful push is dropped, so a flapping
// connection costs LaunchDarkly a reconnect and the org nothing. The comparison is by
// content rather than bytes; refer to canonicalJSON for why that distinction matters.
//
// It returns nil when the bridge shuts down. A failure no retry can fix -- Salesforce
// rejecting the credential on a refresh, or a push URI that cannot be built -- ends the
// daemon: the loop cancels the context and returns the error, and streamLoop reports it.
func (bridge *Bridge) flagPushLoop() error {
	pushURI := bridge.salesforceURL + "store"

	// lastPushed is the canonical form of what the org holds, and pending the snapshot
	// waiting to be pushed -- nil when there is nothing to deliver -- with pendingCanonical
	// its canonical form, which becomes lastPushed once it has been delivered.
	var lastPushed, pending, pendingCanonical []byte
	delay := bridge.flagPushRetryDelay

	// takeSnapshot reads the store and reports whether it holds anything the org does
	// not. An error here cannot happen -- every item was validated as JSON on its way
	// in -- and if it somehow does, the store is unusable.
	takeSnapshot := func() (bool, error) {
		snapshot, err := bridge.flags.snapshot()
		if err != nil {
			return false, err
		}
		canonical, err := canonicalJSON(snapshot)
		if err != nil {
			return false, err
		}

		if bytes.Equal(canonical, lastPushed) {
			log.Print("flag data is unchanged from the last push, not pushing")
			pending, pendingCanonical = nil, nil
			return false, nil
		}

		pending, pendingCanonical = snapshot, canonical
		return true, nil
	}

	for {
		if pending == nil {
			select {
			case <-bridge.context.Done():
				return nil
			case <-bridge.flagsChanged:
			}

			changed, err := takeSnapshot()
			if err != nil {
				bridge.cancel()
				return err
			}
			if !changed {
				continue
			}
		}

		err, permanent := bridge.pushFlags(pushURI, pending)
		if permanent {
			bridge.cancel()
			return err
		}
		if err == nil {
			// Said explicitly, because the only other evidence of a push succeeding is
			// the absence of a failure line after it.
			log.Printf("pushed %d flags and %d segments to Salesforce",
				bridge.flags.count("flags"), bridge.flags.count("segments"))
			lastPushed = pendingCanonical
			pending, pendingCanonical = nil, nil
			delay = bridge.flagPushRetryDelay
			continue
		}

		log.Print("failed pushing flags to salesforce: ", err, "; retrying in ", delay)

		select {
		case <-bridge.context.Done():
			return nil
		case <-bridge.flagsChanged:
			// Newer data supersedes the snapshot that was waiting. It goes out now
			// rather than after the delay, because what the org holds is already stale
			// and the delay was for the benefit of an org that may now be answering.
			if _, err := takeSnapshot(); err != nil {
				bridge.cancel()
				return err
			}
		case <-time.After(delay):
		}

		delay *= 2
		if delay > FLAG_PUSH_MAX_RETRY_DELAY {
			delay = FLAG_PUSH_MAX_RETRY_DELAY
		}
	}
}
