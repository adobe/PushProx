package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

var (
	registrationTimeout = kingpin.Flag("registration.timeout", "After how long a registration expires.").Default("5m").Duration()
)

type Coordinator struct {
	mu sync.Mutex

	// Clients waiting for a scrape.
	waiting map[string]chan *http.Request
	// Responses from clients.
	responses map[string]chan *http.Response
	// Clients we know about and when they last contacted us.
	known map[string]time.Time

	logger log.Logger
}

func NewCoordinator(logger log.Logger) *Coordinator {
	c := &Coordinator{
		waiting:   map[string]chan *http.Request{},
		responses: map[string]chan *http.Response{},
		known:     map[string]time.Time{},
		logger:    logger,
	}
	go c.gc()
	return c
}

var idCounter int64

// Generate a unique ID
func genId() string {
	id := atomic.AddInt64(&idCounter, 1)
	// TODO: Add MAC address.
	// TODO: Sign these to prevent spoofing.
	return fmt.Sprintf("%d-%d-%d", time.Now().Unix(), id, os.Getpid())
}

func (c *Coordinator) getRequestChannel(fqdn string) chan *http.Request {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch, ok := c.waiting[fqdn]
	if !ok {
		ch = make(chan *http.Request)
		c.waiting[fqdn] = ch
	}
	return ch
}

// Remove a request channel.
func (c *Coordinator) removeRequestChannel(fqdn string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.waiting, fqdn)
}


func (c *Coordinator) getResponseChannel(id string) chan *http.Response {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch, ok := c.responses[id]
	if !ok {
		ch = make(chan *http.Response)
		c.responses[id] = ch
	}
	return ch
}

// Remove a response channel. Idempotent.
func (c *Coordinator) removeResponseChannel(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.responses, id)
}

// Request a scrape.
// needs context, the request and the writer
// returns the response from the scrape or nil, an error or nil, and true if the client disconnected.
func (c *Coordinator) DoScrape(ctx context.Context, r *http.Request, w http.ResponseWriter) (*http.Response, error, bool) {
	id := genId()
	level.Info(c.logger).Log("msg", "DoScrape", "scrape_id", id, "url", r.URL.String())
	r.Header.Add("Id", id)
	// send the request out to the client to request a scape, by getting the request channel
	// and sending it.
	// if the client is not connected, then this will block until it is connected.
	// the server doing the scrape could disconnect before the requestChannel becomes available
	// that would leave the sockets in an ugly state and should be handled
	// the key is the FQDN and the port, 
	notify := w.(http.CloseNotifier).CloseNotify()
	select {
	case <-notify:
		level.Info(c.logger).Log("msg", "DoScrape", "client closed, scrape id", id )
		return nil, nil, true
	case <-ctx.Done():
		return nil, fmt.Errorf("Matching client not found for %q: %s", r.URL.String(), ctx.Err()), false
	case c.getRequestChannel(r.URL.Hostname()+":"+r.URL.Port()) <- r:
	}

	// grab the response channel and wait for the client to push the data.
	respCh := c.getResponseChannel(id)
	defer c.removeResponseChannel(id)

	// the server requesting the scrape could disconnect here so must handle that
	// while waiting for data to come in on the response channel.
	select {
	case <-notify:
		level.Info(c.logger).Log("msg", "DoScrape", "client closed, scrape id", id )
		return nil, nil, true
	case <-ctx.Done():
		level.Debug(c.logger).Log("msg", "DoScrape", "Done timeout", id )
		return nil, ctx.Err(), false
	case resp := <-respCh:
		level.Debug(c.logger).Log("msg", "DoScrape", "Response Ok", id )
		return resp, nil, false
	}
}

// Client registering to accept a scrape request. Blocking.
func (c *Coordinator) WaitForScrapeInstruction(w http.ResponseWriter, fqdn string) (*http.Request, bool) {

	c.addKnownClient(fqdn)
	notify := w.(http.CloseNotifier).CloseNotify()
	ch := c.getRequestChannel(fqdn)
	// always remove the request channel when scape is done even if the client is gone.
	defer c.removeRequestChannel(fqdn)
	for {
		select {
		case <-notify:
			level.Info(c.logger).Log("msg", "WaitForScrapeInstruction", "client closed", fqdn)

			return nil, false
		case request := <-ch:
			for {
				select {
					case <-notify:
						level.Info(c.logger).Log("msg", "WaitForScrapeInstruction", "client closed while processing scrape (rare)", fqdn)
					case <-request.Context().Done():
						level.Info(c.logger).Log("msg", "WaitForScrapeInstruction", "Timeout waiting for scape ", fqdn)
					// Request has timed out, get another one.
					default:
						level.Debug(c.logger).Log("msg", "WaitForScrapeInstruction", "Ok waiting for scrape ", fqdn)
						return request, true
				}
			}
		}
	}
}

// Client sending a scrape result in.
// this is super confusing.
// the Response is the response is a pre-prepared response generated 
// from the body of the request that came in from the client
// that body contains all the headers of the response in the body.
// When a response channel is available, the preformed response is sent 
// directly to the channel which returns to the 
func (c *Coordinator) ScrapeResult(r *http.Response) error {
	id := r.Header.Get("Id")
	level.Info(c.logger).Log("msg", "ScrapeResult", "scrape_id", id)
	ctx, _ := context.WithTimeout(context.Background(), GetScrapeTimeout(r.Header))
	// Don't expose internal headers.
	r.Header.Del("Id")
	r.Header.Del("X-Prometheus-Scrape-Timeout-Seconds")
	// When the response channel becomes available, (should already be available, if the prom server didnt disconnect)
	// then send the result to the response channel.
	// it doesnt matter if the client performing the request disconnects
	// If the prom server did disconnect, then this will block until the next time the prom server comes in
	// but since it can not set a timeout > poll, that can never happen. (lol never always happens distributed)
	// even so, this self heals if the server disconnects.
	// if the client disconnects, we dont care, the response is already captured.
	select {
	case c.getResponseChannel(id) <- r:
		level.Debug(c.logger).Log("msg", "ScrapeResult", "Sent to response channel ", id)
		return nil
	case <-ctx.Done():
		// timeout, remove theResponse channel since the request that asked
		// for the scrape wont remove it.
		level.Debug(c.logger).Log("msg", "ScrapeResult", "Timeout waiting for response channel ", id)
		c.removeResponseChannel(id)
		return ctx.Err()
	}
}

func (c *Coordinator) addKnownClient(fqdn string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.known[fqdn] = time.Now()
}

// What clients are alive.
func (c *Coordinator) KnownClients() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	limit := time.Now().Add(-*registrationTimeout)
	known := make([]string, 0, len(c.known))
	for k, t := range c.known {
		if limit.Before(t) {
			known = append(known, k)
		}
	}
	return known
}

// Garbagee collect old clients.
func (c *Coordinator) gc() {
	for range time.Tick(1 * time.Minute) {
		func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			limit := time.Now().Add(-*registrationTimeout)
			deleted := 0
			for k, ts := range c.known {
				if ts.Before(limit) {
					delete(c.known, k)
					deleted++
				}
			}
			level.Info(c.logger).Log("msg", "GC of clients completed", "deleted", deleted, "remaining", len(c.known))
		}()
	}
}
