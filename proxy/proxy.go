package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"regexp"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/promlog/flag"

)

var (
	listenAddress = kingpin.Flag("web.listen-address", "Address to listen on for proxy and client requests.").Default(":8080").String()
) 

func copyHTTPResponse(resp *http.Response, w http.ResponseWriter) {
	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

type targetGroup struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

func main() {
	allowedLevel := promlog.AllowedLevel{}
	flag.AddFlags(kingpin.CommandLine, &allowedLevel)
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()
	logger := promlog.New(allowedLevel)
	coordinator := NewCoordinator(logger)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Proxy request
		if r.URL.Host != "" {
			timeout := GetScrapeTimeout(r.Header)
			level.Debug(logger).Log("msg", "Scraping", "timeout",  timeout)
			ctx, _ := context.WithTimeout(r.Context(), timeout)
			request := r.WithContext(ctx)
			request.RequestURI = ""

			resp, err, disconnect := coordinator.DoScrape(ctx, request, w)
			if disconnect {
				level.Error(logger).Log("msg", "Scraping: Disconnected")
				return
			}
			if err != nil {
				level.Error(logger).Log("msg", "Error scraping:", "err", err, "url", request.URL.String())
				http.Error(w, fmt.Sprintf("Error scraping %q: %s", request.URL.String(), err.Error()), 500)
				return
			}
			defer resp.Body.Close()
			level.Debug(logger).Log("msg", "Scraping: Sending scrap response")
			copyHTTPResponse(resp, w)
			return
		}

		// Client registering and asking for scrapes.
		if r.URL.Path == "/poll" {
			fqdn, _ := ioutil.ReadAll(r.Body)
			r, _ := regexp.Compile(":.*$")
			// the key is the FQDN and the port
			key := strings.TrimSpace(string(fqdn))
			if !r.MatchString(key) {
				// assume port 80 if none specified in teh key.
				key = key + ":80"
			}
			request, doscrape := coordinator.WaitForScrapeInstruction(w, key)
			if doscrape {
				request.WriteProxy(w) // Send full request as the body of the response.
				level.Debug(logger).Log("msg", "Responded to /poll", "url", request.URL.String(), "scrape_id", request.Header.Get("Id"))
			} else {
				level.Info(logger).Log("msg", "Connection was closed by client ")

			}
			return
		}

		// Scrape response from client.
		if r.URL.Path == "/push" {
			buf := &bytes.Buffer{}
			io.Copy(buf, r.Body)

			scrapeResult, _ := http.ReadResponse(bufio.NewReader(buf), nil)
			level.Info(logger).Log("msg", "Got /push", "scrape_id", scrapeResult.Header.Get("Id"))
			err := coordinator.ScrapeResult(scrapeResult)
			if err != nil {
				level.Error(logger).Log("msg", "Error pushing:", "err", err, "scrape_id", scrapeResult.Header.Get("Id"))
				http.Error(w, fmt.Sprintf("Error pushing: %s", err.Error()), 500)
			}
			return
		}

		if r.URL.Path == "/clients" {
			known := coordinator.KnownClients()
			targets := make([]*targetGroup, 0, len(known))
			for _, k := range known {
				targets = append(targets, &targetGroup{Targets: []string{k}})
			}
			json.NewEncoder(w).Encode(targets)
			level.Info(logger).Log("msg", "Responded to /clients", "client_count", len(known))
			return
		}

		http.Error(w, "404: Unknown path", 404)
	})

	level.Info(logger).Log("msg", "Listening", "address", *listenAddress)
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
