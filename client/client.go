// 
// Description
//
package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/ShowMax/go-fqdn"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/promlog/flag"
)

var (
	myFqdn   = kingpin.Flag("fqdn", "FQDN to register with, typically best to use the default").Default(fqdn.Get()).String()
	pullURL  = kingpin.Flag("pull-url", "Pull URL to use").Required().String()
	proxyURL = kingpin.Flag("proxy-url", "Push proxy to talk to.").Required().String()
	promToken = os.Getenv("PROM_TOKEN")
)

type Coordinator struct {
	logger log.Logger
}

func (c *Coordinator) doScrape(request *http.Request, client *http.Client) {
	logger := log.With(c.logger, "scrape_id", request.Header.Get("id"))
	ctx, _ := context.WithTimeout(request.Context(), GetScrapeTimeout(request.Header))
	request = request.WithContext(ctx)

	// We cannot handle http requests at the proxy, as we would only
	// see a CONNECT, so use a URL parameter to trigger it.
	params := request.URL.Query()

	// override the url from the server adn use the configured url.\
	// this has beem checked already.
	pullU, err := url.Parse(*pullURL)
	request.URL = pullU;
	request.URL.RawQuery = params.Encode()
	request.Header.Set("x-prom-pull-token", promToken)

	scrapeResp, err := client.Do(request)
	if err != nil {
		msg := fmt.Sprintf("Failed to scrape %s: %s", request.URL.String(), err)
		level.Warn(logger).Log("msg", msg)
		resp := &http.Response{
			StatusCode: 500,
			Header:     http.Header{},
			Body:       ioutil.NopCloser(strings.NewReader(msg)),
		}
		err = c.doPush(resp, request, client)
		if err != nil {
			msg2 := fmt.Sprintf("Failed to push failed scrape response from %s: %s", request.URL.String(), err)
			level.Warn(logger).Log("msg", msg2)
			return
		}
		return
	}
	err = c.doPush(scrapeResp, request, client)
	if err != nil {
		msg2 := fmt.Sprintf("Failed to push failed scrape response from %s: %s", request.URL.String(), err)
		level.Warn(logger).Log("msg", msg2)
		return
	}
}

// Report the result of the scrape back up to the proxy.
func (c *Coordinator) doPush(resp *http.Response, origRequest *http.Request, client *http.Client) error {
	resp.Header.Set("id", origRequest.Header.Get("id")) // Link the request and response
	// Remaining scrape deadline.
	deadline, _ := origRequest.Context().Deadline()
	resp.Header.Set("X-Prometheus-Scrape-Timeout", fmt.Sprintf("%f", float64(time.Until(deadline))/1e9))

	base, err := url.Parse(*proxyURL)
	if err != nil {
		return err
	}
	u, err := url.Parse("/push")
	if err != nil {
		return err
	}
	url := base.ResolveReference(u)

	buf := &bytes.Buffer{}
	resp.Write(buf)
	request := &http.Request{
		Method:        "POST",
		URL:           url,
		Body:          ioutil.NopCloser(buf),
		ContentLength: int64(buf.Len()),
	}
	request = request.WithContext(origRequest.Context())
	_, err = client.Do(request)
	if err != nil {
		return err
	}
	return nil
}

func loop(c Coordinator) {
	client := &http.Client{}
	base, err := url.Parse(*proxyURL)
	if err != nil {
		level.Error(c.logger).Log("msg", "Error parsing url:", "err", err)
		return
	}
	u, err := url.Parse("/poll")
	if err != nil {
		level.Error(c.logger).Log("msg", "Error parsing url:", "err", err)
		return
	}
	url := base.ResolveReference(u)
	resp, err := client.Post(url.String(), "", strings.NewReader(*myFqdn))
	if err != nil {
		level.Error(c.logger).Log("msg", "Error polling:", "err", err)
		time.Sleep(time.Second) // Don't pound the server. TODO: Randomised exponential backoff.
		return
	}
	defer resp.Body.Close()
	request, err := http.ReadRequest(bufio.NewReader(resp.Body))
	if err != nil {
		level.Error(c.logger).Log("msg", "Error reading request:", "err", err)
		return
	}
	level.Info(c.logger).Log("msg", "Got scrape request", "scrape_id", request.Header.Get("id"), "url", request.URL)

	request.RequestURI = ""

	request.Host = ""

	go c.doScrape(request, client)
}

func main() {
    kingpin.CommandLine.Help = "Prometheus PushProx client. \n\n"+
    	"Will register itself using the FQDN with the PushProx proxy /poll end point \n"+
    	"When Prometheus pulls it calls PushProx /poll end point which causes clients to \n"+
    	"return and scrape their pull-url end points, writing the respionse, which are \n"+
    	"writren into the Prometheus /poll reponse. \n"+
    	"--pull-url must be set to a URL pull end point where the application is running, typically http://localhost:4502/metrics\n"+
    	"--proy-url must be set to the base URL of the proxy"
	allowedLevel := promlog.AllowedLevel{}
	allowedLevel.Set("info")
	flag.AddFlags(kingpin.CommandLine, &allowedLevel)
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()
	logger := promlog.New(allowedLevel)
	coordinator := Coordinator{logger: logger}
	if *proxyURL == "" {
		level.Error(coordinator.logger).Log("msg", "--proxy-url flag must be specified.")
		os.Exit(1)
	}
	if *pullURL == "" {
		level.Error(coordinator.logger).Log("msg", "--pull-url flag must be specified.")
		os.Exit(1)
	}
	_, err := url.Parse(*pullURL)
	if err != nil {
		level.Warn(logger).Log("msg", "--pull-url not a valid url valid ", *pullURL, "err", err)
		os.Exit(1)
	}
	msg := fmt.Sprintf("URL and FQDN info proxy_url %s Using FQDN of %s  and Pull URL %s ", *proxyURL, *myFqdn, *pullURL )
	level.Info(coordinator.logger).Log("msg", msg)
	for {
		loop(coordinator)
	}
}
