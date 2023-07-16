package observability

import (
	"github.com/danthegoodman1/BigHouse/gologger"
	"log"
	"net/http"
	httppprof "net/http/pprof"

	"github.com/labstack/echo/v4"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/uber-go/tally/v4/prometheus"
)

var logger = gologger.NewLogger()

func StartInternalHTTPServer(address string, prom prometheus.Reporter) error {
	server := echo.New()
	server.HideBanner = true
	server.HidePort = true
	logger.Info().Str("address", address).Msg("Starting Internal API")
	server.GET("/metrics", echo.WrapHandler(prom.HTTPHandler()))
	server.Any("/debug/pprof/*", echo.WrapHandler(http.HandlerFunc(httppprof.Index)))
	server.Any("/debug/pprof/cmdline", echo.WrapHandler(http.HandlerFunc(httppprof.Cmdline)))
	server.Any("/debug/pprof/profile", echo.WrapHandler(http.HandlerFunc(httppprof.Profile)))
	server.Any("/debug/pprof/symbol", echo.WrapHandler(http.HandlerFunc(httppprof.Symbol)))
	server.Any("/debug/pprof/trace", echo.WrapHandler(http.HandlerFunc(httppprof.Trace)))
	return server.Start(address)
}

func NewPrometheusReporter() prometheus.Reporter {
	c := prometheus.Configuration{
		TimerType: "histogram",
	}
	reporter, err := c.NewReporter(
		prometheus.ConfigurationOptions{
			Registry: prom.DefaultRegisterer.(*prom.Registry),
			OnError: func(err error) {
				log.Println("error in prometheus reporter", err)
			},
		},
	)
	if err != nil {
		log.Fatalln("error creating prometheus reporter", err)
	}
	return reporter
}
