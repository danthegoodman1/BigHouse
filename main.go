package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/danthegoodman1/BigHouse/observability"
	"github.com/danthegoodman1/BigHouse/temporal"
	"github.com/joho/godotenv"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/danthegoodman1/BigHouse/crdb"
	"github.com/danthegoodman1/BigHouse/gologger"
	"github.com/danthegoodman1/BigHouse/http_server"
	"github.com/danthegoodman1/BigHouse/migrations"
	"github.com/danthegoodman1/BigHouse/utils"
)

var logger = gologger.NewLogger()

func main() {
	if _, err := os.Stat(".env"); err == nil {
		err = godotenv.Load()
		if err != nil {
			logger.Error().Err(err).Msg("error loading .env file, exiting")
			os.Exit(1)
		}
	}
	logger.Debug().Msg("starting Tangia mono api")

	if err := crdb.ConnectToDB(); err != nil {
		logger.Error().Err(err).Msg("error connecting to CRDB")
		os.Exit(1)
	}

	err := migrations.CheckMigrations(utils.CRDB_DSN)
	if err != nil {
		logger.Error().Err(err).Msg("Error checking migrations")
		os.Exit(1)
	}

	prometheusReporter := observability.NewPrometheusReporter()
	err = observability.StartInternalHTTPServer(":8042", prometheusReporter)
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error().Err(err).Msg("internal server couldn't start")
		os.Exit(1)
	}

	err = temporal.Run(context.Background(), prometheusReporter)
	if err != nil {
		logger.Error().Err(err).Msg("Temporal init error")
		os.Exit(1)
	}

	httpServer := http_server.StartHTTPServer()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	logger.Warn().Msg("received shutdown signal!")

	// For AWS ALB needing some time to de-register pod
	// Convert the time to seconds
	sleepTime := utils.GetEnvOrDefaultInt("SHUTDOWN_SLEEP_SEC", 0)
	logger.Info().Msg(fmt.Sprintf("sleeping for %ds before exiting", sleepTime))

	time.Sleep(time.Second * time.Duration(sleepTime))
	logger.Info().Msg(fmt.Sprintf("slept for %ds, exiting", sleepTime))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to shutdown HTTP server")
	} else {
		logger.Info().Msg("successfully shutdown HTTP server")
	}
}
