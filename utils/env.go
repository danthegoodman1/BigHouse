package utils

import "os"

var (
	Env = os.Getenv("ENV")

	CRDB_DSN = os.Getenv("CRDB_DSN")

	FLY_API_TOKEN = MustEnv("FLY_API_TOKEN")
	FLY_APP       = MustEnv("FLY_APP")

	TEMPORAL_URL = MustEnv("TEMPORAL_URL")
)
