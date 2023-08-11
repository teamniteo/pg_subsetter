package main

import (
	"flag"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"niteo.co/subsetter/subsetter"
)

var src = flag.String("src", "", "Source database DSN")
var dst = flag.String("dst", "", "Destination database DSN")
var fraction = flag.Float64("f", 0.05, "Fraction of rows to copy")
var verbose = flag.Bool("verbose", true, "Show more information during sync")
var forceSync arrayForce

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack

	flag.Var(&forceSync, "force", "Query to copy required tables (users: id = 1)")
	flag.Parse()

	if *src == "" || *dst == "" {
		log.Fatal().Msg("Source and destination DSNs are required")
	}

	if *fraction <= 0 || *fraction > 1 {
		log.Fatal().Msg("Fraction must be between 0 and 1")
	}

	if len(forceSync) > 0 {
		log.Info().Str("forced", forceSync.String()).Msg("Forcing sync for tables")
	}

	s, err := subsetter.NewSync(*src, *dst, *fraction, forceSync, *verbose)
	if err != nil {
		log.Fatal().Stack().Err(err).Msg("Failed to configure sync")
	}

	defer s.Close()

	err = s.Sync()
	if err != nil {
		log.Fatal().Stack().Err(err).Msg("Failed to sync")
	}

}
