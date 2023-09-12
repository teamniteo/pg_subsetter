package main

import (
	"flag"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"niteo.co/subsetter/subsetter"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

var src = flag.String("src", "", "Source database DSN")
var dst = flag.String("dst", "", "Destination database DSN")
var fraction = flag.Float64("f", 0.05, "Fraction of rows to copy")
var verbose = flag.Bool("verbose", false, "Show more information during sync")
var ver = flag.Bool("v", false, "Release information")
var extraInclude arrayExtra
var extraExclude arrayExtra

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack

	flag.Var(&extraInclude, "include", "Query to copy required rows 'users: id = 1', can be used multiple times")
	flag.Var(&extraExclude, "exclude", "Query to ignore tables 'users: all', can be used multiple times")
	flag.Parse()

	if *ver {
		log.Info().Str("version", version).Str("commit", commit).Str("date", date).Msg("Version")
		os.Exit(0)
	}

	if *src == "" || *dst == "" {
		log.Fatal().Msg("Source and destination DSNs are required")
	}

	if *fraction <= 0 || *fraction > 1 {
		log.Fatal().Msg("Fraction must be between 0 and 1")
	}

	if *verbose {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	if len(extraInclude) > 0 {
		log.Info().Str("include", extraInclude.String()).Msg("Forcibly")
	}
	if len(extraExclude) > 0 {
		log.Info().Str("exclude", extraExclude.String()).Msg("Forcibly")
	}

	s, err := subsetter.NewSync(*src, *dst, *fraction, extraInclude, extraExclude, *verbose)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to configure sync")
	}

	defer s.Close()

	err = s.Sync()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to sync")
	}

}
