package main

import (
	"flag"

	"github.com/rs/zerolog/log"
)

var src = flag.String("src", "", "Source database DSN")
var dst = flag.String("dst", "", "Destination database DSN")
var fraction = flag.Float64("f", 0.05, "Fraction of rows to copy")

func main() {
	flag.Parse()
	log.Info().Msg("Starting")

	if *src == "" || *dst == "" {
		log.Fatal().Msg("Source and destination DSNs are required")
	}

	if *fraction <= 0 || *fraction > 1 {
		log.Fatal().Msg("Fraction must be between 0 and 1")
	}

}
