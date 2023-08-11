package main

import (
	"flag"

	"github.com/rs/zerolog/log"
)

var src = flag.String("src", "", "Source DSN")
var dst = flag.String("dst", "", "Destination DSN")
var fraction = flag.Float64("f", 1.0, "Fraction of rows to copy")

func main() {
	flag.Parse()
	log.Info().Msg("Starting")

}
