package main

import (
	"github.com/urfave/cli/v2"
)

var announceFlags = []cli.Flag{
	adminAPIFlag,
}

var announceHttpFlags = []cli.Flag{
	adminAPIFlag,
	indexerFlag,
}

var daemonFlags = []cli.Flag{
	carZeroLengthAsEOFFlag,
	&cli.StringFlag{
		Name:     "log-level",
		Usage:    "Set the log level",
		EnvVars:  []string{"GOLOG_LOG_LEVEL"},
		Value:    "info",
		Required: false,
	},
}

var initFlags = []cli.Flag{}

var connectFlags = []cli.Flag{
	&cli.StringFlag{
		Name:     "indexermaddr",
		Usage:    "Indexer multiaddr to connect",
		Aliases:  []string{"imaddr"},
		Required: true,
	},
	adminAPIFlag,
}

var indexerFlag = &cli.StringFlag{
	Name:     "indexer",
	Usage:    "Host or host:port of indexer to use",
	Aliases:  []string{"i"},
	Required: true,
}

var addrFlag = &cli.StringSliceFlag{
	Name:     "addr",
	Usage:    `Provider address as multiaddr string, example: "/ip4/127.0.0.1/tcp/3103"`,
	Aliases:  []string{"a"},
	Required: true,
}

var indexFlags = []cli.Flag{
	indexerFlag,
	addrFlag,
	&cli.StringFlag{
		Name:     "mh",
		Usage:    "Specify multihash to use as indexer key",
		Required: false,
	},
	&cli.StringFlag{
		Name:     "cid",
		Usage:    "Specify CID to use as indexer key",
		Required: false,
	},
	&cli.StringFlag{
		Name:     "ctxid",
		Usage:    "Context ID",
		Required: true,
	},
	metadataFlag,
}

var importCarFlags = []cli.Flag{
	adminAPIFlag,
	carPathFlag,
	metadataFlag,
	keyFlag,
}

var removeCarFlags = []cli.Flag{
	adminAPIFlag,
	optionalCarPathFlag,
	keyFlag,
}

var (
	metadataFlagValue string
	metadataFlag      = &cli.StringFlag{
		Name:        "metadata",
		Usage:       "Base64 encoded metadata bytes.",
		Aliases:     []string{"m"},
		Required:    false,
		Destination: &metadataFlagValue,
	}
)

var (
	keyFlagValue string
	keyFlag      = &cli.StringFlag{
		Name:        "key",
		Usage:       "Base64 encoded lookup key to associate with imported CAR.",
		Aliases:     []string{"k"},
		Required:    false,
		Destination: &keyFlagValue,
	}
)

var (
	carPathFlagValue string
	carPathFlag      = &cli.StringFlag{
		Name:        "input",
		Aliases:     []string{"i"},
		Usage:       "Path to the CAR file to import",
		Destination: &carPathFlagValue,
		Required:    true,
	}
)

var (
	optionalCarPathFlagValue string
	optionalCarPathFlag      = &cli.StringFlag{
		Name:        "input",
		Aliases:     []string{"i"},
		Usage:       "The CAR file path.",
		Destination: &optionalCarPathFlagValue,
	}
)

var (
	adminAPIFlagValue string
	adminAPIFlag      = &cli.StringFlag{
		Name:        "listen-admin",
		Usage:       "Admin HTTP API listen address",
		Aliases:     []string{"l"},
		EnvVars:     []string{"PROVIDER_LISTEN_ADMIN"},
		Value:       "http://localhost:3102",
		Destination: &adminAPIFlagValue,
	}
)

var (
	carZeroLengthAsEOFFlagValue bool
	carZeroLengthAsEOFFlag      = &cli.BoolFlag{
		Name:        "carZeroLengthAsEOF",
		Aliases:     []string{"cz"},
		Usage:       "Specifies whether zero-length blocks in CAR should be consideted as EOF.",
		Value:       false, // Default to disabled, consistent with go-car/v2 defaults.
		Destination: &carZeroLengthAsEOFFlagValue,
	}
)
