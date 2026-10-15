package main

import (
	"os"

	"github.com/jeon-jihyeon/gatereach/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:], os.Stdout, os.Stderr))
}
