package main

import (
	"github.com/forchain/bitcoinbigdata/parsers"
	"flag"
	"github.com/piotrnar/gocoin/lib/others/sys"
)

func main() {
	var help bool
	var home string
	var out string
	flag.StringVar(&home, "home", sys.BitcoinHome(), "Bitcoin data path")
	flag.StringVar(&out, "out", "/tmp", "Out path")
	flag.BoolVar(&help, "help", help, "Show help")
	flag.Parse()

	e := new(parsers.BalanceParser)
	e.Parse(0, home, out)
}
