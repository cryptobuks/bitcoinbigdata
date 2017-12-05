package main

import (
	"github.com/forchain/bitcoinbigdata/parsers"
	"flag"
	"github.com/piotrnar/gocoin/lib/others/sys"
	"log"
)

func main() {
	var home string
	var out string
	flag.StringVar(&home, "home", sys.BitcoinHome(), "Bitcoin data path")
	flag.StringVar(&out, "out", "/tmp/out", "Out path")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		log.Println("-out OUT_PATH -home BITCOIN_HOME <balance|chain>")
		return
	}

	if args[0] == "balance"{
		p := new(parsers.BalanceParser)
		p.Parse(0, home, out)
	} else {
		p := new(parsers.ChainStateParser)
		p.Parse(home, out)
	}
}
