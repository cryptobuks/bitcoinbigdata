package main

import (
	"github.com/Newtrong/bitcoinbigdata/parsers"
	"flag"
	"github.com/piotrnar/gocoin/lib/others/sys"
	"log"
	"strconv"
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

	if args[0] == "balance" {
		p := new(parsers.BalanceParser)
		p.Parse(0, home, out)
	} else if args[0] == "chain" {
		max := uint32(0)
		if len(args) > 1 {
			arg, err := strconv.Atoi(args[1])
			if  err != nil {
				log.Fatalln(err)
			}
			max = uint32(arg)
		}

		p := new(parsers.ChainStateParser)
		p.Parse(home, out, max)
	}
}
