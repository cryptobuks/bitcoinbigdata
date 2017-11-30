package main

import "github.com/forchain/bitcoinbigdata/parsers"

func main() {
	e := new(parsers.BalanceParser)
	e.Parse(0, "/Users/Tony/.bitcoin", "/Users/Tony/out")
}
