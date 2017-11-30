package parsers

import (
	"encoding/hex"
	"log"

	"github.com/piotrnar/gocoin/lib/others/blockdb"
	"github.com/piotrnar/gocoin/lib/btc"
)

func main() {
	// Set real Bitcoin network
	Magic := [4]byte{0xF9, 0xBE, 0xB4, 0xD9}

	// Specify blocks directory
	BlockDatabase := blockdb.NewBlockDB("/Users/Tony/.bitcoin/blocks", Magic)

	start_block := 0
	end_block := 30000

	for i := 0; i < end_block+1; i++ {

		dat, er := BlockDatabase.FetchNextBlock()
		if dat == nil || er != nil {
			log.Println("END of DB file")
			break
		}
		bl, er := btc.NewBlock(dat[:])

		if er != nil {
			println("Block inconsistent:", er.Error())
			break
		}

		// Read block till we reach start_block
		if i < start_block {
			continue
		}

		log.Printf("Current block height: %v", i)

		// Basic block info
		log.Printf("Block hash: %v", bl.Hash)
		log.Printf("Block time: %v", bl.BlockTime)
		log.Printf("Block version: %v", bl.Version)
		log.Printf("Block parent: %v", btc.NewUint256(bl.ParentHash()).String())
		log.Printf("Block merkle root: %v", hex.EncodeToString(bl.MerkleRoot()))
		log.Printf("Block bits: %v", bl.Bits)
		log.Printf("Block size: %v", len(bl.Raw))

		// Fetch TXs and iterate over them
		bl.BuildTxList()
		for _, tx := range bl.Txs {
			log.Printf("TxId: %v", tx.Hash.String())
			log.Printf("Tx Size: %v", tx.Size)
			log.Printf("Tx Lock time: %v", tx.Lock_time)
			log.Printf("Tx Version: %v", tx.Version)

			log.Println("TxIns:")

			if tx.IsCoinBase() {
				log.Printf("TxIn coinbase, newly generated coins")
			} else {
				for txin_index, txin := range tx.TxIn {
					log.Printf("TxIn index: %v", txin_index)
					log.Printf("TxIn Input: %v", txin.Input.String())
					log.Printf("TxIn ScriptSig: %v", hex.EncodeToString(txin.ScriptSig))
					log.Printf("TxIn Sequence: %v", txin.Sequence)
				}
			}

			log.Println("TxOuts:")

			for txo_index, txout := range tx.TxOut {
				log.Printf("TxOut index: %v", txo_index)
				log.Printf("TxOut value: %v", txout.Value)
				log.Printf("TxOut script: %s", hex.EncodeToString(txout.Pk_script))
				txout_addr := btc.NewAddrFromPkScript(txout.Pk_script, false)
				if txout_addr != nil {
					log.Printf("TxOut address: %v", txout_addr.String())
				} else {
					log.Printf("TxOut address: can't decode address")
				}
			}
		}
		log.Println()
	}
}
