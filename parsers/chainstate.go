package parsers

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"log"
	"github.com/btcsuite/btcd/chaincfg/chainhash"

	"github.com/forchain/bitcoinbigdata/lib"
	"encoding/hex"
	"github.com/piotrnar/gocoin/lib/btc"
	"sync"
	"sort"
	"runtime"
	"fmt"
	"os"
	"github.com/ruqqq/blockchainparser/db"
)

type tKV struct {
	key string
	val uint64
}

//type tBalanceList []tKV
//
//func (h tBalanceList) Len() int           { return len(h) }
//func (h tBalanceList) Less(i, j int) bool { return h[i].val > h[j].val }
//func (h tBalanceList) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

type ChainStateParser struct {
	db_           *leveldb.DB
	balanceMap_   tBalanceMap
	kvChan_       chan tKV
	obfuscateKey_ []byte
	balanceList_  lib.Uint64Sorted

	home_ string
	out_  string

	periodList_ [721]uint64
	bestHeight_ uint32
}

func (_c *ChainStateParser) processUTXO(_wg *sync.WaitGroup) {
	defer _wg.Done()

	for kv := range _c.kvChan_ {
		_c.balanceMap_[kv.key] += kv.val
	}

	log.Println("[UTXO]", len(_c.balanceMap_))

	_c.saveReport()
}

func (_c *ChainStateParser) saveReport() {
	wg := new(sync.WaitGroup)
	wg.Add(2)
	_c.saveHold(wg)
	_c.savePeriod(wg)
	wg.Wait()
}

func (_c *ChainStateParser) savePeriod(_wg *sync.WaitGroup) {
	defer _wg.Done()

	fileName := fmt.Sprintf("%v/period.csv", _c.out_)
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()

	for k, v := range _c.periodList_ {
		line := fmt.Sprintf("%v,%v\n", k, v)
		if _, err = f.WriteString(line); err != nil {
			log.Fatalln(err, line)
		} else {
			log.Print(line)
		}
	}

	log.Println("[PERIOD]", fileName)
}

func (_c *ChainStateParser) saveHold(_wg *sync.WaitGroup) {
	defer _wg.Done()

	balanceList := make(lib.Uint64Sorted, 0)
	for _, v := range _c.balanceMap_ {
		balanceList = append(balanceList, v)
	}
	sort.Sort(balanceList)
	_c.balanceList_ = balanceList

	divide := len(balanceList) / 1000
	n := 0
	sum := uint64(0)
	divideSum := uint64(0)
	sum1000, sum10000, sum100000 := uint64(0), uint64(0), uint64(0)
	divideKeys := make([]int, 0)
	divideVals := make([]uint64, 0)
	for k, v := range balanceList {
		n = k + 1
		sum += v
		divideSum += v

		if n == 1000 {
			sum1000 = sum
		} else if n == 10000 {
			sum10000 = sum
		} else if n == 100000 {
			sum100000 = sum
		}

		if n%divide == 0 {
			divideKeys = append(divideKeys, n)
			divideVals = append(divideVals, divideSum)
			divideSum = 0
		}
	}

	fileName := fmt.Sprintf("%v/hold.csv", _c.out_)
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}

	defer f.Close()

	line := fmt.Sprintf("1000,%v,%v\n", sum1000, float64(sum1000)/float64(sum))
	if _, err = f.WriteString(line); err != nil {
		log.Fatalln(err, line)
	} else {
		log.Print(line)
	}
	line = fmt.Sprintf("10000,%v,%v\n", sum10000, float64(sum10000)/float64(sum))
	if _, err = f.WriteString(line); err != nil {
		log.Fatalln(err, line)
	} else {
		log.Print(line)
	}
	line = fmt.Sprintf("100000,%v,%v\n", sum100000, float64(sum100000)/float64(sum))
	if _, err = f.WriteString(line); err != nil {
		log.Fatalln(err, line)
	} else {
		log.Print(line)
	}

	for k, key := range divideKeys {
		val := divideVals[k]
		line = fmt.Sprintf("%v,%v,%v\n", key, val, float64(val)/float64(sum))
		if _, err = f.WriteString(line); err != nil {
			log.Fatalln(err, line)
		} else {
			log.Print(line)
		}
	}

	log.Println("[HOLD]", fileName)
}

func (_c *ChainStateParser) parseUTXO(_key, _val []byte, _wg *sync.WaitGroup) {
	defer _wg.Done()

	if _key[0] != 'C' {
		return
	}
	h := new(chainhash.Hash)

	if err := h.SetBytes(_key[1:33]); err == nil {
		//varIndex := _key[33:]
		//index, _ := lib.DeserializeVLQ(varIndex)

		decrypted := lib.Xor(_val, _c.obfuscateKey_)

		code, offset1 := lib.DeserializeVLQ(decrypted)
		height := uint32(code >> 1)
		//codeBase := code & 1

		dist := _c.bestHeight_ - height
		if period := dist / 144; period < 720 {
			_c.periodList_[period] += 1
		} else {
			_c.periodList_[720] += 1
		}

		amount, offset2 := lib.DeserializeVLQ(decrypted[offset1:])
		amount = lib.DecompressTxOutAmount(amount)

		script := lib.DecompressScript(decrypted[offset1+offset2:], 0)
		addr := btc.NewAddrFromPkScript(script, false)

		addrKey := ""
		if addr == nil {
			addrKey = string(script)
		} else {
			addrKey = addr.String()
		}

		kv := tKV{addrKey, amount}
		_c.kvChan_ <- kv

	} else {
		log.Println(err)
	}
}

func (_c *ChainStateParser) Parse(_home string, _out string) {
	ldb, err := leveldb.OpenFile(_home+"/chainstate/", &opt.Options{ReadOnly: true})
	if err != nil {
		log.Fatal(err)
	}
	_c.db_ = ldb

	i := 0

	h, err := hex.DecodeString("0e00")
	if err != nil {
		log.Fatal(err)
	}
	h = append(h, "obfuscate_key"...)

	ro := new(opt.ReadOptions)
	obfuscateKey, err := ldb.Get(h, ro)
	if err != nil {
		log.Fatal(err)
	}

	_c.home_ = _home
	_c.out_ = _out
	cpuNum := runtime.NumCPU()
	_c.obfuscateKey_ = obfuscateKey[1:]
	_c.kvChan_ = make(chan tKV, cpuNum)
	_c.balanceMap_ = make(tBalanceMap)

	bestBlock, err := ldb.Get([]byte{'B'}, ro)
	if err != nil {
		log.Fatal(err)
	}
	decrypted := lib.Xor(bestBlock, _c.obfuscateKey_)
	blockHash := new(chainhash.Hash)
	blockHash.SetBytes(decrypted)

	indexDB, err := db.OpenIndexDb(_home)
	if err != nil {
		log.Fatalln(err)
	}
	bi, err := db.GetBlockIndexRecord(indexDB, blockHash[:])
	if err != nil {
		log.Fatalln(err)
	}
	_c.bestHeight_ = uint32(bi.Height)

	os.RemoveAll(_out)
	os.Mkdir(_out, os.ModePerm)

	log.Println("[START]", "cpuNum", cpuNum, "obfuscateKey_", _c.obfuscateKey_, "home_", _c.home_, "out_", _c.out_)

	wgProcess := new(sync.WaitGroup)
	wgProcess.Add(1)
	go _c.processUTXO(wgProcess)

	it := ldb.NewIterator(nil, ro)
	wgParse := new(sync.WaitGroup)
	for it.Next() {
		wgParse.Add(1)
		key := make([]byte, len(it.Key()))
		copy(key, it.Key())

		val := make([]byte, len(it.Value()))
		copy(val, it.Value())

		go _c.parseUTXO(key, val, wgParse)

		if i > 200000 {
			break
		}
		i++
	}
	wgParse.Wait()
	close(_c.kvChan_)
	wgProcess.Wait()
}
