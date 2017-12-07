package parsers

import (
	"github.com/piotrnar/gocoin/lib/btc"
	"bytes"
	"fmt"
	"io/ioutil"
	"sync"
	"os"
	"log"
	"compress/gzip"
	"strconv"
	"strings"
	"sort"
	"runtime"
	"github.com/piotrnar/gocoin/lib/others/blockdb"
	"time"
	"github.com/forchain/bitcoinbigdata/lib"
)

const (
	HALVING_BLOCKS = 210000
	MAX_REWARD     = 50.0 * 1e8
)

type tAccount struct {
	balance uint64
	time    uint32
}

type tAccountMap map[string]tAccount

type tOutput struct {
	addr string // index
	val  uint64 // val
}

//  (index -> output)
type tOutputMap map[uint16]tOutput

// tx -> tOutputMap
type tUnspentMap map[btc.Uint256]tOutputMap

type tPrev2Spent struct {
	final bool
	prev  string
	last  string

	file     uint32
	blockNum uint32

	unspentMap tUnspentMap
	balanceMap tBalanceMap
	spentList  []string
}

type tChangeSet struct {
	sumOut     uint64
	spentMap   map[btc.Uint256][]uint16
	balanceMap map[btc.Uint256]tOutputMap
	block      *btc.Block
}

type tBalanceChange struct {
	addr   string
	change int64
	time   time.Time
}

type BalanceParser struct {
	endBlock_ uint32
	fileNO_   int
	outDir_   string

	unspentMap_ tUnspentMap
	accountMap_ tAccountMap

	reduceNum_ uint32
	reduceSum_ uint64

	reduceNum2010_ uint32
	reduceSum2010_ uint64

	reduceNum2014_ uint32
	reduceSum2014_ uint64

	unspentMapLock_ *sync.RWMutex
	balanceMapLock_ *sync.RWMutex

	changeSetCh_     chan *tChangeSet
	prevMap_         map[btc.Uint256]*tChangeSet
	balanceChangeCh_ chan *tBalanceChange
	balanceReadyCh_  chan bool

	fileList_ []int
	blockNum_ uint32

	blockNO_     uint32
	sumReward_   uint64
	sumFee_      uint64
	halvingRate_ float64
}

func (_b *BalanceParser) loadBlock(dat []byte, _wg *sync.WaitGroup) {
	defer _wg.Done()

	bl, er := btc.NewBlock(dat[:])

	if er != nil {
		log.Fatalln("Block inconsistent:", er.Error())
	}

	bl.BuildTxList()

	sumOut := uint64(0)

	unspentMap := make(map[btc.Uint256][]uint16)
	balanceMap := make(map[btc.Uint256]tOutputMap)
	for _, t := range bl.Txs {
		txID := *t.Hash
		if t.IsCoinBase() {
			for _, v := range t.TxOut {
				sumOut += v.Value
			}
		} else {
			for _, i := range t.TxIn {
				hash := *btc.NewUint256(i.Input.Hash[:])
				index := uint16(i.Input.Vout)
				unspentMap[hash] = append(unspentMap[hash], index)
			}
		}

		outputMap := make(tOutputMap)
		for i, o := range t.TxOut {
			if o.Value == 0 {
				continue
			}
			addr := ""
			a := btc.NewAddrFromPkScript(o.Pk_script, false)
			if a == nil {
				addr = string(o.Pk_script)
			} else {
				addr = a.String()
			}
			val := uint64(o.Value)
			outputMap[uint16(i)] = tOutput{addr, val}
		}
		balanceMap[txID] = outputMap
	}

	_b.changeSetCh_ <- &tChangeSet{sumOut, unspentMap, balanceMap, bl}
}

func (_b *BalanceParser) Parse(_blockNO uint32, _dataDir string, _outDir string) {
	cpuNum := runtime.NumCPU()
	magicID := [4]byte{0xF9, 0xBE, 0xB4, 0xD9}

	_b.endBlock_ = _blockNO

	_b.outDir_ = _outDir
	_b.fileList_ = make([]int, 0)
	_b.fileNO_ = -1

	_b.prevMap_ = make(map[btc.Uint256]*tChangeSet)

	_b.blockNum_ = uint32(0)

	_b.changeSetCh_ = make(chan *tChangeSet)
	_b.balanceChangeCh_ = make(chan *tBalanceChange, cpuNum)
	_b.balanceReadyCh_ = make(chan bool)

	_b.unspentMap_ = make(tUnspentMap)
	_b.unspentMapLock_ = new(sync.RWMutex)
	// address -> balance
	_b.accountMap_ = make(tAccountMap)
	_b.balanceMapLock_ = new(sync.RWMutex)

	_b.blockNO_ = uint32(0)
	_b.sumReward_ = uint64(0)
	_b.sumFee_ = uint64(0)
	_b.halvingRate_ = 1.0

	// Specify blocks directory
	blockDatabase := blockdb.NewBlockDB(_dataDir+"/blocks", magicID)

	endBlock := 100 * 10000

	waitProcess := new(sync.WaitGroup)
	waitProcess.Add(1)
	go _b.processBlock(waitProcess)

	go _b.processBalance(nil)

	os.RemoveAll(_outDir)
	os.Mkdir(_outDir, os.ModePerm)

	waitLoad := new(sync.WaitGroup)
	for i := 0; i < endBlock; i++ {

		dat, er := blockDatabase.FetchNextBlock()
		if dat == nil || er != nil {
			log.Println("END of DB file")
			break
		}
		waitLoad.Add(1)
		go _b.loadBlock(dat, waitLoad)

		if i%cpuNum == 0 {
			waitLoad.Wait()
		}
	}

	waitLoad.Wait()
	close(_b.changeSetCh_)
	close(_b.balanceReadyCh_)
	waitProcess.Wait()

	log.Print("account number:", len(_b.accountMap_))
	log.Print("unspent number:", len(_b.unspentMap_))
}

func (_b *BalanceParser) saveUnspent(_wg *sync.WaitGroup, _path string) {
	defer _wg.Done()
	fileName := fmt.Sprintf("%v/unspent.gz", _path)

	b := new(bytes.Buffer)
	w, err := gzip.NewWriterLevel(b, gzip.BestSpeed)
	if err != nil {
		log.Fatal(err)
	}

	bb := new(bytes.Buffer)
	for tx, outputs := range _b.unspentMap_ {
		bb.WriteString(tx.String())
		for i, o := range outputs {
			l := fmt.Sprintf(",%v %v %v", i, o.addr, o.val)
			bb.WriteString(l)
		}
		bb.WriteByte('\n')
		w.Write([]byte(bb.Bytes()))
		bb.Reset()
	}

	w.Close()
	if err := ioutil.WriteFile(fileName, b.Bytes(), 0666); err != nil {
		log.Fatal(err)
	}
	log.Println("saved", fileName)
}

type tSortedBalance []string

func (s tSortedBalance) Len() int {
	return len(s)
}
func (s tSortedBalance) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s tSortedBalance) Less(i, j int) bool {
	// remove trailing return
	t1 := strings.Split(s[i][:len(s[i])-1], " ")
	t2 := strings.Split(s[j][:len(s[j])-1], " ")
	if len(t1) == 2 && len(t2) == 2 {
		if v1, err := strconv.ParseUint(t1[1], 10, 0); err == nil {
			if v2, err := strconv.ParseUint(t2[1], 10, 0); err == nil {
				return v1 > v2
			}
		}
	}

	return len(s[i]) < len(s[j])
}

func (_b *BalanceParser) saveBalance(_wg *sync.WaitGroup, _path string) {
	defer _wg.Done()

	fileName := fmt.Sprintf("%v/balance.gz", _path)

	b := new(bytes.Buffer)
	w, err := gzip.NewWriterLevel(b, gzip.BestSpeed)
	if err != nil {
		log.Fatal(err)
	}

	// if OOM, try delete map item then append to list
	sorted := make(tSortedBalance, 0)

	for k, v := range _b.accountMap_ {
		line := fmt.Sprintln(k, v.balance)
		sorted = append(sorted, line)
	}
	sort.Sort(sorted)
	for _, v := range sorted {
		w.Write([]byte(v))
	}

	w.Close()
	if err := ioutil.WriteFile(fileName, b.Bytes(), 0666); err != nil {
		log.Fatal(err)
	}
	log.Println("saved", fileName)
}

func (_b *BalanceParser) saveMap(_files uint32) {
	wg := new(sync.WaitGroup)
	wg.Add(2)

	path := fmt.Sprintf("%v/%v.%v", _b.outDir_, _files, _b.blockNum_)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, os.ModePerm)
	}

	go _b.saveBalance(wg, path)
	go _b.saveUnspent(wg, path)

	wg.Wait()
}

func FakeAddr(_script []byte) string {
	hash := make([]byte, 20)
	btc.RimpHash(_script, hash)

	var ad [25]byte
	copy(ad[1:21], hash)
	sh := btc.Sha2Sum(ad[0:21])
	copy(ad[21:25], sh[:4])
	addr58 := btc.Encodeb58(ad[:])
	return addr58
}

func (_b *BalanceParser) saveDayReport(_blockTime time.Time) {
	delta := time.Now().Sub(_blockTime)
	days := uint32(delta.Hours() / 24)
	if days >= 720 {
		return
	}

	fileName := fmt.Sprintf("%v/reduce.csv", _b.outDir_)
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()
	line := fmt.Sprintf("%v,%v,%v\n", days, _b.reduceNum_, _b.reduceSum_)
	f.WriteString(line)

	fileName2010 := fmt.Sprintf("%v/reduce2010.csv", _b.outDir_)
	f2010, err := os.OpenFile(fileName2010, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}
	defer f2010.Close()
	line2010 := fmt.Sprintf("%v,%v,%v\n", days, _b.reduceNum2010_, _b.reduceSum2010_)
	f2010.WriteString(line2010)

	fileName2014 := fmt.Sprintf("%v/reduce2014.csv", _b.outDir_)
	f2014, err := os.OpenFile(fileName2014, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}
	defer f2014.Close()
	line2014 := fmt.Sprintf("%v,%v,%v\n", days, _b.reduceNum2014_, _b.reduceSum2014_)
	f2014.WriteString(line2014)

	log.Println("[REDUCE]", line, line2010, line2014)
}

func (_b *BalanceParser) saveMonthReport(_blockTime time.Time) {
	lastDate := _blockTime.Add(-time.Hour * 24)

	fileName := fmt.Sprintf("%v/balance.csv", _b.outDir_)
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()

	fileName100 := fmt.Sprintf("%v/balance100.csv", _b.outDir_)
	f100, err := os.OpenFile(fileName100, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}
	defer f100.Close()

	fileName1000 := fmt.Sprintf("%v/balance1000.csv", _b.outDir_)
	f1000, err := os.OpenFile(fileName1000, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}
	defer f1000.Close()

	fileName10000 := fmt.Sprintf("%v/balance10000.csv", _b.outDir_)
	f10000, err := os.OpenFile(fileName10000, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}
	defer f10000.Close()

	fileName100000 := fmt.Sprintf("%v/balance100000.csv", _b.outDir_)
	f100000, err := os.OpenFile(fileName100000, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}
	defer f100000.Close()

	topList := new(lib.TopList)
	topList.Init(100000)
	balanceNum := len(_b.accountMap_)
	balanceSum := uint64(0)
	for _, v := range _b.accountMap_ {
		balanceSum += v.balance
		topList.Push(v.balance)
	}
	line := fmt.Sprintf("%v,%v,%v\n", lastDate.Local().Format("2006-01-02"), balanceNum, balanceSum)
	if _, err = f.WriteString(line); err != nil {
		log.Fatalln(err, line)
	}
	log.Println("[ALL]", line)

	top := topList.Sorted()
	sum := uint64(0)
	for k, v := range top {
		sum += v
		if k == 99 {
			line = fmt.Sprintf("%v,%v,%v\n", lastDate.Local().Format("2006-01-02"), sum, float64(sum)/float64(balanceSum))
			if _, err = f100.WriteString(line); err != nil {
				log.Fatalln(err, line)
			}
			log.Println("[100]", line)
		} else if k == 999 {
			line = fmt.Sprintf("%v,%v,%v\n", lastDate.Local().Format("2006-01-02"), sum, float64(sum)/float64(balanceSum))
			if _, err = f1000.WriteString(line); err != nil {
				log.Fatalln(err, line)
			}
			log.Println("[1000]", line)
		} else if k == 9999 {
			line = fmt.Sprintf("%v,%v,%v\n", lastDate.Local().Format("2006-01-02"), sum, float64(sum)/float64(balanceSum))
			if _, err = f10000.WriteString(line); err != nil {
				log.Fatalln(err, line)
			}
			log.Println("[10000]", line)
		}
	}
	if len(top) >= 100000 {
		line = fmt.Sprintf("%v,%v,%v\n", lastDate.Local().Format("2006-01-02"), sum, float64(sum)/float64(balanceSum))
		if _, err = f100000.WriteString(line); err != nil {
			log.Fatalln(err, line)
		}
		log.Println("[100000]", line)
	}

	fileNameReward := fmt.Sprintf("%v/reward.csv", _b.outDir_)
	fReward, err := os.OpenFile(fileNameReward, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}
	defer fReward.Close()

	line = fmt.Sprintf("%v,%v,%v\n", lastDate.Local().Format("2006-01-02"), _b.sumReward_, _b.sumFee_)
	if _, err = fReward.WriteString(line); err != nil {
		log.Fatalln(err, line)
	}
	log.Println("[REWARD]", line)
}

func (_b *BalanceParser) processBalance(_blockTime *time.Time) {
	for change := range _b.balanceChangeCh_ {
		account, ok := _b.accountMap_[change.addr]
		if !ok {
			account.time = uint32(change.time.Unix())
		}

		balance := int64(account.balance)
		if balance >= 1000*1e8 && change.change < 0 && _blockTime != nil {
			delta := time.Now().Sub(*_blockTime)
			days := uint32(delta.Hours() / 24)
			if days <= 720 {
				_b.reduceNum_ += 1
				_b.reduceSum_ += uint64(-change.change)

				t := time.Unix(int64(account.time), 0)
				if t.Year() < 2010 {
					_b.reduceNum2010_ += 1
					_b.reduceSum2010_ += uint64(-change.change)

				} else if t.Year() < 2014 {
					_b.reduceNum2014_ += 1
					_b.reduceSum2014_ += uint64(-change.change)
				}
			}
		}

		if balance += change.change; balance > 0 {
			account.balance = uint64(balance)
			_b.accountMap_[change.addr] = account
		} else if balance == 0 {
			delete(_b.accountMap_, change.addr)
		}
	}

	_b.balanceReadyCh_ <- true
}

func (_b *BalanceParser) processBlock(_wg *sync.WaitGroup) {
	defer _wg.Done()

	genesis := new(btc.Uint256)
	prev := *genesis

	lastLogTime := new(time.Time)
	for {
		if changeSet, ok := _b.prevMap_[prev]; ok {
			block := changeSet.block

			if (_b.blockNO_+1)%HALVING_BLOCKS == 0 {
				_b.halvingRate_ /= 2
			}
			reward := uint64(_b.halvingRate_ * MAX_REWARD)
			_b.sumReward_ += reward
			fee := changeSet.sumOut - reward
			_b.sumFee_ += fee

			blockTime := time.Unix(int64(block.BlockTime()), 0)
			if blockTime.Day() != lastLogTime.Day() {
				close(_b.balanceChangeCh_)
				<-_b.balanceReadyCh_

				_b.saveDayReport(blockTime)
				_b.reduceNum_ = 0
				_b.reduceSum_ = 0
				_b.reduceNum2010_ = 0
				_b.reduceSum2010_ = 0
				_b.reduceNum2014_ = 0
				_b.reduceSum2014_ = 0

				if blockTime.Month() != lastLogTime.Month() {
					_b.saveMonthReport(blockTime)
					_b.sumFee_ = 0
					_b.sumReward_ = 0
				}
				lastLogTime = &blockTime

				_b.balanceChangeCh_ = make(chan *tBalanceChange)
				go _b.processBalance(&blockTime)
			}

			// must first
			for txID, outputs := range changeSet.balanceMap {
				for _, output := range outputs {
					_b.balanceChangeCh_ <- &tBalanceChange{output.addr, int64(output.val), blockTime}
				}
				_b.unspentMap_[txID] = outputs
			}

			for txID, spent := range changeSet.spentMap {
				if unspent, ok := _b.unspentMap_[txID]; ok {
					for _, i := range spent {
						if output, ok := unspent[i]; ok {
							delete(unspent, i)
							_b.balanceChangeCh_ <- &tBalanceChange{output.addr, -int64(output.val), blockTime}
						}
					}
					if len(unspent) == 0 {
						delete(_b.unspentMap_, txID)
					}
				}
			}

			delete(_b.prevMap_, prev)

			prev = *block.Hash

			_b.blockNO_++
		} else {
			changeSet, ok := <-_b.changeSetCh_
			if !ok {
				break
			}

			block := changeSet.block

			parent := btc.NewUint256(block.ParentHash())
			_b.prevMap_[*parent] = changeSet
		}
	}
}
