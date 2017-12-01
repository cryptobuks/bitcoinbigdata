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
	"regexp"
	"strconv"
	"bufio"
	"strings"
	"sort"
	"runtime"
	"github.com/piotrnar/gocoin/lib/others/blockdb"
	"time"
	"github.com/forchain/bitcoinbigdata/lib"
)

type tOutput struct {
	addr string // index
	val  uint64 // val
}

//  (index -> output)
type tOutputMap map[uint16]tOutput

// tx -> tOutputMap
type tUnspentMap map[btc.Uint256]tOutputMap

// add -> balance
type tBalanceMap map[string]uint64

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

type BalanceParser struct {
	blockNO_ uint32
	fileNO_  int
	outDir_  string

	unspentMap_ tUnspentMap
	balanceMap_ tBalanceMap

	blocksCh_ chan *btc.Block
	prevMap_  map[btc.Uint256]*btc.Block

	fileList_ []int
	blockNum_ uint32
}

func (_b *BalanceParser) loadUnspent(_path string, _wg *sync.WaitGroup) {
	defer _wg.Done()

	filename := fmt.Sprintf("%v/unspent.gz", _path)
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	gr, err := gzip.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}
	defer gr.Close()

	scanner := bufio.NewScanner(gr)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1000*1024*1024)
	for scanner.Scan() {
		l := scanner.Text()
		if tokens := strings.Split(l, ","); len(tokens) == 2 {
			txID := *btc.NewUint256FromString(tokens[0])
			outputs := tokens[1:]

			out := make(tOutputMap)
			for _, output := range outputs {
				if tokens := strings.Split(output, " "); len(tokens) == 3 {
					if index, err := strconv.Atoi(tokens[0]); err == nil {
						addr := tokens[1]
						if val, err := strconv.ParseUint(tokens[2], 10, 0); err == nil {
							out[uint16(index)] = tOutput{
								addr,
								val,
							}
						}
					}
				}
			}
			_b.unspentMap_[txID] = out
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	log.Println("loaded", filename)
}

func (_b *BalanceParser) loadBalance(_path string, _wg *sync.WaitGroup) {
	defer _wg.Done()

	filename := fmt.Sprintf("%v/balance.gz", _path)

	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	gr, err := gzip.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}
	defer gr.Close()

	scanner := bufio.NewScanner(gr)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 100*1024*1024)
	for scanner.Scan() {
		l := scanner.Text()

		if tokens := strings.Split(l, " "); len(tokens) == 2 {
			addr := tokens[0]
			if balance, err := strconv.ParseUint(tokens[1], 10, 0); err == nil {
				_b.balanceMap_[addr] = balance
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	log.Println("loaded", filename)
}

func (_b *BalanceParser) loadMap() {
	if files, err := ioutil.ReadDir(_b.outDir_); err == nil && len(files) > 0 {
		start := 0
		var fi os.FileInfo
		for _, f := range files {
			if f.IsDir() {
				r, err := regexp.Compile("(\\d+)\\.(\\d+)") // Do we have an 'N' or 'index' at the beginning?
				if err != nil {
					log.Println(err)
					break
				}
				if matches := r.FindStringSubmatch(f.Name()); len(matches) == 3 {
					if fileNO, err := strconv.Atoi(matches[1]); err == nil {
						if blockNO, err := strconv.Atoi(matches[2]); err == nil {
							if uint32(blockNO) < _b.blockNO_ {
								if blockNO > start {
									start = blockNO
									fi = f
									_b.fileNO_ = fileNO
								}
							} else if uint32(blockNO) == _b.blockNO_ {
								start = blockNO
								fi = f
								_b.fileNO_ = fileNO
								break
							}
						}
					}
				}
			}
		}

		if start > 0 {
			wg := new(sync.WaitGroup)
			wg.Add(2)
			path := fmt.Sprintf("%v/%v", _b.outDir_, fi.Name())
			go _b.loadUnspent(path, wg)
			go _b.loadBalance(path, wg)

			wg.Wait()
		}
	}
}

func (_b *BalanceParser) loadBlock(dat []byte, _wg *sync.WaitGroup) {
	defer _wg.Done()

	bl, er := btc.NewBlock(dat[:])

	if er != nil {
		log.Fatalln("Block inconsistent:", er.Error())
	}

	bl.BuildTxList()

	_b.blocksCh_ <- bl
}

func (_b *BalanceParser) Parse(_blockNO uint32, _dataDir string, _outDir string) {
	cpuNum := runtime.NumCPU()
	magicID := [4]byte{0xF9, 0xBE, 0xB4, 0xD9}

	_b.blockNO_ = _blockNO

	_b.outDir_ = _outDir
	_b.fileList_ = make([]int, 0)
	_b.fileNO_ = -1

	_b.prevMap_ = make(map[btc.Uint256]*btc.Block)

	_b.blockNum_ = uint32(0)
	_b.blocksCh_ = make(chan *btc.Block, cpuNum)

	_b.unspentMap_ = make(tUnspentMap)
	// address -> balance
	_b.balanceMap_ = make(tBalanceMap)

	// Specify blocks directory
	blockDatabase := blockdb.NewBlockDB(_dataDir+"/blocks", magicID)

	end_block := 500000

	waitProcess := new(sync.WaitGroup)
	waitProcess.Add(1)
	go _b.processBlock(waitProcess)

	waitLoad := new(sync.WaitGroup)
	for i := 0; i < end_block+1; i++ {

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
	waitProcess.Wait()

	log.Print("balance number:", len(_b.balanceMap_))
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

	for k, v := range _b.balanceMap_ {
		line := fmt.Sprintln(k, v)
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
		os.Mkdir(path, 0755)
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

func (_b *BalanceParser) saveByAddress(_blockTime time.Time) {

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
	balanceNum := len(_b.balanceMap_)
	balanceSum := uint64(0)
	for _, v := range _b.balanceMap_ {
		balanceSum += v
		topList.Push(v)
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
}

func (_b *BalanceParser) processBlock(_wg *sync.WaitGroup) {
	defer _wg.Done()

	genesis := new(btc.Uint256)
	prev := *genesis
	blockNum := 0
	lastMonth := time.January
	for {
		unspentMap := _b.unspentMap_
		balanceMap := _b.balanceMap_

		if block, ok := _b.prevMap_[prev]; ok {
			blockTime := time.Unix(int64(block.BlockTime()), 0)
			if blockTime.Month() != lastMonth {
				_b.saveByAddress(blockTime)
				lastMonth = blockTime.Month()
			}

			for _, t := range block.Txs {
				txID := *t.Hash

				for _, i := range t.TxIn {
					if int32(i.Input.Vout) >= 0 {
						hash := *btc.NewUint256(i.Input.Hash[:])
						index := uint16(i.Input.Vout)
						if unspent, ok := unspentMap[hash]; ok {
							if o, ok := unspent[index]; ok {
								delete(unspent, index)
								if len(unspent) == 0 {
									delete(unspentMap, hash)
								}

								balance := balanceMap[o.addr]
								balance -= o.val
								if balance <= 0 {
									delete(balanceMap, o.addr)
								} else {
									balanceMap[o.addr] = balance
								}
							}
						}
					}
				}

				unspent := make(tOutputMap)
				for i, o := range t.TxOut {
					if o.Value == 0 {
						continue
					}
					index := uint16(i)
					addr := ""
					a := btc.NewAddrFromPkScript(o.Pk_script, false)
					if a == nil {
						addr = FakeAddr(o.Pk_script)
					} else {
						addr = a.String()
					}
					val := uint64(o.Value)
					balanceMap[addr] = balanceMap[addr] + val
					unspent[index] = tOutput{addr, val}
				}
				unspentMap[txID] = unspent
			}
			delete(_b.prevMap_, prev)

			prev = *block.Hash

			blockNum++
		} else {
			block := <-_b.blocksCh_

			parent := btc.NewUint256(block.ParentHash())
			_b.prevMap_[*parent] = block
		}
	}
}
