package parsers

import (
	"testing"
	"container/heap"
	"fmt"
)

func TestBalanceParser_Parse(t *testing.T) {

}

func TestBalanceParser_Address(t *testing.T) {

	data := "483045022100f4ece69a7c50c911b3af6fa017dcf22de4df66699cd85c5753634d85140b955602204996b677af3a0b5835b36ae1db6323a125f1525edd4727be3209a0535073f42201410412b80271b9e034006fd944ae4cdbdbc45ee30595c1f8961439385575f1973019b3ff615afed85a75737ff0d43cd81df74bc76004b45a6e7c9e2d115f364da1d7"

	addr := FakeAddr([]byte(data))
	t.Log(addr, len(addr))

}

// This example demonstrates an integer heap built using the heap interface.

// An IntHeap is a min-heap of ints.
type IntHeap []int

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *IntHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(int))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// This example inserts several ints into an IntHeap, checks the minimum,
// and removes them in order of priority.
func TestIntHeap(t *testing.T) {
	h := &IntHeap{2, 1, 5}
	heap.Init(h)
	heap.Push(h, 3)
	fmt.Printf("minimum: %d\n", (*h)[0])
	for h.Len() > 0 {
		fmt.Printf("%d ", heap.Pop(h))
	}
}
