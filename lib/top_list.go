package lib

import (
	"container/heap"
	"sort"
)

// An uint64Heap is a min-heap of ints.
type uint64Heap []uint64

func (h uint64Heap) Len() int           { return len(h) }
func (h uint64Heap) Less(i, j int) bool { return h[i] < h[j] }
func (h uint64Heap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *uint64Heap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(uint64))
}

func (h *uint64Heap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0: n-1]
	return x
}

type TopList struct {
	capacity_ uint32
	heap_     uint64Heap
}

func (_t *TopList) Init(_capacity uint32) {
	_t.capacity_ = _capacity
	heap.Init(&_t.heap_)
}

func (_t *TopList) Push(_x uint64) {
	if uint32(_t.heap_.Len()) >= _t.capacity_ {
		if _x > _t.Min() {
			heap.Push(&_t.heap_, _x)
			heap.Pop(&_t.heap_)
		}
	} else {
		heap.Push(&_t.heap_, _x)
	}
}

func (_t *TopList) Pop() uint64 {
	return heap.Pop(&_t.heap_).(uint64)
}

func (_t *TopList) Len() int {
	return _t.heap_.Len()
}

func (_t *TopList) Min() uint64 {
	return _t.heap_[0]
}

type Uint64Sorted []uint64

func (h Uint64Sorted) Len() int           { return len(h) }
func (h Uint64Sorted) Less(i, j int) bool { return h[i] > h[j] }
func (h Uint64Sorted) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (_t *TopList) Sorted() []uint64 {

	list := make(Uint64Sorted, 0)

	for _, v := range _t.heap_ {
		list = append(list, v)
	}

	sort.Sort(list)

	return list
}
