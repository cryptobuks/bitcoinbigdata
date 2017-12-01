package lib

import "testing"

func TestTopList_Init(t *testing.T) {

	list := new(TopList)
	list.Init(3)
	list.Push(2)
	list.Push(1)
	list.Push(3)

	list.Push(9)

	for list.Len() > 0 {
		t.Log(list.Pop())
	}
}
