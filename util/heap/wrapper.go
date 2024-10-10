package heap

import "container/heap"

var _ heap.Interface = (*heapWrapper[any])(nil)

type heapWrapper[T any] struct {
	comparator func(a, b T) int
	items      []T
}

func (me *heapWrapper[T]) Len() int {
	return len(me.items)
}

func (me *heapWrapper[T]) Swap(i, j int) {
	me.items[i], me.items[j] = me.items[j], me.items[i]
}

func (me *heapWrapper[T]) Less(i, j int) bool {
	return me.comparator(me.items[i], me.items[j]) < 0
}

// Pop implements heap.Interface.
func (me *heapWrapper[T]) Pop() any {
	out := me.items[len(me.items)-1]
	me.items = me.items[:len(me.items)-1]
	return out
}

// Push implements heap.Interface.
func (me *heapWrapper[T]) Push(x any) {
	me.items = append(me.items, x.(T))
}
