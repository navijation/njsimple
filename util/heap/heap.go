package heap

import "container/heap"

type Heap[T any] struct {
	wrapper heapWrapper[T]
}

func NewHeap[T any](comparator func(a, b T) int, items ...T) Heap[T] {
	out := Heap[T]{
		wrapper: heapWrapper[T]{
			comparator: comparator,
			items:      items,
		},
	}
	heap.Init(&out.wrapper)
	return out
}

func (me *Heap[T]) Size() int {
	return len(me.wrapper.items)
}

func (me *Heap[T]) Peek() T {
	return me.wrapper.items[0]
}

func (me *Heap[T]) Pop() T {
	return heap.Pop(&me.wrapper).(T)
}

func (me *Heap[T]) Push(value T) {
	heap.Push(&me.wrapper, value)
}
