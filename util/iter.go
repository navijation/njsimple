package util

import "iter"

func SeqOf[T any](items ...T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, item := range items {
			if !yield(item) {
				return
			}
		}
	}
}

func SeqAt[T any](seq iter.Seq[T], idx int) (out T, exists bool) {
	var i int
	for item := range seq {
		if i == idx {
			return item, true
		}
		i++
	}
	return out, false
}

func Seq2At[U, V any](seq iter.Seq2[U, V], idx int) (out1 U, out2 V, exists bool) {
	var i int
	for item1, item2 := range seq {
		if i == idx {
			return item1, item2, true
		}
		i++
	}
	return out1, out2, false
}
