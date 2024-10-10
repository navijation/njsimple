package util

type Optional[T any] struct {
	item   T
	exists bool
}

func Some[T any](v T) Optional[T] {
	return Optional[T]{
		item:   v,
		exists: true,
	}
}

func None[T any]() Optional[T] {
	return Optional[T]{}
}

func (me *Optional[T]) Unpack() (T, bool) {
	return me.item, me.exists
}

func (me *Optional[T]) Or(defaultValue T) T {
	if me.exists {
		return me.item
	}
	return defaultValue
}
