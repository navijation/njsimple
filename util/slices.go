package util

func CloneSliceFunc[T any](slice []T, copy func(T) T) (out []T) {
	if slice == nil {
		return nil
	}

	out = make([]T, 0, len(slice))
	for _, item := range slice {
		out = append(out, copy(item))
	}

	return out
}
