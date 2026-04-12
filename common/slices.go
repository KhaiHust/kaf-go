package common

func InSlice[T comparable](originalSlices []T, value T) bool {
	for _, slice := range originalSlices {
		if value == slice {
			return true
		}
	}
	return false
}
