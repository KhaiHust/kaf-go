package types

type CompactString string

func NewCompactString(s string) CompactString {
	return CompactString(s)
}
