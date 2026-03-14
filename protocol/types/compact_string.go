package types

type CompactString string

func (c CompactString) String() string {
	return string(c)
}

func NewCompactString(s string) CompactString {
	return CompactString(s)
}
