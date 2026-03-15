package types

type Varlong int64

func (b Varlong) Long() int64 {
	return int64(b)
}
