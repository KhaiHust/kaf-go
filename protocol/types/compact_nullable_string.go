package types

type CompactNullableString *string

func NewCompactNullableString(s string) CompactNullableString {
	return &s
}
