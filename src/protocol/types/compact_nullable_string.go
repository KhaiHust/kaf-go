package types

type CompactNullableString *string

func NewCompactNullableString(s *string) CompactNullableString {
	if s == nil {
		return nil
	}
	return CompactNullableString(s)
}
