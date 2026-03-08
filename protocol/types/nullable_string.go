package types

type NullableString *string

func NewNullableString(s *string) NullableString {
	if s == nil {
		return nil
	}
	return NullableString(s)
}
