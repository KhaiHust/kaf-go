package constant

import (
	"errors"
	"fmt"
)

const ErrorFormat = "KAF_GO_ERR_%d"

const (
	ErrCorruptMessage          = 2
	ErrUnknownTopicOrPartition = 3
	ErrInvalidTopicException   = 17
	ErrTopicAlreadyExists      = 36
	ErrKafkaStorageError       = 56
)

func CreateError(errID int16) error {
	return errors.New(fmt.Sprintf(ErrorFormat, errID))
}
