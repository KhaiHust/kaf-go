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
	ErrIllegalGeneration       = 22
	ErrUnknowMemberId          = 25
	ErrRebalanceInProgress     = 27
	ErrTopicAlreadyExists      = 36
	ErrKafkaStorageError       = 56
	ErrGroupIdNotFound         = 69
	ErrMemberIdRequired        = 79
)

func IsError(err error) bool {
	return err != nil && len(err.Error()) > len(ErrorFormat)-2 && err.Error()[:len(ErrorFormat)-2] == ErrorFormat[:len(ErrorFormat)-2]
}

func GetErrorId(err error) int16 {
	if !IsError(err) {
		return -1
	}

	var errID int16
	_, errScan := fmt.Sscanf(err.Error(), ErrorFormat, &errID)
	if errScan != nil {
		return -1
	}

	return errID
}

func CreateError(errID int16) error {
	return errors.New(fmt.Sprintf(ErrorFormat, errID))
}
