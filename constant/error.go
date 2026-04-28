package constant

import (
	"errors"
	"fmt"
)

const ErrorFormat = "KAF_GO_ERR_%d"

const (
	ErrCorruptMessage               = 2
	ErrUnknownTopicOrPartition      = 3
	ErrLeaderNotAvailable           = 5
	ErrErrNotLeaderForPartition     = 6
	ErrRequestTimedOut              = 7
	ErrInvalidTopicException        = 17
	ErrNotEnoughReplicas            = 19
	ErrNotEnoughReplicasAfterAppend = 20
	ErrIllegalGeneration            = 22
	ErrUnknowMemberId               = 25
	ErrRebalanceInProgress          = 27
	ErrTopicAlreadyExists           = 36
	ErrDuplicateSequenceNumber      = 45
	ErrInvalidProducerEpoch         = 46
	ErrKafkaStorageError            = 56
	ErrUnknownProducerId            = 59

	ErrGroupIdNotFound          = 69
	ErrMemberIdRequired         = 79
	ErrInvalidRequest           = 42
	ErrInvalidRequiredAcks      = 72
	ErrStaleBrokerEpoch         = 77
	ErrOutOfOrderSequenceNumber = 90
	ErrNotController            = 41
	ErrUnknownControllerID      = 116
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
