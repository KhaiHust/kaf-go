package common

import "github.com/KhaiHust/kaf-go/constant"

func ErrorMessage(code int16) string {
	switch code {
	case constant.ErrUnknownTopicOrPartition:
		return "unknown topic or partition"
	case constant.ErrCorruptMessage:
		return "corrupt message"
	case constant.ErrKafkaStorageError:
		return "kafka storage error"
	default:
		return "unknown error"
	}
}
