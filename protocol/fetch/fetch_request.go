package fetch

import (
	"fmt"

	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
	"github.com/gofrs/uuid/v5"
)

type FetchRequest struct {
	MaxWaitMs       int32
	MinBytes        int32
	MaxBytes        int32
	IsolationLevel  int8
	SessionId       int32
	SessionEpoch    int32
	Topics          []FetchTopic
	ForgottenTopics []ForgottenTopic
	RackId          types.CompactString

	// tagged fields at request level
	ClusterId    types.CompactNullableString // tag: 0
	ReplicaState *ReplicaState               // tag: 1
}

type FetchTopic struct {
	TopicId    uuid.UUID
	Partitions []FetchPartition
}

type FetchPartition struct {
	Partition          int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LastFetchedEpoch   int32
	LogStartOffset     int64
	PartitionMaxBytes  int32

	// tagged fields at partition level
	ReplicaDirectoryId *uuid.UUID // tag: 0
	HighWatermark      *int64     // tag: 1
}

type ForgottenTopic struct {
	TopicId    uuid.UUID
	Partitions []int32
}

type ReplicaState struct {
	ReplicaId    int32
	ReplicaEpoch int64
}

func (f *FetchRequest) Decode(r *protocol.Reader) error {
	var err error

	if f.MaxWaitMs, err = r.ReadInt32(); err != nil {
		return err
	}
	if f.MinBytes, err = r.ReadInt32(); err != nil {
		return err
	}
	if f.MaxBytes, err = r.ReadInt32(); err != nil {
		return err
	}
	if f.IsolationLevel, err = r.ReadInt8(); err != nil {
		return err
	}
	if f.SessionId, err = r.ReadInt32(); err != nil {
		return err
	}
	if f.SessionEpoch, err = r.ReadInt32(); err != nil {
		return err
	}

	// topics
	topicsLen, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}
	f.Topics = make([]FetchTopic, topicsLen)
	for i := range f.Topics {
		if err = f.Topics[i].decode(r); err != nil {
			return fmt.Errorf("topic[%d]: %w", i, err)
		}
	}

	// forgotten_topics_data
	forgottenLen, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}
	f.ForgottenTopics = make([]ForgottenTopic, forgottenLen)
	for i := range f.ForgottenTopics {
		if err = f.ForgottenTopics[i].decode(r); err != nil {
			return fmt.Errorf("forgotten_topic[%d]: %w", i, err)
		}
	}

	// rack_id — regular COMPACT_STRING (not tagged)
	if f.RackId, err = r.ReadCompactString(); err != nil {
		return err
	}

	// request-level tagged fields
	return f.decodeTaggedFields(r)
}

// decodeTaggedFields reads cluster_id (tag:0) and replica_state (tag:1)
func (f *FetchRequest) decodeTaggedFields(r *protocol.Reader) error {
	count, err := r.ReadUVarInt()
	if err != nil {
		return err
	}
	for i := uint64(0); i < count; i++ {
		tag, err := r.ReadUVarInt()
		if err != nil {
			return err
		}
		size, err := r.ReadUVarInt()
		if err != nil {
			return err
		}

		switch tag {
		case 0: // cluster_id => COMPACT_NULLABLE_STRING
			_ = size
			f.ClusterId, err = r.ReadCompactNullableString()
			if err != nil {
				return fmt.Errorf("cluster_id tag: %w", err)
			}

		case 1: // replica_state => replica_id(INT32) + replica_epoch(INT64)
			_ = size
			f.ReplicaState = &ReplicaState{}
			if f.ReplicaState.ReplicaId, err = r.ReadInt32(); err != nil {
				return fmt.Errorf("replica_state.replica_id: %w", err)
			}
			if f.ReplicaState.ReplicaEpoch, err = r.ReadInt64(); err != nil {
				return fmt.Errorf("replica_state.replica_epoch: %w", err)
			}

		default:
			// unknown tag — skip by size
			if _, err = r.ReadRawBytes(int(size)); err != nil {
				return fmt.Errorf("skip unknown tag %d: %w", tag, err)
			}
		}
	}
	return nil
}

// --- FetchTopic ---

func (t *FetchTopic) decode(r *protocol.Reader) error {
	var err error
	if t.TopicId, err = r.ReadUUID(); err != nil {
		return err
	}

	partitionsLen, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}
	t.Partitions = make([]FetchPartition, partitionsLen)
	for i := range t.Partitions {
		if err = t.Partitions[i].decode(r); err != nil {
			return fmt.Errorf("partition[%d]: %w", i, err)
		}
	}
	return r.ReadTaggedFields() // topic-level tagged fields (empty in v18)
}

// --- FetchPartition ---

func (p *FetchPartition) decode(r *protocol.Reader) error {
	var err error
	if p.Partition, err = r.ReadInt32(); err != nil {
		return err
	}
	if p.CurrentLeaderEpoch, err = r.ReadInt32(); err != nil {
		return err
	}
	if p.FetchOffset, err = r.ReadInt64(); err != nil {
		return err
	}
	if p.LastFetchedEpoch, err = r.ReadInt32(); err != nil {
		return err
	}
	if p.LogStartOffset, err = r.ReadInt64(); err != nil {
		return err
	}
	if p.PartitionMaxBytes, err = r.ReadInt32(); err != nil {
		return err
	}

	// partition-level tagged fields
	return p.decodeTaggedFields(r)
}

func (p *FetchPartition) decodeTaggedFields(r *protocol.Reader) error {
	count, err := r.ReadUVarInt()
	if err != nil {
		return err
	}
	for i := uint64(0); i < count; i++ {
		tag, err := r.ReadUVarInt()
		if err != nil {
			return err
		}
		size, err := r.ReadUVarInt()
		if err != nil {
			return err
		}

		switch tag {
		case 0: // replica_directory_id => UUID
			_ = size
			id, err := r.ReadUUID()
			if err != nil {
				return fmt.Errorf("replica_directory_id tag: %w", err)
			}
			p.ReplicaDirectoryId = &id

		case 1: // high_watermark => INT64
			_ = size
			hw, err := r.ReadInt64()
			if err != nil {
				return fmt.Errorf("high_watermark tag: %w", err)
			}
			p.HighWatermark = &hw

		default:
			if _, err = r.ReadRawBytes(int(size)); err != nil {
				return fmt.Errorf("skip unknown tag %d: %w", tag, err)
			}
		}
	}
	return nil
}

// --- ForgottenTopic ---

func (f *ForgottenTopic) decode(r *protocol.Reader) error {
	var err error
	if f.TopicId, err = r.ReadUUID(); err != nil {
		return err
	}

	partitionsLen, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}
	f.Partitions = make([]int32, partitionsLen)
	for i := range f.Partitions {
		if f.Partitions[i], err = r.ReadInt32(); err != nil {
			return fmt.Errorf("forgotten partition[%d]: %w", i, err)
		}
	}
	return r.ReadTaggedFields()
}
