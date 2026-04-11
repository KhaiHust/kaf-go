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

func (f *FetchRequest) Encode(w *protocol.Writer) error {
	w.WriteInt32(f.MaxWaitMs)
	w.WriteInt32(f.MinBytes)
	w.WriteInt32(f.MaxBytes)
	w.WriteInt8(f.IsolationLevel)
	w.WriteInt32(f.SessionId)
	w.WriteInt32(f.SessionEpoch)

	// topics
	w.WriteCompactArrayLen(len(f.Topics))
	for i := range f.Topics {
		if err := f.Topics[i].encode(w); err != nil {
			return fmt.Errorf("topic[%d]: %w", i, err)
		}
	}

	// forgotten_topics_data
	w.WriteCompactArrayLen(len(f.ForgottenTopics))
	for i := range f.ForgottenTopics {
		if err := f.ForgottenTopics[i].encode(w); err != nil {
			return fmt.Errorf("forgotten_topic[%d]: %w", i, err)
		}
	}

	// rack_id — regular COMPACT_STRING (not tagged)
	w.WriteCompactString(f.RackId)

	// request-level tagged fields
	f.encodeTaggedFields(w)

	return nil
}

func (f *FetchRequest) encodeTaggedFields(w *protocol.Writer) {
	var count int
	if f.ClusterId != nil {
		count++
	}
	if f.ReplicaState != nil {
		count++
	}
	w.WriteUVarInt(uint64(count))

	if f.ClusterId != nil {
		s := *f.ClusterId
		size := protocol.UVarIntSize(uint64(len(s)+1)) + len(s)
		w.WriteUVarInt(0) // tag 0: cluster_id
		w.WriteUVarInt(uint64(size))
		w.WriteCompactNullableString(f.ClusterId)
	}

	if f.ReplicaState != nil {
		w.WriteUVarInt(1)  // tag 1: replica_state
		w.WriteUVarInt(12) // INT32 + INT64 = 4 + 8 bytes
		w.WriteInt32(f.ReplicaState.ReplicaId)
		w.WriteInt64(f.ReplicaState.ReplicaEpoch)
	}
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

func (t *FetchTopic) encode(w *protocol.Writer) error {
	w.WriteUUID(t.TopicId)

	w.WriteCompactArrayLen(len(t.Partitions))
	for i := range t.Partitions {
		if err := t.Partitions[i].encode(w); err != nil {
			return fmt.Errorf("partition[%d]: %w", i, err)
		}
	}
	w.WriteEmptyTaggedFields() // empty in v18
	return nil
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

func (p *FetchPartition) encode(w *protocol.Writer) error {
	w.WriteInt32(p.Partition)
	w.WriteInt32(p.CurrentLeaderEpoch)
	w.WriteInt64(p.FetchOffset)
	w.WriteInt32(p.LastFetchedEpoch)
	w.WriteInt64(p.LogStartOffset)
	w.WriteInt32(p.PartitionMaxBytes)
	p.encodeTaggedFields(w)
	return nil
}

func (p *FetchPartition) encodeTaggedFields(w *protocol.Writer) {
	var count int
	if p.ReplicaDirectoryId != nil {
		count++
	}
	if p.HighWatermark != nil {
		count++
	}
	w.WriteUVarInt(uint64(count))

	if p.ReplicaDirectoryId != nil {
		w.WriteUVarInt(0)  // tag 0: replica_directory_id
		w.WriteUVarInt(16) // UUID = 16 bytes
		w.WriteUUID(*p.ReplicaDirectoryId)
	}

	if p.HighWatermark != nil {
		w.WriteUVarInt(1) // tag 1: high_watermark
		w.WriteUVarInt(8) // INT64 = 8 bytes
		w.WriteInt64(*p.HighWatermark)
	}
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

func (f *ForgottenTopic) encode(w *protocol.Writer) error {
	w.WriteUUID(f.TopicId)

	w.WriteCompactArrayLen(len(f.Partitions))
	for i := range f.Partitions {
		w.WriteInt32(f.Partitions[i])
	}
	w.WriteEmptyTaggedFields() // empty in v18
	return nil
}
