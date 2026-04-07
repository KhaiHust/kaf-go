package coordinator

import (
	"fmt"
	"sync"
	"time"

	"github.com/KhaiHust/kaf-go/constant"
	"github.com/gofrs/uuid/v5"
)

type GroupStore struct {
	mu      sync.RWMutex
	groups  map[string]*GroupMeta                  // groupId => GroupMeta
	offsets map[string]map[string]*OffsetPartition // groupId => (topicId:partition => OffsetPartition)
}

func offsetKey(topicName string, partitionIdx int32) string {
	return fmt.Sprintf("%s:%d", topicName, partitionIdx)
}

type OffsetPartition struct {
	TopicName           string
	PartitionIdx        int32
	Offset              int64
	LeaderEpoch         int32
	Metadata            *string
	LastCommitTimestamp time.Time
}

type CommitOffset struct {
	TopicName    string
	PartitionIdx int32
	Offset       int64
	LeaderEpoch  int32
	Metadata     *string
}

type FetchOffsetQuery struct {
	TopicName    string
	PartitionIdx int32
}

type FetchOffsetResult struct {
	TopicName    string
	PartitionIdx int32
	Offset       int64
	LeaderEpoch  int32
	Metadata     *string
}

func NewGroupStore() *GroupStore {
	return &GroupStore{
		mu:      sync.RWMutex{},
		groups:  make(map[string]*GroupMeta),
		offsets: make(map[string]map[string]*OffsetPartition),
	}
}

type GroupMeta struct {
	GroupId          string
	State            int8
	GenerationId     int32
	LeaderId         string
	ProtocolName     string
	members          map[string]*GroupMemberMetadata
	pendingJoins     []pendingJoin
	pendingSyncs     []pendingSync
	leaderAssignment map[string][]byte
	rebalanceTimer   *time.Timer
}

type GroupMemberMetadata struct {
	MemberId         string
	GroupInstanceId  string
	SessionTimeout   time.Duration
	RebalanceTimeout time.Duration
	Metadata         []byte
	Assignment       []byte

	sessionTimer *time.Timer
	evicted      bool
	evictMu      sync.Mutex
}

type pendingJoin struct {
	memberId string
	resultCh chan joinResult
}

type JoinMember struct {
	MemberId        string
	GroupInstanceId string
	Metadata        []byte
}

type joinResult struct {
	GenerationId int32
	LeaderId     string
	ProtocolName string
	Members      []JoinMember
	Err          error
}

type pendingSync struct {
	memberId   string
	resultSync chan SyncResult
}

type SyncResult struct {
	Assignment []byte
	Err        error
}

func (gs *GroupStore) Heartbeat(groupId, memberId string, generationId int32) int8 {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	group, existed := gs.groups[groupId]
	if !existed {
		return constant.HeartbeatUnknowMember
	}

	member, existed := group.members[memberId]
	if !existed {
		return constant.HeartbeatUnknowMember
	}

	if generationId != group.GenerationId {
		return constant.HeartbeatIllegalGeneration
	}

	gs.resetSessionTimer(group, member)

	if group.State == constant.GroupStatePreparingRebalance ||
		group.State == constant.GroupStateCompletingRebalance {
		return constant.HeartbeatRebalanceInProcess
	}

	return constant.HeartbeatOK
}

func (gs *GroupStore) JoinGroup(
	groupId, memberId, groupInstanceId, protocolName string,
	metadata []byte,
	sessionTimeout, rebalanceTimeout time.Duration) (<-chan joinResult, error) {

	gs.mu.Lock()
	defer gs.mu.Unlock()

	group, existed := gs.groups[groupId]
	if !existed {
		group = &GroupMeta{
			GroupId:      groupId,
			State:        constant.GroupStateEmpty,
			members:      make(map[string]*GroupMemberMetadata),
			ProtocolName: protocolName,
		}
		gs.groups[groupId] = group
	}

	if memberId == "" {
		return nil, constant.CreateError(constant.ErrMemberIdRequired)
	}

	member, memberExisted := group.members[memberId]
	if !memberExisted {
		member = &GroupMemberMetadata{
			MemberId:         memberId,
			GroupInstanceId:  groupInstanceId,
			SessionTimeout:   sessionTimeout,
			RebalanceTimeout: rebalanceTimeout,
		}
		group.members[memberId] = member
	}

	member.Metadata = metadata

	gs.resetSessionTimer(group, member)

	if group.State == constant.GroupStateStable ||
		group.State == constant.GroupStateEmpty ||
		group.State == constant.GroupStateDead {
		gs.transitionToPrepareRebalance(group)
	}

	resultCh := make(chan joinResult, 1)
	group.pendingJoins = append(group.pendingJoins, pendingJoin{
		memberId: memberId,
		resultCh: resultCh,
	})

	gs.maybeCloseJoinBarrier(group)

	return resultCh, nil

}

func (gs *GroupStore) LeaveGroup(groupId string, memberId string) error {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	group, existed := gs.groups[groupId]
	if !existed {
		return constant.CreateError(constant.ErrGroupIdNotFound)
	}

	member, existed := group.members[memberId]
	if !existed {
		return constant.CreateError(constant.ErrUnknowMemberId)
	}

	member.sessionTimer.Stop()
	member.evictMu.Lock()
	member.evicted = true
	member.evictMu.Unlock()

	delete(group.members, memberId)

	if len(group.members) == 0 {
		group.State = constant.GroupStateDead
		gs.scheduleGroupCleanup(groupId)
	} else {
		gs.transitionToPrepareRebalance(group)
	}

	return nil

}

func (gs *GroupStore) SyncGroup(
	groupId, memberId string,
	generationId int32,
	assignments map[string][]byte,
) (<-chan SyncResult, error) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	group, existed := gs.groups[groupId]
	if !existed {
		return nil, constant.CreateError(constant.ErrGroupIdNotFound)
	}

	if generationId != group.GenerationId {
		return nil, constant.CreateError(constant.ErrIllegalGeneration)
	}

	if group.State != constant.GroupStateCompletingRebalance {
		return nil, constant.CreateError(constant.ErrRebalanceInProgress)
	}

	resultCh := make(chan SyncResult, 1)

	if memberId == group.LeaderId && assignments != nil {
		group.leaderAssignment = assignments

		leaderAssignment := assignments[memberId]
		resultCh <- SyncResult{
			Assignment: leaderAssignment,
			Err:        nil,
		}

		for _, ps := range group.pendingSyncs {
			followAssignment := assignments[ps.memberId]
			ps.resultSync <- SyncResult{
				Assignment: followAssignment,
			}
		}
		group.pendingSyncs = nil
		group.State = constant.GroupStateStable

	} else {
		if group.leaderAssignment != nil {
			resultCh <- SyncResult{Assignment: group.leaderAssignment[memberId]}
		} else {
			group.pendingSyncs = append(group.pendingSyncs, pendingSync{
				memberId:   memberId,
				resultSync: resultCh,
			})
		}
	}

	return resultCh, nil

}
func (gs *GroupStore) GenerateMemberId(groupId string) string {
	return fmt.Sprintf("%s-%s", groupId, uuid.Must(uuid.NewV7()).String())
}

func (gs *GroupStore) resetSessionTimer(group *GroupMeta, member *GroupMemberMetadata) {
	if member.sessionTimer == nil {
		member.sessionTimer = time.AfterFunc(member.SessionTimeout, func() {
			gs.evictMember(group.GroupId, member.MemberId)
		})
	} else {
		member.sessionTimer.Reset(member.SessionTimeout)
	}
}

func (gs *GroupStore) evictMember(groupId, memberId string) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	group, isExisted := gs.groups[groupId]
	if !isExisted {
		return
	}

	member, isExisted := group.members[memberId]

	if !isExisted {
		return
	}

	member.evictMu.Lock()
	if member.evicted {
		member.evictMu.Unlock()
		return
	}

	member.evicted = true
	member.evictMu.Unlock()

	delete(group.members, memberId)

	if len(group.members) == 0 {
		group.State = constant.GroupStateDead
		gs.scheduleGroupCleanup(groupId)
		return
	}

	gs.transitionToPrepareRebalance(group)
}

func (gs *GroupStore) transitionToPrepareRebalance(group *GroupMeta) {
	group.State = constant.GroupStatePreparingRebalance
	group.GenerationId++
	group.LeaderId = ""
	group.ProtocolName = ""
	group.leaderAssignment = nil
	group.pendingJoins = nil
	group.pendingSyncs = nil

	if group.rebalanceTimer != nil {
		group.rebalanceTimer.Stop()
	}

	maxTimeout := gs.maxRebalanceTimeout(group)
	group.rebalanceTimer = time.AfterFunc(maxTimeout, func() {
		gs.onRebalanceTimeout(group.GroupId, group.GenerationId)
	})

}

func (gs *GroupStore) onRebalanceTimeout(groupId string, expectedGeneration int32) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	group, existed := gs.groups[groupId]
	if !existed || group.GenerationId != expectedGeneration {
		return
	}

	joinMemberIds := make(map[string]bool)
	for _, pj := range group.pendingJoins {
		joinMemberIds[pj.memberId] = true
	}

	for memberId, member := range group.members {
		if !joinMemberIds[memberId] {
			member.sessionTimer.Stop()
			delete(group.members, memberId)
		}
	}
	gs.maybeCloseJoinBarrier(group)
}

func (gs *GroupStore) maybeCloseJoinBarrier(group *GroupMeta) {
	if group.State != constant.GroupStatePreparingRebalance {
		return
	}

	if len(group.pendingJoins) != len(group.members) {
		return
	}

	group.LeaderId = group.pendingJoins[0].memberId // todo: select leader by protocol type and member id
	group.ProtocolName = constant.JoinGroupProtocolNameRoundrobin
	group.State = constant.GroupStateCompletingRebalance

	if group.rebalanceTimer != nil {
		group.rebalanceTimer.Stop()
	}

	allMembers := make([]JoinMember, 0, len(group.members))

	for _, member := range group.members {
		allMembers = append(allMembers, JoinMember{
			MemberId:        member.MemberId,
			GroupInstanceId: member.GroupInstanceId,
			Metadata:        member.Metadata,
		})
	}

	for _, pj := range group.pendingJoins {
		result := joinResult{
			GenerationId: group.GenerationId,
			LeaderId:     group.LeaderId,
			ProtocolName: group.ProtocolName,
			Err:          nil,
		}

		if pj.memberId == group.LeaderId {
			result.Members = allMembers
		}

		pj.resultCh <- result
	}

	group.pendingJoins = nil
}
func (gs *GroupStore) maxRebalanceTimeout(group *GroupMeta) time.Duration {
	members := group.members
	//todo: add to config
	maxTimeOut := 3 * time.Second

	for _, mem := range members {
		if mem.RebalanceTimeout > maxTimeOut {
			maxTimeOut = mem.RebalanceTimeout
		}
	}
	return maxTimeOut
}

func (gs *GroupStore) CommitOffsets(groupId string, commits []CommitOffset) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	partitions, ok := gs.offsets[groupId]
	if !ok {
		partitions = make(map[string]*OffsetPartition)
		gs.offsets[groupId] = partitions
	}

	for _, c := range commits {
		key := offsetKey(c.TopicName, c.PartitionIdx)
		partitions[key] = &OffsetPartition{
			TopicName:           c.TopicName,
			PartitionIdx:        c.PartitionIdx,
			Offset:              c.Offset,
			LeaderEpoch:         c.LeaderEpoch,
			Metadata:            c.Metadata,
			LastCommitTimestamp: time.Now(),
		}
	}
}

func (gs *GroupStore) FetchOffsets(groupId string, queries []FetchOffsetQuery) []FetchOffsetResult {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	results := make([]FetchOffsetResult, 0, len(queries))
	partitions := gs.offsets[groupId] // nil if group has no committed offsets

	for _, q := range queries {
		result := FetchOffsetResult{
			TopicName:    q.TopicName,
			PartitionIdx: q.PartitionIdx,
			Offset:       -1, // -1 = no committed offset
			LeaderEpoch:  -1,
		}
		if partitions != nil {
			if op, ok := partitions[offsetKey(q.TopicName, q.PartitionIdx)]; ok {
				result.Offset = op.Offset
				result.LeaderEpoch = op.LeaderEpoch
				result.Metadata = op.Metadata
			}
		}
		results = append(results, result)
	}
	return results
}

func (gs *GroupStore) scheduleGroupCleanup(groupId string) {
	time.AfterFunc(5*time.Minute, func() {
		gs.mu.Lock()
		defer gs.mu.Unlock()
		group, exists := gs.groups[groupId]
		if exists && group.State == constant.GroupStateDead {
			delete(gs.groups, groupId)
		}
	})
}
