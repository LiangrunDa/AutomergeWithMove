package opset

import (
	"fmt"
	"github.com/google/uuid"
)

var ExRootOpId = ExOpId{ActorId: uuid.UUID{}, Counter: 0}
var ExNullOpId = ExOpId{ActorId: uuid.UUID{}, Counter: ^uint64(0)} // used by movemanager
var RootOpId = OpId{ActorId: 0, Counter: 0}
var NullOpId = OpId{ActorId: 0, Counter: ^uint64(0)} // used by movemanager

type OpId struct {
	ActorId uint   `json:"ActorId"`
	Counter uint64 `Counter:"Counter"`
}

type OpIdWithValid struct {
	Id    OpId
	Valid bool
}

type ExOpId struct {
	ActorId uuid.UUID `json:"ActorId"`
	Counter uint64    `json:"Counter"`
}

func NewOpIdWithValid(id OpId) *OpIdWithValid {
	return &OpIdWithValid{Id: id, Valid: true}
}

func (ex *ExOpId) ToOpId(s *OpSet) *OpId {
	if ex == nil {
		return nil
	}
	return &OpId{
		ActorId: s.GetIdx(ex.ActorId),
		Counter: ex.Counter,
	}
}

func (opId *OpId) ToExOpId(s *OpSet) *ExOpId {
	if opId == nil {
		return nil
	}
	return &ExOpId{
		ActorId: s.GetActorId(opId.ActorId),
		Counter: opId.Counter,
	}
}

func (opId *OpId) increment() OpId {
	opId.Counter++
	return *opId
}

func (opId *OpId) GreaterThan(s *OpSet, opId2 *OpId) bool {
	if opId.Counter > opId2.Counter {
		return true
	} else if opId.Counter == opId2.Counter {
		return s.GetActorId(opId.ActorId).String() > s.GetActorId(opId2.ActorId).String()
	} else {
		return false
	}
}

func (opId *OpId) GreaterThanOrEqual(s *OpSet, opId2 *OpId) bool {
	return opId.GreaterThan(s, opId2) || opId == opId2
}

func (opId *OpId) String() string {
	return fmt.Sprintf("%v@%v", opId.Counter, opId.ActorId)
}

func (ex *ExOpId) EqualsTo(other *ExOpId) bool {
	return ex.ActorId == other.ActorId && ex.Counter == other.Counter
}
