package transaction

import (
	"crypto/sha256"
	"encoding/json"
	"github.com/LiangrunDa/AutomergeWithMove/internal/opset"
	"github.com/google/uuid"
)

type Change struct {
	ActorId      uuid.UUID          `json:"ActorId"`
	Seq          uint32             `json:"Seq"`
	Dependencies []ChangeHash       `json:"Dependencies"`
	Operations   []*opset.Operation `json:"Operations"`
	StartOp      uint64             `json:"StartOp"`
	HashCache    ChangeHash         `json:"HashCache"`
}

type ExChange struct {
	ActorId      uuid.UUID            `json:"ActorId"`
	Seq          uint32               `json:"Seq"`
	Dependencies []ChangeHash         `json:"Dependencies"`
	Operations   []*opset.ExOperation `json:"Operations"`
	StartOp      uint64               `json:"StartOp"`
	HashCache    ChangeHash           `json:"HashCache"`
}

func NewChange(actorId uuid.UUID, seq uint32, operations []*opset.Operation, deps []ChangeHash, startOp uint64, s *opset.OpSet) *Change {
	change := &Change{
		ActorId:      actorId,
		Seq:          seq,
		Operations:   operations,
		Dependencies: deps,
		StartOp:      startOp,
		HashCache:    ChangeHash{},
	}
	change.HashCache = change.ComputeHash(s)
	return change
}

func (c *Change) ToExChange(s *opset.OpSet) ExChange {
	var exChange ExChange
	exChange.ActorId = c.ActorId
	exChange.Seq = c.Seq
	exChange.Dependencies = c.Dependencies
	exChange.StartOp = c.StartOp
	exChange.HashCache = c.HashCache
	for _, op := range c.Operations {
		exChange.Operations = append(exChange.Operations, op.ToExOp(s))
	}
	return exChange
}

func (c *Change) FromExChange(exChange ExChange, s *opset.OpSet) {
	c.ActorId = exChange.ActorId
	c.Seq = exChange.Seq
	c.Dependencies = exChange.Dependencies
	c.StartOp = exChange.StartOp
	c.HashCache = exChange.HashCache
	for _, op := range exChange.Operations {
		c.Operations = append(c.Operations, op.ToOp(s))
	}
}

func (c *Change) ToBytes(s *opset.OpSet) []byte {
	exChange := c.ToExChange(s)
	bytes, err := json.Marshal(exChange)
	if err == nil {
		return bytes
	} else {
		return nil
	}
}

func (c *Change) FromBytes(bytes []byte, s *opset.OpSet) error {
	var exChange ExChange
	err := json.Unmarshal(bytes, &exChange)
	if err == nil {
		c.FromExChange(exChange, s)
		return nil
	} else {
		return err
	}
}

func (c *Change) ComputeHash(s *opset.OpSet) ChangeHash {
	hash := sha256.Sum256(c.ToBytes(s))
	return hash
}

func (c *Change) Hash() ChangeHash {
	return c.HashCache
}

type ChangeArray []*Change
type ExChangeArray []*ExChange

func NewChangeArrayFromBytes(bytes []byte, s *opset.OpSet) ChangeArray {
	var exChanges ExChangeArray
	var changes ChangeArray
	err := json.Unmarshal(bytes, &exChanges)
	if err == nil {
		for _, exChange := range exChanges {
			var change Change
			for j := 0; j < len(exChange.Operations); j++ {
				exChange.Operations[j].Convert()
			}
			change.FromExChange(*exChange, s)
			changes = append(changes, &change)
		}
	} else {
		return nil
	}
	return changes
}

func NewChangeFromBytes(bytes []byte, s *opset.OpSet) *Change {
	var exChange ExChange
	var change Change
	err := json.Unmarshal(bytes, &exChange)
	if err == nil {
		for j := 0; j < len(exChange.Operations); j++ {
			exChange.Operations[j].Convert()
		}
		change.FromExChange(exChange, s)
		return &change
	} else {
		return nil
	}
}
