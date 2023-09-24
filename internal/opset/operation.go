package opset

import (
	"fmt"
	"github.com/google/uuid"
)

type Operation struct {
	OpId    *OpIdWithValid `json:"OpId"`
	ObjId   OpId           `json:"ObjId"`
	Prop    any            `json:"Prop"`
	Action  ActionType     `json:"Action"`
	Value   interface{}    `json:"Value"`
	MovedID *OpId          `json:"MovedID"`
	MoveSrc *OpId          `json:"MoveSrc"`
	Pred    []OpId         `json:"Pred"`
	Succ    []OpId         `json:"Succ"`
	Insert  bool           `json:"Insert"`
}

type ExOperation struct {
	OpId    ExOpId      `json:"OpId"`
	ObjId   ExOpId      `json:"ObjId"`
	Prop    any         `json:"Prop"`
	Action  ActionType  `json:"Action"`
	Value   interface{} `json:"Value"`
	MovedID *ExOpId     `json:"MovedID"`
	MoveSrc *ExOpId     `json:"MoveSrc"`
	Pred    []ExOpId    `json:"Pred"`
	Succ    []ExOpId    `json:"Succ"`
	Insert  bool        `json:"Insert"`
}

func (op *ExOperation) ToOp(s *OpSet) *Operation {
	opId := op.OpId.ToOpId(s)
	objId := op.ObjId.ToOpId(s)
	moveID := op.MovedID.ToOpId(s)
	moveSrc := op.MoveSrc.ToOpId(s)
	pred := make([]OpId, 0)
	for _, p := range op.Pred {
		pred = append(pred, *p.ToOpId(s))
	}
	succ := make([]OpId, 0)
	for _, successor := range op.Succ {
		succ = append(succ, *successor.ToOpId(s))
	}

	newOp := &Operation{
		OpId:    NewOpIdWithValid(*opId),
		ObjId:   *objId,
		Prop:    op.Prop,
		Action:  op.Action,
		Value:   op.Value,
		MovedID: moveID,
		MoveSrc: moveSrc,
		Pred:    pred,
		Succ:    succ,
		Insert:  op.Insert,
	}
	if prop, ok := op.Prop.(ExOpId); ok {
		newOp.Prop = *prop.ToOpId(s)
	}
	if value, ok := op.Value.(ExOpId); ok {
		newOp.Value = *value.ToOpId(s)
	}
	return newOp
}

func (op *Operation) ToExOp(s *OpSet) *ExOperation {
	opId := op.OpId.Id.ToExOpId(s)
	objId := op.ObjId.ToExOpId(s)
	moveID := op.MovedID.ToExOpId(s)
	moveSrc := op.MoveSrc.ToExOpId(s)
	pred := make([]ExOpId, 0)
	for _, p := range op.Pred {
		pred = append(pred, *p.ToExOpId(s))
	}
	succ := make([]ExOpId, 0)
	for _, successor := range op.Succ {
		succ = append(succ, *successor.ToExOpId(s))
	}
	newOp := &ExOperation{
		OpId:    *opId,
		ObjId:   *objId,
		Prop:    op.Prop,
		Action:  op.Action,
		Value:   op.Value,
		MovedID: moveID,
		MoveSrc: moveSrc,
		Pred:    pred,
		Succ:    succ,
		Insert:  op.Insert,
	}
	if prop, ok := op.Prop.(OpId); ok {
		newOp.Prop = *prop.ToExOpId(s)
	}
	if value, ok := op.Value.(OpId); ok {
		newOp.Value = *value.ToExOpId(s)
	}
	return newOp
}

func (op *Operation) isMoveVisible(moveManager *MoveManager) bool {
	if op.Action == DELETE {
		return false
	}
	// if it is valid, and all successors are invalid, then it is visible
	if op.OpId.Valid {
		for _, succ := range op.Succ {
			if moveManager.IsValid(succ) {
				return false
			}
		}
		return true
	} else {
		return false
	}
}

func (op *Operation) isVisible(moveManager *MoveManager) bool {
	if MoveEnabled {
		return op.isMoveVisible(moveManager)
	} else {
		if op.Action == DELETE {
			return false
		}
		return len(op.Succ) == 0
	}
}

func (op *Operation) addSuccessor(succ OpId) {
	for _, s := range op.Succ {
		if s == succ {
			return
		}
	}
	op.Succ = append(op.Succ, succ)
}

func (op *Operation) overwrites(other *Operation) bool {
	for _, pred := range op.Pred {
		if pred == other.OpId.Id {
			return true
		}
	}
	return false
}

func (op *Operation) isMovingScalar() bool {
	if op.Action == MOVE && op.Value != nil {
		return true
	} else {
		return false
	}
}
func operationVisualizeHeader() string {
	return "<tr><td>OpId</td><td>ObjId</td><td>Prop</td><td>Action</td><td>Value</td><td>MovedID</td><td>MoveFrom</td><td>Pred</td><td>Succ</td><td>Insert</td></tr>"
}
func (op *Operation) visualize() string {
	var value string
	var movedObjId any
	var moveFrom any

	if op.Value != nil {
		value = fmt.Sprintf("%v", op.Value)
	} else {
		value = ""
	}

	if op.MovedID != nil {
		movedObjId = *op.MovedID
	} else {
		movedObjId = ""
	}

	if op.MoveSrc != nil {
		moveFrom = *op.MoveSrc
	} else {
		moveFrom = ""
	}

	res := fmt.Sprintf("<td>%v</td><td>%v</td><td>%v</td><td>%v</td><td>%v</td><td>%v</td><td>%v</td><td>%v</td><td>%v</td><td>%v</td>", op.OpId, op.ObjId, op.Prop, op.Action, value, movedObjId, moveFrom, op.Pred, op.Succ, op.Insert)
	return res
}

func covertToExOpId(jsonMap map[string]any) ExOpId {
	actorIdStr := jsonMap["ActorId"].(string)
	actorId, _ := uuid.Parse(actorIdStr)
	counter := uint64(jsonMap["Counter"].(float64))
	return ExOpId{actorId, counter}
}

func (op *ExOperation) Convert() {
	if value, ok := op.Prop.(map[string]any); ok {
		op.Prop = covertToExOpId(value)
	}
}

func (op *Operation) String() string {
	result := fmt.Sprintf("{%v}: ", op.OpId.Id.String())
	switch op.Action {
	case MAKE:
		{
			result += "Make "
			if value, ok := op.Value.(float64); ok {
				result += fmt.Sprintf("%v", (ObjType)(value))
			} else {
				result += fmt.Sprintf("%v", op.Value)
			}
			result += fmt.Sprintf(" object %v at %v", op.OpId.Id.String(), op.ObjId.String())
			if len(op.Pred) > 0 {
				result += fmt.Sprintf(", overwrites %v", op.Pred)
			}
			return result
		}
	case MOVE:
		{
			if p, ok := op.Prop.(OpId); !ok {
				result += fmt.Sprintf("Move %v to be a child of %v, from %v, rename/put it to %v", op.MovedID, &op.ObjId, op.MoveSrc, op.Prop)
			} else {
				result += fmt.Sprintf("Move %v to be a child of %v, from %v, rename/put it to %v", op.MovedID, &op.ObjId, op.MoveSrc, p.String())
			}
			if len(op.Pred) > 0 {
				result += fmt.Sprintf(", overwrites %v", op.Pred)
			}
			return result
		}
	case DELETE:
		{
			result += fmt.Sprintf("Delete %v", op.Pred)
			return result
		}
	case PUT:
		{
			if p, ok := op.Prop.(OpId); ok {
				result += fmt.Sprintf("Put %v at %v.%v", op.Value, op.ObjId.String(), p.String())
			} else {
				result += fmt.Sprintf("Put %v at %v.%v", op.Value, op.ObjId.String(), op.Prop)
			}
			return result
		}
	default:
		return "Unknown"
	}
}
