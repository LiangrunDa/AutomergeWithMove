package opset

import (
	"container/list"
	"fmt"
	"github.com/google/uuid"
)

type OperationTree interface {
	ReplicatedMap
	ReplicatedList
}

type ObjType uint8

const (
	LIST ObjType = iota
	MAP
)

func (ot ObjType) String() string {
	switch ot {
	case LIST:
		return "LIST"
	case MAP:
		return "MAP"
	default:
		return ""
	}
}

type OpTree struct {
	operations   *list.List
	lamportClock *OpId
	Type         ObjType
	ObjId        OpId
	actorId      uuid.UUID
	ops          *OpSet
}

func NewOpTree(actorId uuid.UUID, lamportClock *OpId, objType ObjType, ObjId OpId, ops *OpSet) *OpTree {
	return &OpTree{
		actorId:      actorId,
		operations:   list.New(),
		lamportClock: lamportClock,
		Type:         objType,
		ObjId:        ObjId,
		ops:          ops,
	}
}

func (opt *OpTree) insertOpWithValidityUpdate(op *Operation, index int, update bool) {

	var elementAtIndex *list.Element
	if index < opt.operations.Len() {
		elementAtIndex = opt.operations.Front()
		for i := 0; i < index; i++ {
			elementAtIndex = elementAtIndex.Next()
		}
	} else {
		elementAtIndex = nil
	}

	if elementAtIndex != nil {
		opt.operations.InsertBefore(op, elementAtIndex)
	} else {
		opt.operations.PushBack(op)
	}

	if MoveEnabled && update {
		opt.ops.moveManager.UpdateValidity(op)
	}
	opt.ops.lastOperation = op
}

func (opt *OpTree) insertOp(op *Operation, index int) {
	opt.insertOpWithValidityUpdate(op, index, true)
}

func (opt *OpTree) visualize() string {

	htmlLabel := fmt.Sprintf("<<table border='1' cellspacing='0'>%v", operationVisualizeHeader())

	for element := opt.operations.Front(); element != nil; element = element.Next() {
		if row, ok := element.Value.(*Operation); ok {
			htmlLabel += fmt.Sprintf("<tr>%v</tr>", row.visualize())
		} else {
			panic("element is not an operation")
		}
	}
	htmlLabel += "</table>>"

	return htmlLabel
}
