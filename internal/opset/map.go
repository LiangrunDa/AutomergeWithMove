package opset

import (
	"container/list"
	"github.com/LiangrunDa/AutomergeWithMove/errors"
	"github.com/sirupsen/logrus"
)

type ReplicatedMap interface {
	MapGet(prop string) (any, error)
	MapPut(prop string, value any) error
	MapDelete(prop string) error
}

// Return the first operation matching the prop.
// If no operations is matched, return the position where the operation should be inserted
func (opt *OpTree) findStart(prop string) (int, *list.Element) {
	pos := 0
	for element := opt.operations.Front(); element != nil; element = element.Next() {
		if operation, ok := element.Value.(*Operation); ok {
			strProp := operation.Prop.(string)
			if strProp >= prop {
				return pos, element
			}
		} else {
			panic("element is not an operation")
		}
		pos++
	}
	return pos, nil
}

func (opt *OpTree) moveSearch(prop string) (OpId, []*Operation, any) {
	var targetObjId OpId
	var pred []*Operation
	var scalarValue any
	pos, start := opt.findStart(prop)

	for element := start; element != nil; element = element.Next() {
		if operation, ok := element.Value.(*Operation); ok {
			if operation.Prop != prop {
				return targetObjId, pred, scalarValue
			}
			if operation.isVisible(opt.ops.moveManager) {
				pred = append(pred, operation)
			} else {
				continue
			}
			if operation.Action == MAKE {
				targetObjId = operation.OpId.Id
			} else if operation.Action == PUT {
				scalarValue = operation.Value
				targetObjId = operation.OpId.Id
			} else if operation.Action == MOVE {
				targetObjId = *operation.MovedID
				if operation.Value != nil {
					scalarValue = operation.Value
				}
			}
			pos++
		} else {
			panic("element is not an operation")
		}
	}

	return targetObjId, pred, scalarValue
}

func (opt *OpTree) debugSrcNotFound(op *Operation) {
	logrus.Errorf("%v", op)
	selfValid := opt.ops.moveManager.IsValid(op.OpId.Id)
	logrus.Errorf("selfValid: %v", selfValid)
	for _, pred := range op.Pred {
		logrus.Errorf("predValid %v: %v", pred, opt.ops.moveManager.IsValid(pred))
	}

}

func (opt *OpTree) search(prop string) ([]*Operation, int) {
	var result []*Operation

	pos, start := opt.findStart(prop)

	for element := start; element != nil; element = element.Next() {
		if operation, ok := element.Value.(*Operation); ok {
			if operation.Prop != prop {
				return result, pos
			}

			if operation.isVisible(opt.ops.moveManager) {
				result = append(result, operation)
			}
			pos++
		} else {
			panic("element is not an operation")
		}
	}
	return result, pos
}

func (opt *OpTree) MapPut(prop string, value interface{}) {
	operations, lastIdx := opt.search(prop)
	var pred []OpId
	for _, operation := range operations {
		pred = append(pred, operation.OpId.Id)
	}
	localOp := Operation{
		OpId:   NewOpIdWithValid(opt.lamportClock.increment()),
		ObjId:  opt.ObjId,
		Prop:   prop,
		Action: PUT,
		Value:  value,
		Pred:   pred,
		Succ:   []OpId{},
		Insert: false,
	}

	for _, op := range operations {
		op.addSuccessor(localOp.OpId.Id)
	}
	opt.insertOp(&localOp, lastIdx)
}

func (opt *OpTree) MapGet(prop string) (any, error) {
	operations, _ := opt.search(prop)

	if len(operations) == 0 {
		return nil, errors.PropertyNotFoundError{PropertyName: prop}
	}

	last := operations[len(operations)-1]
	if last.Action == MAKE {
		return last.OpId, nil
	} else if last.Action == MOVE {
		if last.Value != nil {
			// Move a scalar value
			return last.Value, nil
		} else {
			// move an object
			return *last.MovedID, nil
		}
	} else {
		return last.Value, nil
	}
}

func (opt *OpTree) MapDelete(prop string) error {
	operations, lastIdx := opt.search(prop)
	if len(operations) == 0 {
		return errors.PropertyNotFoundError{PropertyName: prop}
	}
	var pred []OpId
	for _, operation := range operations {
		pred = append(pred, operation.OpId.Id)
	}
	newClock := opt.lamportClock.increment()
	localOp := Operation{
		OpId:   NewOpIdWithValid(newClock),
		ObjId:  opt.ObjId,
		Prop:   prop,
		Action: DELETE,
		Value:  nil,
		Pred:   pred,
		Succ:   []OpId{},
		Insert: false,
	}
	for _, op := range operations {
		op.addSuccessor(newClock)
	}
	opt.insertOp(&localOp, lastIdx)
	return nil
}

func (opt *OpTree) MapSeekOperation(seekOp *Operation) (int, bool, []*Operation) {
	pos, start := opt.findStart(seekOp.Prop.(string))
	pred := []*Operation{}
	found := false
	for element := start; element != nil; element = element.Next() {
		if operation, ok := element.Value.(*Operation); ok {
			if operation.Prop != seekOp.Prop {
				return pos, found, pred
			}

			if seekOp.overwrites(operation) {
				pred = append(pred, operation)
			}

			if operation.OpId.Id.GreaterThanOrEqual(opt.ops, &seekOp.OpId.Id) {
				if operation.OpId == seekOp.OpId {
					found = true
				}
				return pos, found, pred
			}
			pos++
		} else {
			panic("element is not an operation")
		}
	}
	return pos, found, pred
}

func (opt *OpTree) UpdateSourcePredecessors(moveOp *Operation) {
	for element := opt.operations.Front(); element != nil; element = element.Next() {
		if operation, ok := element.Value.(*Operation); ok {
			if moveOp.overwrites(operation) {
				operation.Succ = append(operation.Succ, moveOp.OpId.Id)
			}
		} else {
			panic("element is not an operation")
		}
	}
}
