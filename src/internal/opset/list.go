package opset

import (
	"container/list"
	errors "github.com/LiangrunDa/AutomergeWithMove/errors"
)

type ReplicatedList interface {
	ListGet(index int) (any, error)
	ListInsert(index int, values any) error
	ListDelete(index int) error
	ListPut(index int, value any) error
}

func (opt *OpTree) moveNth(index int) ([]*Operation, OpId, any) {
	var lastSeen *Operation
	seen := 0
	var res []*Operation
	var targetObjId OpId
	var scalarValue any

	for operation := opt.operations.Front(); operation != nil; operation = operation.Next() {
		if op, ok := operation.Value.(*Operation); ok {
			if op.Insert {
				if seen > index {
					return res, targetObjId, scalarValue
				}
				lastSeen = nil
			}
			if op.isVisible(opt.ops.moveManager) && lastSeen == nil {
				seen += 1
				lastSeen = op
			}
			if op.isVisible(opt.ops.moveManager) && seen > index {
				res = append(res, op)
				if op.Action == MAKE {
					targetObjId = op.OpId.Id
				} else if op.Action == PUT {
					targetObjId = op.OpId.Id
					scalarValue = op.Value
				} else if op.Action == MOVE {
					targetObjId = *op.MovedID
					if op.Value != nil {
						scalarValue = op.Value
					}
				}
			}
		} else {
			panic("element is not an operation")
		}
	}
	return res, targetObjId, scalarValue
}

func (opt *OpTree) nth(index int) ([]*Operation, int) {
	var lastSeen *Operation
	seen := 0
	pos := 0
	var res []*Operation

	for operation := opt.operations.Front(); operation != nil; operation = operation.Next() {
		if op, ok := operation.Value.(*Operation); ok {
			if op.Insert {
				if seen > index {
					return res, pos
				}
				lastSeen = nil
			}
			if op.isVisible(opt.ops.moveManager) && lastSeen == nil {
				seen += 1
				lastSeen = op
			}
			if op.isVisible(opt.ops.moveManager) && seen > index {
				res = append(res, op)
			}
			pos++
		} else {
			panic("element is not an operation")
		}
	}
	return res, pos
}

func (opt *OpTree) ListGet(index int) (any, error) {
	operations, _ := opt.nth(index)
	if len(operations) == 0 {
		return nil, errors.ListIndexExceedsLengthError{Index: index}
	}
	last := operations[len(operations)-1]
	if last.Action == MAKE {
		return last.OpId, nil
	} else if last.Action == MOVE {
		if last.Value != nil {
			return last.Value, nil
		} else {
			return *last.MovedID, nil
		}
	} else if last.Action == PUT {
		return last.Value, nil
	} else {
		return nil, errors.UnknownError{}
	}
}

func (opt *OpTree) insertNth(index int) (int, OpId, error) {
	insertRowNumber := -1
	var insertProp OpId
	var lastSeen *Operation
	seen := 0 // number of elements we've ever seen
	pos := 0  // row number we're currently at
	found := false
	var lastOperation *list.Element

	for operation := opt.operations.Front(); operation != nil; operation = operation.Next() {
		lastOperation = operation
		if operation, ok := operation.Value.(*Operation); ok {
			// if we find the insert operation at `index`, we will insert the new operation before it
			if operation.Insert {
				if insertRowNumber == -1 && seen >= index {
					insertRowNumber = pos
					found = true
				}
			}

			// if we find an insert operation, then we potentially find an element
			// 1. if it is invisible, and no following put operation is visible, then we don't find an element
			// 2. if it is visible, then we find an element
			// 3. if it is invisible, but a following put operation is visible, then we find an element
			if operation.Insert {
				lastSeen = nil
			}

			// if we find an operation (could be an insert op, or a following put op) that is visible, then we find an element
			if operation.isVisible(opt.ops.moveManager) && lastSeen == nil {
				// if we already find the insert location, simply return the result
				if found {
					return insertRowNumber, insertProp, nil
				}
				// update number of elements we've ever seen
				seen += 1
				// update the last seen operation
				lastSeen = operation
				if operation.Insert {
					insertProp = operation.OpId.Id
				} else {
					if opId, ok := operation.Prop.(OpId); ok {
						insertProp = opId
					} else {
						panic("operation Value is not an OpId")
					}
				}
			}
			pos++
		} else {
			panic("element is not an operation")
		}
	}

	// if we don't find an operation at `index`, we need to insert it at the end of the optree
	if lastOperation == nil {
		// the list is empty, the property is <0, 0> which is the virtual head
		return pos, OpId{}, nil
	} else {
		// the list is not empty
		return pos, insertProp, nil
	}
}

func (opt *OpTree) ListInsert(index int, value any) {
	insertRowNumber, insertProp, _ := opt.insertNth(index)
	localOp := Operation{
		OpId:   NewOpIdWithValid(opt.lamportClock.increment()),
		ObjId:  opt.ObjId,
		Prop:   insertProp,
		Action: PUT,
		Value:  value,
		Pred:   []OpId{},
		Succ:   []OpId{},
		Insert: true,
	}
	opt.insertOp(&localOp, insertRowNumber)
}

func (opt *OpTree) ListPut(index int, value any) error {
	operations, ops := opt.nth(index)
	if len(operations) == 0 {
		return errors.ListIndexExceedsLengthError{
			Index: index,
		}
	}
	first := operations[0]
	var prop OpId
	if first.Insert {
		prop = first.OpId.Id
	} else {
		if opId, ok := first.Prop.(OpId); ok {
			prop = opId
		} else {
			panic("operation Value is not an OpId")
		}
	}
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

	for _, operation := range operations {
		operation.addSuccessor(localOp.OpId.Id)
	}
	opt.insertOp(&localOp, ops)
	return nil
}

func (opt *OpTree) ListDelete(index int) error {
	operations, pos := opt.nth(index)
	if len(operations) == 0 {
		return errors.ListIndexExceedsLengthError{Index: index}
	}

	first := operations[0]
	var prop OpId
	if first.Insert {
		prop = first.OpId.Id
	} else {
		if opId, ok := first.Prop.(OpId); ok {
			prop = opId
		} else {
			panic("operation Value is not an OpId")
		}
	}
	var pred []OpId
	for _, operation := range operations {
		pred = append(pred, operation.OpId.Id)
	}

	localOp := Operation{
		OpId:   NewOpIdWithValid(opt.lamportClock.increment()),
		ObjId:  opt.ObjId,
		Prop:   prop,
		Action: DELETE,
		Value:  nil,
		Pred:   pred,
		Succ:   []OpId{},
		Insert: false,
	}

	for _, operation := range operations {
		operation.addSuccessor(localOp.OpId.Id)
	}
	opt.insertOp(&localOp, pos)
	return nil
}

func (opt *OpTree) ListSeekOperation(seekOp *Operation) (int, bool, []*Operation) {
	pos := 0
	foundTarget := false
	found := false
	pred := []*Operation{}
	seekOpId := seekOp.Prop.(OpId)

	if seekOpId == RootOpId {
		foundTarget = true
	}
	for operation := opt.operations.Front(); operation != nil; operation = operation.Next() {
		if op, ok := operation.Value.(*Operation); ok {
			if !foundTarget { // find target parent of the RGA tree
				if op.Insert && op.OpId.Id == seekOpId {
					foundTarget = true
					if seekOp.overwrites(op) {
						pred = append(pred, op)
					}
				}
			} else { // if found target, find the position to insert the new operation
				if seekOp.overwrites(op) {
					pred = append(pred, op)
				}
				if seekOp.Insert { // if the operation is an Insert, we should find the first operation whose OpId is less than the seekOp's
					if op.Insert && seekOp.OpId.Id.GreaterThanOrEqual(opt.ops, &op.OpId.Id) {
						if seekOp.OpId == op.OpId {
							found = true
						}
						return pos, found, pred // insert the new operation at pos
					}
				} else if op.Insert || op.OpId.Id.GreaterThan(opt.ops, &seekOp.OpId.Id) {
					// if the operation is a Put, we should find
					// 1. the first insert operation after target
					// 2. or the first put operation has a greater OpId than the seekOp's
					if seekOp.OpId == op.OpId {
						found = true
					}
					return pos, found, pred // insert the new operation at pos
				}
			}
		} else {
			panic("element is not an operation")
		}
		pos++
	}
	return pos, found, pred
}
