package opset

import (
	"github.com/LiangrunDa/AutomergeWithMove/internal/log"
	stack "github.com/golang-collections/collections/stack"
	"github.com/sirupsen/logrus"
	"sort"
)

type LogEntry struct {
	op *Operation
}

type tempEntry struct {
	entry LogEntry
	isNew bool
}

type MoveManager struct {
	opLog       *stack.Stack // element: LogEntry
	valid       map[OpId]bool
	tree        *DocumentTree
	winners     map[OpId]*stack.Stack
	moveIDMap   map[OpId]OpId
	lifecycles  map[OpId]*LifeCycleList // key: object ID, value: its lifecycle
	moveParents *stack.Stack            // element: OpId
	ops         *OpSet
}

func NewMoveManager(ops *OpSet) *MoveManager {
	lifecycles := make(map[OpId]*LifeCycleList)
	moveManager := &MoveManager{
		opLog:       stack.New(),
		tree:        NewDocumentTree(lifecycles),
		winners:     make(map[OpId]*stack.Stack),
		moveParents: stack.New(),
		valid:       make(map[OpId]bool),
		moveIDMap:   make(map[OpId]OpId),
		lifecycles:  lifecycles,
		ops:         ops,
	}
	lifecycles[RootOpId] = NewLifeCycleList(NewOpIdWithValid(RootOpId), ops)
	moveManager.tree.add(RootOpId, RootOpId) // root points to itself
	return moveManager
}

func (m *MoveManager) updateLifecycle(operation *Operation) {
	for _, pred := range operation.Pred {
		predObjID := pred
		if moveID, ok := m.moveIDMap[pred]; ok {
			predObjID = moveID
		}
		mid := operation.MovedID
		if mid != nil {
			// new operation is move
			if *mid == predObjID || !m.IsValid(pred) {
				continue
			}
		}
		if !m.IsValid(predObjID) {
			continue
		}
		if lst, ok2 := m.lifecycles[pred]; ok2 {
			lst.insertTrash(operation.OpId)
		} else {
			panic("pred lifecycle not found")
		}
	}
	if operation.Action == MAKE || operation.Action == PUT {
		m.tree.updateParentAs(operation.OpId.Id, operation.ObjId)
		m.tree.updateProperty(operation.OpId.Id, operation.Prop)
		m.lifecycles[operation.OpId.Id] = NewLifeCycleList(operation.OpId, m.ops)
	} else if operation.Action == MOVE {
		m.lifecycles[*operation.MovedID].insertPresent(operation.OpId)
	}
}

func (m *MoveManager) apply(operation *Operation) {
	log.MinimalTracef("tree:\n %s", m.tree.String)
	log.MinimalTracef("Applying %s", operation.String)
	logEntry := LogEntry{
		op: operation,
	}
	if operation.Action == MOVE {
		m.moveIDMap[operation.OpId.Id] = *operation.MovedID
		m.opLog.Push(logEntry)
		mid := *operation.MovedID
		oid := operation.ObjId
		if _, ok := m.winners[mid]; !ok {
			m.winners[mid] = stack.New()
		}
		if m.tree.isAncestorOf(operation.OpId, mid, oid) {
			m.setValid(operation.OpId, false, "It introduces a cycle")
			return
		}
		m.setValid(operation.OpId, true, "It is the winner")
		m.moveParents.Push(m.tree.getParent(mid))
		m.tree.updateParentAs(mid, oid)
		m.tree.updateProperty(mid, operation.Prop)
		prevMove := m.winners[mid].Peek()
		if prevMove != nil {
			temp := prevMove.(*OpIdWithValid)
			m.setValid(temp, false, "It is not the winner anymore")
		}
		m.winners[mid].Push(operation.OpId)
	}
}

func (m *MoveManager) revert(logEntry LogEntry) {
	log.MinimalTracef("tree:\n %s", m.tree.String)
	log.MinimalTracef("Reverting %s", logEntry.op.String)
	if logEntry.op.Action == MOVE && logEntry.op.OpId.Valid {
		moveStack := m.winners[*logEntry.op.MovedID]
		moveStack.Pop()
		// if it is not empty
		if moveStack.Len() != 0 {
			prevMove := moveStack.Peek().(*OpIdWithValid)
			m.setValid(prevMove, true, "It is the winner again")
		}
		oldParent := m.moveParents.Pop().(OpId)
		m.tree.updateParentAs(*logEntry.op.MovedID, oldParent)
		m.tree.updateProperty(*logEntry.op.MovedID, logEntry.op.Prop)
	}
}

func (m *MoveManager) BulkUpdateValidity(operations []*Operation) {
	// sort by opId in ascending order
	sort.Slice(operations, func(i, j int) bool {
		return !operations[i].OpId.Id.GreaterThan(m.ops, &operations[j].OpId.Id)
	})
	currOp := len(operations) - 1
	tempStack := stack.New()
	// undo
	for m.opLog.Len() > 0 && currOp >= 0 {
		logEntry := m.opLog.Peek().(LogEntry)
		if logEntry.op.OpId.Id.GreaterThan(m.ops, &operations[currOp].OpId.Id) {
			tempStack.Push(tempEntry{entry: m.opLog.Pop().(LogEntry), isNew: false})
			m.revert(logEntry)
		} else {
			tempStack.Push(tempEntry{
				entry: LogEntry{
					op: operations[currOp],
				},
				isNew: true,
			})
			currOp--
		}
	}
	for currOp >= 0 {
		tempStack.Push(tempEntry{
			entry: LogEntry{
				op: operations[currOp],
			},
			isNew: true,
		})
		currOp--
	}

	// bulk do & redo
	for tempStack.Len() > 0 {
		logEntry := tempStack.Pop().(tempEntry)
		m.apply(logEntry.entry.op)
		if logEntry.isNew {
			m.updateLifecycle(logEntry.entry.op)
		}
	}

}

func (m *MoveManager) UpdateValidity(operation *Operation) {
	log.MinimalTracef("UpdateValidity: %s", operation.String)
	tempStack := stack.New()
	// undo
	for m.opLog.Len() > 0 {
		logEntry := m.opLog.Peek().(LogEntry)
		if logEntry.op.OpId.Id.GreaterThan(m.ops, &operation.OpId.Id) {
			tempStack.Push(m.opLog.Pop())
			m.revert(logEntry)
		} else {
			break
		}
	}
	// do & redo
	m.apply(operation)
	m.updateLifecycle(operation)
	for tempStack.Len() > 0 {
		logEntry := tempStack.Pop().(LogEntry)
		m.apply(logEntry.op)
	}
}

func (m *MoveManager) IsValid(opId OpId) bool {
	if value, ok := m.valid[opId]; ok {
		return value
	} else {
		return true // PUT, MAKE, DELETE
	}
}

func (m *MoveManager) Visualize() string {
	return m.tree.Visualize()
}

func (m *MoveManager) getParentAndProperty(id OpId) (OpId, any, error) {
	parent, err := m.tree.getPresentParent(id)
	if err != nil {
		return NullOpId, nil, err
	} else {
		return parent, m.tree.getProperty(id), nil
	}
}

func (m *MoveManager) setValid(id *OpIdWithValid, valid bool, reason string) {
	logrus.Debugf("Set %s as %v: %s", id.Id.String(), valid, reason)
	m.valid[id.Id] = valid
	id.Valid = valid
}
