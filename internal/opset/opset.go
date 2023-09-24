package opset

import (
	"fmt"
	"github.com/LiangrunDa/AutomergeWithMove/errors"
	"github.com/awalterschulze/gographviz"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"math/rand"
	"sort"
)

var MoveEnabled = true

type OpSet struct {
	actorId       uuid.UUID
	opTrees       map[OpId]*OpTree
	lamportClock  *OpId
	lastOperation *Operation
	moveManager   *MoveManager
	actorIds      []uuid.UUID
	actorIdMap    map[uuid.UUID]int
}

func NewOpSet(actorId uuid.UUID) *OpSet {
	opTrees := make(map[OpId]*OpTree)
	actorIds := make([]uuid.UUID, 0)
	actorIdMap := make(map[uuid.UUID]int)
	actorIds = append(actorIds, uuid.UUID{}) // root
	actorIdMap[uuid.UUID{}] = 0
	actorIds = append(actorIds, actorId) // self
	actorIdMap[actorId] = 1

	lamportClock := &OpId{1, 0}
	newSet := &OpSet{
		actorId:      actorId,
		opTrees:      opTrees,
		lamportClock: lamportClock,
		actorIds:     actorIds,
		actorIdMap:   actorIdMap,
	}
	newSet.moveManager = NewMoveManager(newSet)
	opTrees[RootOpId] = NewOpTree(actorId, lamportClock, MAP, RootOpId, newSet)
	return newSet
}

func (s *OpSet) GetDocumentTree() map[ExOpId]ExOpId {
	documentTree := make(map[ExOpId]ExOpId)
	for k, v := range s.moveManager.tree.parentMap {
		documentTree[*k.ToExOpId(s)] = *v.ToExOpId(s)
	}
	return documentTree
}

func (s *OpSet) GetIdx(actorId uuid.UUID) uint {
	if index, ok := s.actorIdMap[actorId]; ok {
		return uint(index)
	} else {
		s.actorIds = append(s.actorIds, actorId)
		s.actorIdMap[actorId] = len(s.actorIds) - 1
		return uint(len(s.actorIds) - 1)
	}
}

func (s *OpSet) GetActorId(index uint) uuid.UUID {
	return s.actorIds[index]
}

func (s *OpSet) MoveLogVisualize() string {
	return s.moveManager.Visualize()
}

func (s *OpSet) UpdateLamportClock(counter uint64) {
	s.lamportClock.Counter = counter
}
func (s *OpSet) GetLastOperation() *Operation {
	return s.lastOperation
}

func (s *OpSet) GetLamportClock() *OpId {
	return s.lamportClock
}

func (s *OpSet) Get(objId OpId, propertyOrIndex any) (any, error) {
	tree := s.opTrees[objId]
	if tree.Type == MAP {
		return tree.MapGet(propertyOrIndex.(string))
	} else {
		return tree.ListGet(propertyOrIndex.(int))
	}
}

func (s *OpSet) Put(objId OpId, propertyOrIndex any, value any) error {
	tree := s.opTrees[objId]
	if tree.Type == MAP {
		tree.MapPut(propertyOrIndex.(string), value)
		return nil
	} else {
		return tree.ListPut(propertyOrIndex.(int), value)
	}
}

func (s *OpSet) Delete(objId OpId, propertyOrIndex any) error {
	tree := s.opTrees[objId]
	if tree.Type == MAP {
		return tree.MapDelete(propertyOrIndex.(string))
	} else {
		return tree.ListDelete(propertyOrIndex.(int))
	}
}

func (s *OpSet) PutObject(objId OpId, property string, objType ObjType) (OpId, error) {
	tree := s.opTrees[objId]
	if tree.Type == MAP {
		operations, insertIdx := tree.search(property)
		pred := make([]OpId, 0)
		for _, operation := range operations {
			pred = append(pred, operation.OpId.Id)
		}
		newObjId := tree.lamportClock.increment()
		localOp := Operation{
			OpId:    NewOpIdWithValid(newObjId),
			ObjId:   tree.ObjId,
			Prop:    property,
			Action:  MAKE,
			Value:   objType,
			MovedID: nil,
			Pred:    pred,
			Succ:    make([]OpId, 0),
			Insert:  false,
		}

		for _, op := range operations {
			op.addSuccessor(localOp.OpId.Id)
		}

		tree.insertOp(&localOp, insertIdx)

		s.opTrees[newObjId] = NewOpTree(s.actorId, s.lamportClock, objType, newObjId, s)

		return newObjId, nil
	} else {
		return OpId{}, errors.InvalidOperationError{Reason: "cannot put object on a list"}
	}
}

func (s *OpSet) Insert(objId OpId, index int, value any) error {
	tree := s.opTrees[objId]
	if tree.Type == LIST {
		tree.ListInsert(index, value)
		return nil
	} else {
		return errors.InvalidOperationError{Reason: "cannot insert value on a map"}
	}
}

func (s *OpSet) InsertObject(objId OpId, index int, objType ObjType) (OpId, error) {
	tree := s.opTrees[objId]
	if tree.Type == LIST {
		if insertRowNumber, insertProp, err := tree.insertNth(index); err == nil {
			newObjId := tree.lamportClock.increment()
			localOp := Operation{
				OpId:   NewOpIdWithValid(newObjId),
				ObjId:  tree.ObjId,
				Prop:   insertProp,
				Action: MAKE,
				Value:  objType,
				Pred:   make([]OpId, 0),
				Succ:   make([]OpId, 0),
				Insert: true,
			}
			tree.insertOp(&localOp, insertRowNumber)
			s.opTrees[newObjId] = NewOpTree(s.actorId, s.lamportClock, objType, newObjId, s)
			return newObjId, nil
		} else {
			return OpId{}, err
		}
	} else {
		return OpId{}, errors.InvalidOperationError{Reason: "cannot insert object on a map"}
	}
}

func (s *OpSet) InsertOperation(operation *Operation) {
	tree := s.opTrees[operation.ObjId]
	if tree.Type == MAP {
		if pos, found, pred := tree.MapSeekOperation(operation); !found {

			tree.insertOpWithValidityUpdate(operation, pos, false)
			for _, op := range pred {
				op.addSuccessor(operation.OpId.Id)
			}

			if operation.Action == MAKE {
				objType := (ObjType)(operation.Value.(float64))
				s.opTrees[operation.OpId.Id] = NewOpTree(s.actorId, s.lamportClock, objType, operation.OpId.Id, s)
			} else if operation.Action == MOVE {
				//update source predecessors' successor field
				if srcTree, ok := s.opTrees[*operation.MoveSrc]; ok {
					srcTree.UpdateSourcePredecessors(operation)
				}
			}
		}
	} else {
		if pos, found, pred := tree.ListSeekOperation(operation); !found {
			tree.insertOpWithValidityUpdate(operation, pos, false)
			for _, op := range pred {
				op.addSuccessor(operation.OpId.Id)
			}
			if operation.Action == MAKE {
				objType := (ObjType)(operation.Value.(float64))
				s.opTrees[operation.OpId.Id] = NewOpTree(s.actorId, s.lamportClock, objType, operation.OpId.Id, s)
			} else if operation.Action == MOVE {
				//update source predecessors' successor field
				if srcTree, ok := s.opTrees[*operation.MoveSrc]; ok {
					srcTree.UpdateSourcePredecessors(operation)
				}
			}
		}
	}
}

func (s *OpSet) moveFromMap(srcTree *OpTree, property string) (ObjIdToBeMoved OpId, srcOps []*Operation, scalarValue any) {
	if ObjIdToBeMoved, srcOps, scalarValue = srcTree.moveSearch(property); len(srcOps) == 0 {
		return OpId{}, nil, nil
	} else {
		return ObjIdToBeMoved, srcOps, scalarValue
	}
}

func (s *OpSet) moveToMap(MovedFrom OpId, dstTree *OpTree, property string, ObjIdToBeMoved OpId, srcOps []*Operation, scalarValue any) {
	// if we move an object, scalar value is nil
	dstOps, insertIndex := dstTree.search(property)
	pred := make([]OpId, 0)
	for _, operation := range srcOps {
		pred = append(pred, operation.OpId.Id)
	}
	for _, operation := range dstOps {
		pred = append(pred, operation.OpId.Id)
	}

	localOp := Operation{
		OpId:    NewOpIdWithValid(s.lamportClock.increment()),
		ObjId:   dstTree.ObjId,
		Prop:    property,
		Action:  MOVE,
		Value:   scalarValue,
		MovedID: &ObjIdToBeMoved,
		MoveSrc: &MovedFrom,
		Pred:    pred,
		Succ:    make([]OpId, 0),
		Insert:  false,
	}

	for _, op := range srcOps {
		op.addSuccessor(localOp.OpId.Id)
	}
	for _, op := range dstOps {
		op.addSuccessor(localOp.OpId.Id)
	}

	dstTree.insertOp(&localOp, insertIndex)
}

func (s *OpSet) moveFromList(srcTree *OpTree, index int) (ObjIdToBeMoved OpId, srcOps []*Operation, scalarValue any) {
	if srcOps, ObjIdToBeMoved, scalarValue = srcTree.moveNth(index); len(srcOps) == 0 {
		return OpId{}, nil, nil
	} else {
		return ObjIdToBeMoved, srcOps, scalarValue
	}
}

func (s *OpSet) moveToList(MovedFrom OpId, dstTree *OpTree, index int, ObjIdToBeMoved OpId, srcOps []*Operation, scalarValue any) error {
	// if we move an object, scalar value is nil
	if insertIndex, insertProp, err := dstTree.insertNth(index); err != nil {
		return err
	} else {
		pred := make([]OpId, 0)
		for _, operation := range srcOps {
			pred = append(pred, operation.OpId.Id)
		}
		// move op is part of the RGA tree, so we don't need to update the destination predecessors

		localOp := Operation{
			OpId:    NewOpIdWithValid(s.lamportClock.increment()),
			ObjId:   dstTree.ObjId,
			Prop:    insertProp,
			Action:  MOVE,
			Value:   scalarValue,
			MovedID: &ObjIdToBeMoved,
			MoveSrc: &MovedFrom,
			Pred:    pred,
			Succ:    make([]OpId, 0),
			Insert:  true, // must be set to true, because move op is part of the RGA tree
		}

		for _, op := range srcOps {
			op.addSuccessor(localOp.OpId.Id)
		}

		dstTree.insertOp(&localOp, insertIndex)
	}
	return nil

}

func (s *OpSet) GenericMove(srcObjId OpId, dstObjId OpId, srcPropertyOrIndex any, dstPropertyOrIndex any) error {
	logrus.Tracef("Try to move %v:%v to %v:%v", srcObjId, srcPropertyOrIndex, dstObjId, dstPropertyOrIndex)
	srcTree := s.opTrees[srcObjId]
	dstTree := s.opTrees[dstObjId]
	var objIdToBeMoved OpId
	var srcOps []*Operation
	var scalarValue any
	// handle the source object
	if srcTree.Type == MAP {
		if property, ok := srcPropertyOrIndex.(string); ok {
			objIdToBeMoved, srcOps, scalarValue = s.moveFromMap(srcTree, property)
			if srcOps == nil {
				return errors.PropertyNotFoundError{PropertyName: property}
			}
		} else {
			return errors.InvalidOperationError{Reason: "cannot move property on a list"}
		}
	} else {
		if index, ok := srcPropertyOrIndex.(int); ok {
			objIdToBeMoved, srcOps, scalarValue = s.moveFromList(srcTree, index)
			if srcOps == nil {
				return errors.ListIndexExceedsLengthError{Index: index}
			}
		} else {
			return errors.InvalidOperationError{Reason: "cannot move index on a map"}
		}
	}

	if dstTree.Type == MAP {
		if property, ok := dstPropertyOrIndex.(string); ok {
			s.moveToMap(srcTree.ObjId, dstTree, property, objIdToBeMoved, srcOps, scalarValue)
		} else {
			return errors.InvalidOperationError{Reason: "cannot move property on a list"}
		}
	} else {
		if index, ok := dstPropertyOrIndex.(int); ok {
			return s.moveToList(srcTree.ObjId, dstTree, index, objIdToBeMoved, srcOps, scalarValue)
		} else {
			return errors.InvalidOperationError{Reason: "cannot move index on a map"}
		}
	}

	return nil

}

func (s *OpSet) Visualize() string {

	graphAst := gographviz.NewGraph()
	graphAst.SetName("OpSet")
	graphAst.SetDir(true)
	// Extract the keys into a slice
	keys := make([]OpId, 0, len(s.opTrees))
	for k := range s.opTrees {
		keys = append(keys, k)
	}

	// Sort the keys
	sort.Slice(keys, func(i, j int) bool {
		return !keys[i].GreaterThan(s, &keys[j])
	})

	for _, key := range keys {
		tree := s.opTrees[key]
		html := tree.visualize()
		parentId := fmt.Sprintf("node_%v", rand.Int63())
		graphAst.AddNode("OpSet", parentId, map[string]string{
			"shape": "ellipse",
			"label": fmt.Sprintf("\"%v\"", tree.ObjId.String()),
		})
		graphAst.AddNode(parentId, parentId+"child", map[string]string{
			"shape": "none",
			"label": html,
		})
		graphAst.AddEdge(parentId, parentId+"child", true, map[string]string{
			"label": "\"\"",
		})
	}
	return graphAst.String()
}

func (s *OpSet) MoveObject(src OpId, dst OpId) error {
	parent, property, err := s.moveManager.getParentAndProperty(src)
	if property == nil {
		panic(src.String() + ": property is nil")
	} else if err != nil {
		return err
	}
	return s.GenericMove(parent, dst, property, property)
}

func (s *OpSet) BulkUpdateValidity(operations []*Operation) {
	s.moveManager.BulkUpdateValidity(operations)
}
