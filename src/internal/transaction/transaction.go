package transaction

import (
	"fmt"
	"github.com/LiangrunDa/AutomergeWithMove/internal/log"
	"github.com/LiangrunDa/AutomergeWithMove/internal/opset"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

type Transaction interface {
	Get(objId opset.ExOpId, propertyOrIndex any) (any, error)
	Put(objId opset.ExOpId, propertyOrIndex any, value any)
	Delete(objId opset.ExOpId, propertyOrIndex any) error
	PutObject(objId opset.ExOpId, property string, objType opset.ObjType) (opset.ExOpId, error)
	Insert(objId opset.ExOpId, index int, value any)
	InsertObject(objId opset.ExOpId, index int, objType opset.ObjType) (opset.ExOpId, error)
	Move(srcObjId opset.ExOpId, dstObjId opset.ExOpId, srcPropertyOrIndex any, dstPropertyOrIndex any) error
	Commit() (*Change, error)
	MoveObject(srcObjId opset.ExOpId, dstObjId opset.ExOpId) error
}

type TransactionImpl struct {
	actorId           uuid.UUID
	seq               uint32
	ops               *opset.OpSet
	pendingOperations []*opset.Operation
	deps              []ChangeHash
	startOpCounter    uint64
}

func NewTransaction(actorId uuid.UUID, seq uint32, ops *opset.OpSet, startOpCounter uint64, deps []ChangeHash, analysisMode bool) Transaction {
	ops.UpdateLamportClock(startOpCounter)
	txn := &TransactionImpl{
		actorId:        actorId,
		seq:            seq,
		ops:            ops,
		deps:           deps,
		startOpCounter: startOpCounter,
	}
	txnId := fmt.Sprintf("%d-%d", ops.GetIdx(actorId), seq)
	logrus.SetFormatter(log.NewTransactionFormatter(txnId, analysisMode))
	return txn
}

func (t *TransactionImpl) addOperation(op *opset.Operation) {
	t.pendingOperations = append(t.pendingOperations, op)
}

func (t *TransactionImpl) updatePendingOps() {
	op := t.ops.GetLastOperation()
	if op != nil {
		t.addOperation(op)
	}
}

func covertToFloat64(value any) any {
	switch value.(type) {
	case int:
		return float64(value.(int))
	case int64:
		return float64(value.(int64))
	case float32:
		return float64(value.(float32))
	default:
		return value
	}
}

func (t *TransactionImpl) Get(objId opset.ExOpId, propertyOrIndex any) (any, error) {
	id := objId.ToOpId(t.ops)
	res, err := t.ops.Get(*id, propertyOrIndex)
	if v, ok := res.(*opset.OpIdWithValid); ok {
		return *v.Id.ToExOpId(t.ops), err
	} else if v, ok := res.(opset.OpId); ok {
		return *v.ToExOpId(t.ops), err
	} else {
		return res, err
	}
}

func (t *TransactionImpl) Put(objId opset.ExOpId, propertyOrIndex any, value any) {
	id := objId.ToOpId(t.ops)
	if err := t.ops.Put(*id, propertyOrIndex, covertToFloat64(value)); err == nil {
		t.updatePendingOps()
	} else {
		logrus.Error(err)
	}
}

func (t *TransactionImpl) Delete(objId opset.ExOpId, propertyOrIndex any) error {
	id := objId.ToOpId(t.ops)
	if err := t.ops.Delete(*id, propertyOrIndex); err == nil {
		t.updatePendingOps()
		return nil
	} else {
		return err
	}
}

func (t *TransactionImpl) PutObject(objId opset.ExOpId, property string, objType opset.ObjType) (opset.ExOpId, error) {
	if id, err := t.ops.PutObject(*objId.ToOpId(t.ops), property, objType); err == nil {
		t.updatePendingOps()
		return *id.ToExOpId(t.ops), nil
	} else {
		return opset.ExOpId{}, err
	}
}

func (t *TransactionImpl) Insert(objId opset.ExOpId, index int, value any) {
	if err := t.ops.Insert(*objId.ToOpId(t.ops), index, covertToFloat64(value)); err == nil {
		t.updatePendingOps()
	} else {
		logrus.Error(err)
	}
}

func (t *TransactionImpl) InsertObject(objId opset.ExOpId, index int, objType opset.ObjType) (opset.ExOpId, error) {
	if id, err := t.ops.InsertObject(*objId.ToOpId(t.ops), index, objType); err == nil {
		t.updatePendingOps()
		return *id.ToExOpId(t.ops), nil
	} else {
		return opset.ExOpId{}, err
	}
}

func (t *TransactionImpl) Move(srcObjId opset.ExOpId, dstObjId opset.ExOpId, srcPropertyOrIndex any, dstPropertyOrIndex any) error {
	if err := t.ops.GenericMove(*srcObjId.ToOpId(t.ops), *dstObjId.ToOpId(t.ops), srcPropertyOrIndex, dstPropertyOrIndex); err == nil {
		t.updatePendingOps()
		return nil
	} else {
		return err
	}
}

func (t *TransactionImpl) Commit() (*Change, error) {
	copiedOps := make([]*opset.Operation, len(t.pendingOperations))
	for i, op := range t.pendingOperations {
		copiedOps[i] = op
	}
	return NewChange(t.actorId, t.seq, copiedOps, t.deps, t.startOpCounter, t.ops), nil
}

func (t *TransactionImpl) MoveObject(srcObjId opset.ExOpId, dstObjId opset.ExOpId) error {
	if err := t.ops.MoveObject(*srcObjId.ToOpId(t.ops), *dstObjId.ToOpId(t.ops)); err == nil {
		t.updatePendingOps()
		return nil
	} else {
		return err
	}
}
