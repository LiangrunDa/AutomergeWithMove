package automergeproto

import (
	"encoding/json"
	"errors"
	"github.com/LiangrunDa/AutomergeWithMove/internal/log"
	"github.com/LiangrunDa/AutomergeWithMove/internal/opset"
	"github.com/LiangrunDa/AutomergeWithMove/internal/transaction"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"strconv"
	"sync"
)

type Automerge struct {
	history            []*Change
	historyIndex       map[ChangeHash]int
	dependencies       []ChangeHash
	ops                *OpSet
	actorId            uuid.UUID
	maxOp              uint64
	txnSeq             uint32
	currentTransaction transaction.Transaction
	queue              []*Change
	lock               sync.Mutex
	enableAnalysis     bool
}

func NewAutomerge(actorId uuid.UUID) *Automerge {
	//SetLogLevel(logrus.TraceLevel)
	return &Automerge{
		history:        make([]*Change, 0),
		historyIndex:   map[ChangeHash]int{},
		dependencies:   make([]ChangeHash, 0),
		ops:            opset.NewOpSet(actorId),
		actorId:        actorId,
		maxOp:          0,
		lock:           sync.Mutex{},
		enableAnalysis: false,
	}
}

func (a *Automerge) StartTransaction() transaction.Transaction {
	a.lock.Lock()
	clonedDeps := append([]ChangeHash{}, a.dependencies...)
	a.txnSeq = a.txnSeq + 1
	a.currentTransaction = transaction.NewTransaction(a.actorId, a.txnSeq, a.ops, a.maxOp, clonedDeps, a.enableAnalysis)
	logrus.Info("Start transaction")
	return a.currentTransaction
}

func (a *Automerge) CommitTransaction() {
	defer func() {
		if err := recover(); err != nil {
			a.lock.Unlock()
			panic(err)
		} else {
			a.lock.Unlock()
		}
	}()
	if change, err := a.currentTransaction.Commit(); err == nil {
		a.history = append(a.history, change)
		a.historyIndex[change.Hash()] = len(a.history) - 1
		a.dependencies = []ChangeHash{change.Hash()}
		a.maxOp = a.ops.GetLamportClock().Counter
	} else {
		panic(err)
	}
	logrus.Info("Commit transaction")
}

func (a *Automerge) GetHistory() []byte {
	var exHistory transaction.ExChangeArray
	for _, change := range a.history {
		exChange := change.ToExChange(a.ops)
		exHistory = append(exHistory, &exChange)
	}
	if bytes, err := json.Marshal(exHistory); err == nil {
		return bytes
	} else {
		return nil
	}
}

func (a *Automerge) GetLatestChange() (*Change, error) {
	if len(a.history) == 0 {
		return nil, errors.New("no changes")
	} else {
		return a.history[len(a.history)-1], nil
	}
}

func (a *Automerge) GetLatestChangeBytes() ([]byte, error) {
	if len(a.history) == 0 {
		return nil, errors.New("no changes")
	} else {
		return a.history[len(a.history)-1].ToBytes(a.ops), nil
	}
}

func (a *Automerge) isCausallyReady(change *Change) bool {
	for _, dep := range change.Dependencies {
		if _, ok := a.historyIndex[dep]; !ok {
			return false
		}
	}
	return true
}

func (a *Automerge) ApplyChanges(changes []*Change) []*Change {
	applied := make([]*Change, 0)
	a.lock.Lock()
	defer a.lock.Unlock()
	for _, c := range changes {
		if _, ok := a.historyIndex[c.Hash()]; !ok {
			if a.isCausallyReady(c) {
				a.applyChange(c)
				applied = append(applied, c)
			} else {
				a.queue = append(a.queue, c)
			}
		}
	}
	for len(a.queue) > 0 {
		found := false
		for i := 0; i < len(a.queue); i++ { // pop next causally ready change
			if a.isCausallyReady(a.queue[i]) {
				a.applyChange(a.queue[i])
				applied = append(applied, a.queue[i])
				a.queue = append(a.queue[:i], a.queue[i+1:]...)
				found = true
				break
			}
		}
		if !found {
			break
		}
	}
	return applied
}

func (a *Automerge) applyChange(change *Change) {
	changeId := strconv.Itoa(int(a.ops.GetIdx(change.ActorId))) + "-" + strconv.Itoa(int(change.Seq))
	logrus.SetFormatter(log.NewApplyingFormatter(changeId, a.enableAnalysis))
	logrus.Info("Start applying change")
	a.history = append(a.history, change)
	a.historyIndex[change.Hash()] = len(a.history) - 1
	if a.maxOp < change.StartOp+uint64(len(change.Operations)) {
		a.maxOp = change.StartOp + uint64(len(change.Operations))
	}
	filtered := make([]ChangeHash, 0)
	changeDepMap := map[ChangeHash]bool{}
	for _, dep := range change.Dependencies {
		changeDepMap[dep] = true
	}
	for _, dep := range a.dependencies {
		if _, ok := changeDepMap[dep]; !ok {
			filtered = append(filtered, dep)
		}
	}
	a.dependencies = append(filtered, change.Hash())
	for i := range change.Operations {
		a.ops.InsertOperation(change.Operations[i])
	}
	a.ops.BulkUpdateValidity(change.Operations)
	logrus.Info("Finish applying change")
}

func (a *Automerge) Merge(b *Automerge) {
	changes := transaction.NewChangeArrayFromBytes(b.GetHistory(), a.ops)
	a.ApplyChanges(changes)
}

func (a *Automerge) Fork() *Automerge {
	id, _ := uuid.NewRandom()
	doc := NewAutomerge(id)
	doc.Merge(a)
	return doc
}

func (a *Automerge) MergeFromChangeBytes(bytes []byte) {
	change := transaction.NewChangeFromBytes(bytes, a.ops)
	a.ApplyChanges([]*Change{change})
}

// ------------------------------
// BENCHMARK AND CHECKING

func (a *Automerge) MergeFromChangeBytesAndGetNewObjects(bytes []byte) []ExOpId {
	change := transaction.NewChangeFromBytes(bytes, a.ops)
	appliedChanges := a.ApplyChanges([]*Change{change})
	makeOpIds := make([]ExOpId, 0)
	for _, c := range appliedChanges {
		for _, op := range c.Operations {
			if op.Action == opset.MAKE {
				makeOpIds = append(makeOpIds, *op.OpId.Id.ToExOpId(a.ops))
			}
		}
	}
	return makeOpIds
}

func DisableMove() {
	opset.MoveEnabled = false
}

func SetLogLevel(level logrus.Level) {
	logrus.SetLevel(level)
}

func DebugMode() {
	logrus.StandardLogger().SetLevel(logrus.TraceLevel)
}

func SetLogPath(path string) {
	logFile, _ := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	mw := io.MultiWriter(os.Stdout, logFile)
	logrus.SetOutput(mw)
}

func (a *Automerge) EnableAnalysis() {
	a.enableAnalysis = true
}

func (a *Automerge) GetDocumentTree() map[ExOpId]ExOpId {
	return a.ops.GetDocumentTree()
}
