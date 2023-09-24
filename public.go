package automergeproto

import (
	"github.com/LiangrunDa/AutomergeWithMove/internal/opset"
	"github.com/LiangrunDa/AutomergeWithMove/internal/transaction"
)

var RootOpId = opset.RootOpId
var ExRootOpId = opset.ExRootOpId

// type

type ExChange = transaction.ExChange
type ExOperation = opset.ExOperation
type ExOpId = opset.ExOpId
type Change = transaction.Change
type ChangeHash = transaction.ChangeHash
type Operation = opset.Operation
type OpId = opset.OpId
type OpSet = opset.OpSet
type ActionType = opset.ActionType
type ObjType = opset.ObjType

// const

const MAP = opset.MAP
const LIST = opset.LIST
