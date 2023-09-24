package opset

import (
	"fmt"
	"github.com/awalterschulze/gographviz"
)

type DocumentTree struct {
	parentMap   map[OpId]OpId // key: objId, value: parent objId
	lifecycles  map[OpId]*LifeCycleList
	propertyMap map[OpId]any
}

func NewDocumentTree(lifecycles map[OpId]*LifeCycleList) *DocumentTree {
	return &DocumentTree{
		parentMap:   make(map[OpId]OpId),
		lifecycles:  lifecycles,
		propertyMap: make(map[OpId]any),
	}
}

func (tree *DocumentTree) add(opId OpId, parentId OpId) {
	tree.parentMap[opId] = parentId
}

func (tree *DocumentTree) updateParentAs(opId OpId, parentId OpId) {
	tree.parentMap[opId] = parentId
}

func (tree *DocumentTree) remove(opId OpId) {
	delete(tree.parentMap, opId)
}

// return true if a is ancestor of b
func (tree *DocumentTree) isAncestorOf(time *OpIdWithValid, a OpId, b OpId) bool {
	for {
		if b == a {
			// could be in the same trash tree or the present tree
			return true
		}
		if b == RootOpId {
			return false
		}
		if tree.isTrashRoot(time, b) {
			// even if a is in trash, a doesn't exist in the trash tree of b
			return false
		}
		b = tree.getParent(b)
	}
}

func (tree *DocumentTree) getParent(opId OpId) OpId {
	if parent, ok := tree.parentMap[opId]; ok {
		return parent
	} else {
		panic("parent not found")
	}
}

func (tree *DocumentTree) isTrashRoot(time *OpIdWithValid, opId OpId) bool {
	return !tree.lifecycles[opId].isPresent(time)
}

func (tree *DocumentTree) inTrash(time *OpIdWithValid, opId OpId) bool {
	if opId == RootOpId {
		return false
	}
	if tree.isTrashRoot(time, opId) {
		return true
	}
	parent := tree.getParent(opId)
	return tree.inTrash(time, parent)
}

func (tree *DocumentTree) String() string {
	treeMap := make(map[string]string)
	for k, v := range tree.parentMap {
		if k != RootOpId {
			treeMap[k.String()] = v.String()
		}
	}
	return printTree(treeMap, RootOpId.String(), "", true)
}

func printTree(treeMap map[string]string, root string, prefix string, isTail bool) string {
	result := fmt.Sprintf("%s%s%s\n", prefix, "└── ", root)

	children := make([]string, 0)
	for child, parent := range treeMap {
		if parent == root {
			children = append(children, child)
		}
	}

	for i, child := range children {
		isLast := i == len(children)-1
		newPrefix := prefix
		if isTail {
			newPrefix += "    "
		} else {
			newPrefix += "│   "
		}
		result += printTree(treeMap, child, newPrefix, isLast)
	}
	return result
}

func (tree *DocumentTree) Visualize() string {
	graphAst := gographviz.NewGraph()
	graphAst.SetName("DocumentTree")
	graphAst.SetDir(true)
	for opId, parentId := range tree.parentMap {
		parentId.String()
		opId.String()

		parentIdStr := fmt.Sprintf("node%vc%v", parentId.ActorId, parentId.Counter)
		opIdStr := fmt.Sprintf("node%vc%v", opId.ActorId, opId.Counter)
		graphAst.AddNode(parentIdStr, opIdStr, map[string]string{
			"shape": "ellipse",
			"label": fmt.Sprintf("\"%v\"", opId.String()),
		})
		if opId == RootOpId {
			continue
		}
		graphAst.AddEdge(parentIdStr, opIdStr, true, map[string]string{
			"label": "\"\"",
		})
	}

	return graphAst.String()
}

func (tree *DocumentTree) getPresentParent(id OpId) (OpId, error) {
	if tree.inTrash(NewOpIdWithValid(NullOpId), id) {
		return NullOpId, fmt.Errorf("id %v is in trash", id)
	} else {
		return tree.getParent(id), nil
	}
}

func (tree *DocumentTree) updateProperty(opId OpId, property any) {
	tree.propertyMap[opId] = property
}

func (tree *DocumentTree) getProperty(id OpId) any {
	return tree.propertyMap[id]
}
