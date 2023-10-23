package automergeproto

import (
	"github.com/LiangrunDa/AutomergeWithMove/errors"
	"github.com/LiangrunDa/AutomergeWithMove/internal/opset"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMap(t *testing.T) {
	//DebugMode()
	id, _ := uuid.NewRandom()
	doc := NewAutomerge(id)
	tx := doc.StartTransaction()

	tx.Put(ExRootOpId, "name", "Liangrun")
	value, _ := tx.Get(opset.ExRootOpId, "name")
	assert.Equal(t, "Liangrun", value)

	tx.Put(opset.ExRootOpId, "name", "Liangrun Da")
	value, _ = tx.Get(ExRootOpId, "name")
	assert.Equal(t, "Liangrun Da", value)

	tx.Put(ExRootOpId, "age", "21")

	value, _ = tx.Get(ExRootOpId, "age")
	assert.Equal(t, "21", value)

	contact, _ := tx.PutObject(ExRootOpId, "contact", opset.MAP)
	tx.Put(contact, "email", "me@liangrunda.com")

	contactMap, _ := tx.Get(ExRootOpId, "contact")
	contactMapId := contactMap.(ExOpId)
	value, _ = tx.Get(contactMapId, "email")
	assert.Equal(t, "me@liangrunda.com", value)
	doc.CommitTransaction()

	tx2 := doc.StartTransaction()
	tx2.Put(contactMapId, "post", "80336")
	doc.CommitTransaction()
}

func TestList(t *testing.T) {
	id, _ := uuid.NewRandom()
	doc := NewAutomerge(id)
	tx := doc.StartTransaction()

	automerge, _ := tx.PutObject(ExRootOpId, "automerge", opset.LIST)
	tx.Insert(automerge, 0, "a")
	tx.Insert(automerge, 1, "u")
	tx.Insert(automerge, 2, "o")
	tx.Insert(automerge, 2, "t")
	tx.Put(automerge, 0, "A")
	tx.Insert(automerge, 0, "X")
	_ = tx.Delete(automerge, 0)

	output := []string{"A", "u", "t", "o"}
	for i := 0; i < 4; i++ {
		value, _ := tx.Get(automerge, i)
		assert.Equal(t, output[i], value)
	}
	doc.CommitTransaction()
}

func TestListAndMap(t *testing.T) {
	id, _ := uuid.NewRandom()
	doc := NewAutomerge(id)
	tx := doc.StartTransaction()

	students, _ := tx.PutObject(ExRootOpId, "students", opset.LIST)
	liangrun, _ := tx.InsertObject(students, 0, opset.MAP)
	leo, _ := tx.InsertObject(students, 1, opset.MAP)

	tx.Put(liangrun, "name", "Liangrun")
	tx.Put(liangrun, "age", 21)
	tx.Put(leo, "name", "Leo")
	tx.Put(leo, "age", 22)
	tx.Put(liangrun, "age", 99)

	liangrunMap, _ := tx.Get(students, 0)
	liangrunMapId := liangrunMap.(ExOpId)
	value, _ := tx.Get(liangrunMapId, "name")
	assert.Equal(t, "Liangrun", value)
	value, _ = tx.Get(liangrunMapId, "age")
	assert.Equal(t, float64(99), value)
	doc.CommitTransaction()
}

func TestMapMerge(t *testing.T) {
	id1, _ := uuid.NewRandom()

	// doc1: {name: "Liangrun", age: 21}
	doc1 := NewAutomerge(id1)
	tx := doc1.StartTransaction()
	tx.Put(ExRootOpId, "name", "Liangrun Da")
	tx.Put(ExRootOpId, "age", 21)
	doc1.CommitTransaction()

	// doc2: {name: "Liangrun", age: 99}
	doc2 := doc1.Fork()
	tx2 := doc2.StartTransaction()
	tx2.Put(ExRootOpId, "age", 99)
	doc2.CommitTransaction()

	// doc1: {name: "Liangrun", age: 100}
	tx3 := doc1.StartTransaction()
	tx3.Put(ExRootOpId, "age", 100)
	doc1.CommitTransaction()

	// merge doc1 and doc2
	doc1.Merge(doc2)
	doc2.Merge(doc1)

	// see whether doc1 and doc2 are the same
	tx4 := doc1.StartTransaction()
	tx5 := doc2.StartTransaction()

	value1, _ := tx4.Get(ExRootOpId, "name")
	value2, _ := tx5.Get(ExRootOpId, "name")
	assert.Equal(t, value1, value2)

	value1, _ = tx4.Get(ExRootOpId, "age")
	value2, _ = tx5.Get(ExRootOpId, "age")
	assert.Equal(t, value1, value2)

}

func TestListMerge(t *testing.T) {
	id1, _ := uuid.NewRandom()

	// doc1: ["a", "b", "c"]
	doc1 := NewAutomerge(id1)
	tx := doc1.StartTransaction()
	list, _ := tx.PutObject(ExRootOpId, "list", opset.LIST)
	tx.Insert(list, 0, "a")
	tx.Insert(list, 1, "b")
	tx.Insert(list, 2, "c")
	doc1.CommitTransaction()

	// doc2: ["x", "a", "b", "c"]
	doc2 := doc1.Fork()
	tx2 := doc2.StartTransaction()
	tx2.Insert(list, 0, "x")
	doc2.CommitTransaction()

	// doc1: ["a", "b", "c", "d"]
	tx3 := doc1.StartTransaction()
	tx3.Insert(list, 3, "d")
	doc1.CommitTransaction()

	// merge doc1 and doc2, expected ["x", "a", "b", "c", "d"]
	doc1.Merge(doc2)
	doc2.Merge(doc1)

	// see whether doc1 and doc2 are the same
	tx4 := doc1.StartTransaction()
	tx5 := doc2.StartTransaction()

	value1, _ := tx4.Get(list, 0)
	value2, _ := tx5.Get(list, 0)
	assert.Equal(t, value1, value2)
	assert.Equal(t, "x", value1)

	value1, _ = tx4.Get(list, 1)
	value2, _ = tx5.Get(list, 1)
	assert.Equal(t, value1, value2)
	assert.Equal(t, "a", value1)

	value1, _ = tx4.Get(list, 2)
	value2, _ = tx5.Get(list, 2)
	assert.Equal(t, value1, value2)
	assert.Equal(t, "b", value1)

	value1, _ = tx4.Get(list, 3)
	value2, _ = tx5.Get(list, 3)
	assert.Equal(t, value1, value2)
	assert.Equal(t, "c", value1)

	value1, _ = tx4.Get(list, 4)
	value2, _ = tx5.Get(list, 4)
	assert.Equal(t, value1, value2)
	assert.Equal(t, "d", value1)

	//fmt.Println(doc2.ops.Visualize())
}

func TestMapMove(t *testing.T) {
	id1, _ := uuid.NewRandom()
	doc1 := NewAutomerge(id1)
	tx := doc1.StartTransaction()
	A, _ := tx.PutObject(ExRootOpId, "A", opset.MAP)
	B, _ := tx.PutObject(ExRootOpId, "B", opset.MAP)
	tx.PutObject(ExRootOpId, "C", opset.MAP)
	tx.Put(A, "a1", "1")
	tx.Put(A, "a2", "2")
	tx.Put(A, "a3", "3")

	_ = tx.Move(ExRootOpId, B, "A", "A")

	movedA, _ := tx.Get(B, "A")
	movedAId := movedA.(ExOpId)
	moveda1, _ := tx.Get(movedAId, "a1")
	assert.Equal(t, "1", moveda1)

	_, err := tx.Get(ExRootOpId, "A")
	assert.Equal(t, errors.PropertyNotFoundError{PropertyName: "A"}, err)

	doc1.CommitTransaction()
	//fmt.Println(doc1.ops.Visualize())
}

func TestMapMultipleMoves(t *testing.T) {
	id1, _ := uuid.NewRandom()
	doc1 := NewAutomerge(id1)
	tx := doc1.StartTransaction()
	A, _ := tx.PutObject(ExRootOpId, "A", opset.MAP)
	B, _ := tx.PutObject(ExRootOpId, "B", opset.MAP)
	C, _ := tx.PutObject(ExRootOpId, "C", opset.MAP)
	tx.Put(A, "a1", "1")
	tx.Put(A, "a2", "2")
	tx.Put(A, "a3", "3")

	_ = tx.Move(ExRootOpId, B, "A", "A")

	firstMovedA, _ := tx.Get(B, "A")
	firstMovedAId := firstMovedA.(ExOpId)
	firstMoveda1, _ := tx.Get(firstMovedAId, "a1")
	assert.Equal(t, "1", firstMoveda1)

	_, err := tx.Get(ExRootOpId, "A")
	assert.Equal(t, errors.PropertyNotFoundError{PropertyName: "A"}, err)

	_ = tx.Move(B, C, "A", "A")
	secondMovedA, _ := tx.Get(C, "A")
	secondMovedAId := secondMovedA.(ExOpId)
	secondMoveda1, _ := tx.Get(secondMovedAId, "a1")
	assert.Equal(t, "1", secondMoveda1)

	_, err = tx.Get(B, "A")
	assert.Equal(t, errors.PropertyNotFoundError{PropertyName: "A"}, err)

	doc1.CommitTransaction()
	//fmt.Println(doc1.ops.Visualize())
}

func TestMapMoveMerge(t *testing.T) {
	id1, _ := uuid.NewRandom()
	doc1 := NewAutomerge(id1)
	tx := doc1.StartTransaction()
	A, _ := tx.PutObject(ExRootOpId, "A", opset.MAP)
	B, _ := tx.PutObject(ExRootOpId, "B", opset.MAP)
	C, _ := tx.PutObject(ExRootOpId, "C", opset.MAP)
	tx.Put(A, "a1", "1")
	tx.Put(A, "a2", "2")
	tx.Put(A, "a3", "3")
	doc1.CommitTransaction()

	doc2 := doc1.Fork()
	tx2 := doc2.StartTransaction()
	_ = tx2.Move(ExRootOpId, B, "A", "A")
	doc2.CommitTransaction()

	tx3 := doc1.StartTransaction()
	_ = tx3.Move(ExRootOpId, C, "A", "A")
	doc1.CommitTransaction()

	doc1.Merge(doc2)
	doc2.Merge(doc1)

	//fmt.Println(doc1.ops.Visualize())
	// either A is moved to B or C, but not both
	tx4 := doc1.StartTransaction()
	tx5 := doc2.StartTransaction()

	doc1value1, doc1err1 := tx4.Get(B, "A")
	doc1value2, doc1err2 := tx4.Get(C, "A")

	doc2value1, doc2err1 := tx5.Get(B, "A")
	doc2value2, doc2err2 := tx5.Get(C, "A")

	assert.True(t, doc1err1 == doc2err1 && doc1err2 == doc2err2)

	if doc1err1 == nil { // moved A to B
		assert.Equal(t, errors.PropertyNotFoundError{
			PropertyName: "A",
		}, doc1err2)

		// doc1
		doc1MovedAId := doc1value1.(ExOpId)
		doc1Moveda1, _ := tx4.Get(doc1MovedAId, "a1")
		assert.Equal(t, "1", doc1Moveda1)

		// doc2
		doc2MovedAId := doc2value1.(ExOpId)
		doc2Moveda1, _ := tx5.Get(doc2MovedAId, "a1")
		assert.Equal(t, "1", doc2Moveda1)

	} else if doc1err2 == nil { // moved A to C
		assert.Equal(t, errors.PropertyNotFoundError{
			PropertyName: "A",
		}, doc1err1)

		// doc1
		doc1MovedAId := doc1value2.(ExOpId)
		doc1Moveda1, _ := tx4.Get(doc1MovedAId, "a1")
		assert.Equal(t, "1", doc1Moveda1)

		// doc2
		doc2MovedAId := doc2value2.(ExOpId)
		doc2Moveda1, _ := tx5.Get(doc2MovedAId, "a1")
		assert.Equal(t, "1", doc2Moveda1)
	}

	//fmt.Println(doc1.ops.Visualize())
	//fmt.Println(doc2.ops.Visualize())
}

func TestCycleDetection(t *testing.T) {
	id1, _ := uuid.NewRandom()
	doc1 := NewAutomerge(id1)
	tx := doc1.StartTransaction()
	A, _ := tx.PutObject(ExRootOpId, "A", opset.MAP)
	tx.PutObject(ExRootOpId, "B", opset.MAP)

	C, _ := tx.PutObject(A, "C", opset.MAP)

	_ = tx.Move(ExRootOpId, C, "A", "A")
	_, err := tx.Get(C, "A")
	assert.Equal(t, errors.PropertyNotFoundError{PropertyName: "A"}, err)

	value, err := tx.Get(ExRootOpId, "A")
	valueId := value.(ExOpId)
	assert.Equal(t, A, valueId)

	doc1.CommitTransaction()
	//fmt.Println(doc1.ops.MoveLogVisualize())
}

func TestCycleDetectionMerge(t *testing.T) {
	id1, _ := uuid.NewRandom()
	doc1 := NewAutomerge(id1)
	tx := doc1.StartTransaction()
	A, _ := tx.PutObject(ExRootOpId, "A", opset.MAP)
	B, _ := tx.PutObject(ExRootOpId, "B", opset.MAP)

	tx.PutObject(A, "C", opset.MAP)
	doc1.CommitTransaction()

	// move B to be a child of A
	doc2 := doc1.Fork()
	tx2 := doc2.StartTransaction()
	_ = tx2.Move(ExRootOpId, B, "A", "A")
	doc2.CommitTransaction()

	// move A to be a child of B
	tx3 := doc1.StartTransaction()
	_ = tx3.Move(ExRootOpId, A, "B", "B")
	doc1.CommitTransaction()

	// merge
	doc1.Merge(doc2)
	doc2.Merge(doc1)

	// either A is a child of B, or B is a child of A, but not both
	tx4 := doc1.StartTransaction()
	tx5 := doc2.StartTransaction()

	doc1value1, doc1err1 := tx4.Get(B, "A")
	doc1value2, doc1err2 := tx4.Get(A, "B")

	doc2value1, doc2err1 := tx5.Get(B, "A")
	doc2value2, doc2err2 := tx5.Get(A, "B")

	assert.True(t, doc1err1 == doc2err1 && doc1err2 == doc2err2)

	if doc1err1 == nil { // A is a child of B
		assert.Equal(t, errors.PropertyNotFoundError{
			PropertyName: "B",
		}, doc1err2)
		// doc1
		doc1MovedAId := doc1value1.(ExOpId)
		assert.Equal(t, doc1MovedAId, A)
		// doc2
		doc2MovedAId := doc2value1.(ExOpId)
		assert.Equal(t, doc2MovedAId, A)
	} else if doc1err2 == nil { // B is a child of A
		assert.Equal(t, errors.PropertyNotFoundError{
			PropertyName: "A",
		}, doc1err1)
		// doc1
		doc1MovedAId := doc1value2.(ExOpId)
		assert.Equal(t, doc1MovedAId, B)
		// doc2
		doc2MovedAId := doc2value2.(ExOpId)
		assert.Equal(t, doc2MovedAId, B)
	}
	//fmt.Println(doc1.ops.MoveLogVisualize())
	//fmt.Println(doc1.ops.Visualize())
}

func TestMoveScalarValue(t *testing.T) {
	id1, _ := uuid.NewRandom()
	doc1 := NewAutomerge(id1)
	tx := doc1.StartTransaction()
	A, _ := tx.PutObject(ExRootOpId, "A", opset.MAP)
	B, _ := tx.PutObject(ExRootOpId, "B", opset.MAP)
	tx.PutObject(ExRootOpId, "C", opset.MAP)
	tx.Put(A, "a1", "1")
	tx.Put(A, "a2", "2")
	tx.Put(A, "a3", "3")

	tx.Move(A, B, "a1", "b1")
	value, _ := tx.Get(B, "b1")
	assert.Equal(t, "1", value)
	_, err := tx.Get(A, "a1")
	assert.Equal(t, errors.PropertyNotFoundError{PropertyName: "a1"}, err)

	doc1.CommitTransaction()
}

func TestMoveScalarValueMerge(t *testing.T) {
	id1, _ := uuid.NewRandom()
	doc1 := NewAutomerge(id1)
	tx := doc1.StartTransaction()
	A, _ := tx.PutObject(ExRootOpId, "A", opset.MAP)
	B, _ := tx.PutObject(ExRootOpId, "B", opset.MAP)
	C, _ := tx.PutObject(ExRootOpId, "C", opset.MAP)
	tx.Put(A, "a1", "1")
	tx.Put(A, "a2", "2")
	tx.Put(A, "a3", "3")
	doc1.CommitTransaction()

	doc2 := doc1.Fork()
	tx2 := doc2.StartTransaction()
	_ = tx2.Move(A, B, "a1", "b1")
	doc2.CommitTransaction()

	tx3 := doc1.StartTransaction()
	_ = tx3.Move(A, C, "a1", "c1")
	doc1.CommitTransaction()

	doc1.Merge(doc2)
	doc2.Merge(doc1)
	//fmt.Println(doc1.ops.Visualize())

	// either a1 is moved to B or C, but not both
	tx4 := doc1.StartTransaction()
	tx5 := doc2.StartTransaction()

	doc1value1, doc1err1 := tx4.Get(B, "b1")
	doc1value2, doc1err2 := tx4.Get(C, "c1")

	doc2value1, doc2err1 := tx5.Get(B, "b1")
	doc2value2, doc2err2 := tx5.Get(C, "c1")

	assert.True(t, (doc1err1 == doc2err1 && doc1err1 == nil) || (doc1err2 == doc2err2 && doc1err2 == nil)) // one of them must be nil

	if doc1err1 == nil { // moved a1 to B
		assert.Equal(t, errors.PropertyNotFoundError{
			PropertyName: "c1",
		}, doc1err2)

		// doc1
		assert.Equal(t, "1", doc1value1)

		// doc2
		assert.Equal(t, "1", doc2value1)

	} else if doc1err2 == nil { // moved A to C
		assert.Equal(t, errors.PropertyNotFoundError{
			PropertyName: "b1",
		}, doc1err1)

		// doc1
		assert.Equal(t, "1", doc1value2)

		// doc2
		assert.Equal(t, "1", doc2value2)
	}

	//fmt.Println(doc1.ops.Visualize())

}

func TestListMove(t *testing.T) {
	id, _ := uuid.NewRandom()
	doc := NewAutomerge(id)
	tx := doc.StartTransaction()

	todoList, _ := tx.PutObject(ExRootOpId, "todo list", opset.LIST)
	tx.Insert(todoList, 0, "buy milk")
	tx.Insert(todoList, 1, "water plants")
	tx.Insert(todoList, 2, "phone Joe")

	tx.Move(todoList, todoList, 2, 0)
	output := []string{"phone Joe", "buy milk", "water plants"}
	for i := 0; i < 3; i++ {
		value, _ := tx.Get(todoList, i)
		assert.Equal(t, output[i], value)
	}
	doc.CommitTransaction()
}

func TestListMoveMerge(t *testing.T) {
	id, _ := uuid.NewRandom()
	doc := NewAutomerge(id)
	tx := doc.StartTransaction()

	todoList, _ := tx.PutObject(ExRootOpId, "todo list", opset.LIST)
	tx.Insert(todoList, 0, "buy milk")
	tx.Insert(todoList, 1, "water plants")
	tx.Insert(todoList, 2, "phone Joe")
	doc.CommitTransaction()

	doc2 := doc.Fork()
	tx2 := doc2.StartTransaction()
	tx2.Move(todoList, todoList, 2, 1)
	doc2.CommitTransaction()

	tx3 := doc.StartTransaction()
	tx3.Move(todoList, todoList, 2, 0)
	doc.CommitTransaction()

	doc.Merge(doc2)
	doc2.Merge(doc)
	//fmt.Println(doc.ops.Visualize())

	// either "phone Joe" is moved to index 0 or 1, but not both
	output1 := []string{"phone Joe", "buy milk", "water plants"}
	output2 := []string{"buy milk", "phone Joe", "water plants"}

	tx4 := doc.StartTransaction()
	tx5 := doc2.StartTransaction()

	for i := 0; i < 3; i++ {
		value1, _ := tx4.Get(todoList, i)
		value2, _ := tx5.Get(todoList, i)
		assert.True(t, (value1 == output1[i] && value2 == output1[i]) || (value1 == output2[i] && value2 == output2[i]))
	}
	doc.CommitTransaction()
	doc2.CommitTransaction()
}

func TestMoveList2Map(t *testing.T) {
	id, _ := uuid.NewRandom()
	doc := NewAutomerge(id)
	tx := doc.StartTransaction()

	queue, _ := tx.PutObject(ExRootOpId, "queue", opset.LIST)
	alice, _ := tx.InsertObject(queue, 0, opset.MAP)
	tx.Put(alice, "name", "Alice")
	tx.Put(alice, "age", 22)

	bob, _ := tx.InsertObject(queue, 1, opset.MAP)
	tx.Put(bob, "name", "Bob")
	tx.Put(bob, "age", 23)

	leader, _ := tx.PutObject(ExRootOpId, "leader", opset.MAP)
	tx.Put(leader, "duration", "1day")
	tx.Move(queue, leader, 0, "person")

	// test whether alice is moved to leader
	expectAlice, _ := tx.Get(leader, "person")
	assert.Equal(t, alice, expectAlice)

	// test whether alice is removed from queue
	expectBob, _ := tx.Get(queue, 0)
	assert.Equal(t, bob, expectBob)
	_, err := tx.Get(queue, 1)
	assert.Equal(t, err, errors.ListIndexExceedsLengthError{Index: 1})

	doc.CommitTransaction()
}

func TestMoveMap2List(t *testing.T) {
	id, _ := uuid.NewRandom()
	doc := NewAutomerge(id)
	tx := doc.StartTransaction()

	leader, _ := tx.PutObject(ExRootOpId, "leader", opset.MAP)
	tx.Put(leader, "duration", "1day")

	queue, _ := tx.PutObject(ExRootOpId, "queue", opset.LIST)
	alice, _ := tx.PutObject(leader, "person", opset.MAP)
	tx.Put(alice, "name", "Alice")
	tx.Put(alice, "age", 22)

	bob, _ := tx.InsertObject(queue, 0, opset.MAP)
	tx.Put(bob, "name", "Bob")
	tx.Put(bob, "age", 23)

	tx.Move(leader, queue, "person", 0)

	//fmt.Println(doc.ops.Visualize())
	// test whether alice is moved to queue
	expectAlice, _ := tx.Get(queue, 0)
	assert.Equal(t, alice, expectAlice)

	// test whether alice is removed from leader
	_, err := tx.Get(leader, "person")
	assert.Equal(t, err, errors.PropertyNotFoundError{PropertyName: "person"})

	// test whether bob is at index 1
	expectBob, _ := tx.Get(queue, 1)
	assert.Equal(t, bob, expectBob)

	doc.CommitTransaction()
}

func TestMoveList2MapMerge(t *testing.T) {
	id, _ := uuid.NewRandom()
	doc1 := NewAutomerge(id)
	tx := doc1.StartTransaction()

	queue, _ := tx.PutObject(ExRootOpId, "queue", opset.LIST)
	alice, _ := tx.InsertObject(queue, 0, opset.MAP)
	tx.Put(alice, "name", "Alice")
	tx.Put(alice, "age", 22)

	bob, _ := tx.InsertObject(queue, 1, opset.MAP)
	tx.Put(bob, "name", "Bob")
	tx.Put(bob, "age", 23)

	team, _ := tx.PutObject(ExRootOpId, "team", opset.MAP)
	tx.Put(team, "teamName", "team1")
	doc1.CommitTransaction()

	doc2 := doc1.Fork()
	tx2 := doc2.StartTransaction()
	tx2.Move(queue, team, 1, "leader")
	doc2.CommitTransaction()

	tx3 := doc1.StartTransaction()
	tx3.Move(queue, team, 1, "follower")
	doc1.CommitTransaction()

	doc1.Merge(doc2)
	doc2.Merge(doc1)
	//fmt.Println(doc1.ops.Visualize())

	// either bob is moved to leader or follower, but not both
	tx4 := doc1.StartTransaction()
	tx5 := doc2.StartTransaction()

	doc1value1, err1 := tx4.Get(team, "leader")
	doc1value2, err2 := tx5.Get(team, "leader")
	assert.Equal(t, err2, err1)
	assert.Equal(t, doc1value1, doc1value2)

	doc2value1, err3 := tx4.Get(team, "follower")
	doc2value2, err4 := tx5.Get(team, "follower")
	assert.Equal(t, err4, err3)
	assert.Equal(t, doc2value1, doc2value2)

	if err1 != nil {
		assert.Equal(t, err1, errors.PropertyNotFoundError{PropertyName: "leader"})
		assert.Equal(t, doc2value1, bob)
	} else {
		assert.Equal(t, err3, errors.PropertyNotFoundError{PropertyName: "follower"})
		assert.Equal(t, doc1value1, bob)
	}
}

func TestMoveMap2ListMerge(t *testing.T) {
	id, _ := uuid.NewRandom()
	doc1 := NewAutomerge(id)
	tx := doc1.StartTransaction()

	leader, _ := tx.PutObject(ExRootOpId, "leader", opset.MAP)
	tx.Put(leader, "duration", "1day")

	queue, _ := tx.PutObject(ExRootOpId, "queue", opset.LIST)
	alice, _ := tx.PutObject(leader, "person", opset.MAP)
	tx.Put(alice, "name", "Alice")
	tx.Put(alice, "age", 22)

	bob, _ := tx.InsertObject(queue, 0, opset.MAP)
	tx.Put(bob, "name", "Bob")
	tx.Put(bob, "age", 23)
	doc1.CommitTransaction()

	doc2 := doc1.Fork()
	tx2 := doc2.StartTransaction()
	tx2.Move(leader, queue, "person", 0)
	doc2.CommitTransaction()

	tx3 := doc1.StartTransaction()
	tx3.Move(leader, queue, "person", 1)
	doc1.CommitTransaction()

	doc1.Merge(doc2)
	doc2.Merge(doc1)

	// either alice is moved before bob or after bob, but not both
	output1 := []ExOpId{alice, bob}
	output2 := []ExOpId{bob, alice}

	tx4 := doc1.StartTransaction()
	tx5 := doc2.StartTransaction()

	for i := 0; i < 2; i++ {
		value1, _ := tx4.Get(queue, i)
		value2, _ := tx5.Get(queue, i)
		assert.True(t, (value1 == output1[i] && value2 == output1[i]) || (value1 == output2[i] && value2 == output2[i]))
	}
}

func TestCycleDetectionInNestedListAndMap(t *testing.T) {
	id, _ := uuid.NewRandom()
	doc := NewAutomerge(id)
	tx := doc.StartTransaction()

	team, _ := tx.PutObject(ExRootOpId, "team", opset.MAP)
	carol, _ := tx.PutObject(team, "TopLeader", opset.MAP)
	tx.Put(carol, "name", "Carol")
	tx.Put(carol, "age", 24)
	followers, _ := tx.PutObject(carol, "followers", opset.LIST)

	alice, _ := tx.InsertObject(followers, 0, opset.MAP)
	tx.Put(alice, "name", "Alice")
	tx.Put(alice, "age", 22)

	bob, _ := tx.InsertObject(followers, 1, opset.MAP)
	tx.Put(bob, "name", "Bob")
	tx.Put(bob, "age", 23)

	// this will not take effect because of cycle detection
	tx.Move(team, alice, "TopLeader", "follower")
	//fmt.Println(doc.ops.Visualize())

	expectCarol, _ := tx.Get(team, "TopLeader")
	assert.Equal(t, expectCarol, carol)

	_, err := tx.Get(alice, "follower")

	assert.Equal(t, err, errors.PropertyNotFoundError{PropertyName: "follower"})
}

func TestSequentialMove(t *testing.T) {
	id1, _ := uuid.NewRandom()
	doc1 := NewAutomerge(id1)
	tx := doc1.StartTransaction()
	A, _ := tx.PutObject(ExRootOpId, "A", opset.MAP)
	B, _ := tx.PutObject(ExRootOpId, "B", opset.MAP)
	C, _ := tx.PutObject(ExRootOpId, "C", opset.MAP)
	tx.Put(A, "a1", "1")
	tx.Put(A, "a2", "2")
	tx.Put(A, "a3", "3")
	doc1.CommitTransaction()

	doc2 := doc1.Fork()
	tx2 := doc2.StartTransaction()
	_ = tx2.Move(A, B, "a1", "b1")
	//fmt.Println(doc2.ops.Visualize())
	_ = tx2.Move(B, C, "b1", "c2")
	//fmt.Println(doc2.ops.Visualize())
	doc2.CommitTransaction()

	tx3 := doc1.StartTransaction()
	_ = tx3.Move(A, C, "a1", "c1")
	//fmt.Println(doc1.ops.Visualize())
	doc1.CommitTransaction()

	doc1.Merge(doc2)
	doc2.Merge(doc1)

	// either a1 is moved to c1 or c2, but not both
	tx4 := doc1.StartTransaction()
	//fmt.Println(doc1.ops.Visualize())

	location1, err1 := tx4.Get(B, "b1")
	location2, err2 := tx4.Get(C, "c2")
	location3, err3 := tx4.Get(C, "c1")

	assert.Equal(t, err1, errors.PropertyNotFoundError{PropertyName: "b1"}) // not location 1
	assert.Equal(t, location1, nil)
	if err2 != nil { // moved to location 3
		assert.Equal(t, err2, errors.PropertyNotFoundError{PropertyName: "c2"})
		assert.Equal(t, location3, "1")
	} else { // moved to location 2
		assert.Equal(t, err3, errors.PropertyNotFoundError{PropertyName: "c1"})
		assert.Equal(t, location2, "1")
	}

}

func TestConcurrentDeletion(t *testing.T) {
	id1, _ := uuid.NewRandom()
	doc1 := NewAutomerge(id1)
	tx1 := doc1.StartTransaction()
	A, _ := tx1.PutObject(ExRootOpId, "A", opset.MAP)
	B, _ := tx1.PutObject(ExRootOpId, "B", opset.MAP)
	C, _ := tx1.PutObject(B, "C", opset.MAP)
	doc1.CommitTransaction()

	doc2 := doc1.Fork()
	doc3 := doc1.Fork()

	// delete C in doc1
	tx2 := doc1.StartTransaction()
	tx2.Delete(B, "C")
	doc1.CommitTransaction()

	// move A to be a child of C in doc2
	tx3 := doc2.StartTransaction()
	tx3.Move(ExRootOpId, C, "A", "A")
	doc2.CommitTransaction()

	// move B to be a child of A in doc3
	tx4 := doc3.StartTransaction()
	tx4.Move(ExRootOpId, A, "B", "B")
	doc3.CommitTransaction()

	// merge doc2 and doc1
	doc1.Merge(doc2)

	// merge doc3 and doc1
	doc1.Merge(doc3)

	// A, B and C should not exist in doc1
	tx5 := doc1.StartTransaction()
	_, err := tx5.Get(ExRootOpId, "C")
	assert.Equal(t, err, errors.PropertyNotFoundError{PropertyName: "C"})
	//fmt.Println(doc1.ops.Visualize())
}

func TestConcurrentDeletion2(t *testing.T) {
	id1, _ := uuid.NewRandom()
	doc1 := NewAutomerge(id1)
	tx1 := doc1.StartTransaction()
	tx1.PutObject(ExRootOpId, "A", opset.MAP)
	B, _ := tx1.PutObject(ExRootOpId, "B", opset.MAP)
	C, _ := tx1.PutObject(B, "C", opset.MAP)
	doc1.CommitTransaction()

	doc2 := doc1.Fork()

	// move B to be a child of C in doc1, introducing a cycle
	tx2 := doc1.StartTransaction()
	tx2.Move(ExRootOpId, C, "B", "B")
	doc1.CommitTransaction()

	// delete C in doc2
	tx3 := doc2.StartTransaction()
	tx3.Delete(B, "C")
	doc2.CommitTransaction()

	// merge doc2 and doc1
	doc1.Merge(doc2)
	doc2.Merge(doc1)

	// A should exist in doc1 and doc2
	tx4 := doc1.StartTransaction()
	_, err1 := tx4.Get(ExRootOpId, "A")
	assert.Equal(t, err1, nil)
	tx5 := doc2.StartTransaction()
	_, err2 := tx5.Get(ExRootOpId, "A")
	assert.Equal(t, err2, nil)

	// B may or may not exist in doc1 and doc2
	_, err3 := tx4.Get(ExRootOpId, "B")
	_, err4 := tx5.Get(ExRootOpId, "B")
	assert.Equal(t, err3, err4)
	if err3 != nil {
		assert.Equal(t, err3, errors.PropertyNotFoundError{PropertyName: "B"})
	}
}

func TestBenchmarkMove(t *testing.T) {
	id1, _ := uuid.NewRandom()
	doc1 := NewAutomerge(id1)
	tx := doc1.StartTransaction()
	A, _ := tx.PutObject(ExRootOpId, "A", opset.MAP)
	B, _ := tx.PutObject(ExRootOpId, "B", opset.MAP)
	tx.PutObject(ExRootOpId, "C", opset.MAP)
	tx.Put(A, "a1", "1")
	tx.Put(A, "a2", "2")
	tx.Put(A, "a3", "3")

	tx.MoveObject(A, B)

	movedA, _ := tx.Get(B, "A")
	movedAId := movedA.(ExOpId)
	moveda1, _ := tx.Get(movedAId, "a1")
	assert.Equal(t, "1", moveda1)

	_, err := tx.Get(ExRootOpId, "A")
	assert.Equal(t, errors.PropertyNotFoundError{PropertyName: "A"}, err)

	doc1.CommitTransaction()
	//fmt.Println(doc1.ops.Visualize())

}
