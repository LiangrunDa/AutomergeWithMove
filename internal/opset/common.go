package opset

type ActionType uint8

const (
	MAKE ActionType = iota
	DELETE
	PUT
	MOVE
)

func (actionType ActionType) String() string {
	switch actionType {
	case MAKE:
		return "MAKE"
	case DELETE:
		return "DELETE"
	case PUT:
		return "PUT"
	case MOVE:
		return "MOVE"
	}
	return ""
}
