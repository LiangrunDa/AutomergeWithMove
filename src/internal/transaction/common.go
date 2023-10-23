package transaction

type ChangeHash [32]byte

func (ch ChangeHash) Equal(other ChangeHash) bool {
	for i := 0; i < len(ch); i++ {
		if ch[i] != other[i] {
			return false
		}
	}
	return true
}
