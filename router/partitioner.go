package router

// This hashes a the input string into an int between 0 (inclusive) and max (exclusive)
type Hasher interface {
	Hash(input string, max int) (int, error)
}

type DefaultHasher struct {
}

func (this *DefaultHasher) Hash(input string, max int) (int, error) {
	return 0, nil
}
