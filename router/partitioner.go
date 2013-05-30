package router



// This partitions a the input string into an int between 0 (inclusive) and max (exclusive) 
type Partitioner interface {
    Partition(input string, max int) (int, error)
}


type DefaultPartitioner struct {
}

func (this *DefaultPartitioner) Partition(input string, max int) (int, error) {
    return 0, nil
}
