package queue

import (
	"container/list"
	"sync"
	class "github.com/matehaxor03/holistic_db_client/class"
)


type Queue struct {
	PushFront func(message *(class.Map)) *(class.Map)
	PushBack func(message *(class.Map)) *(class.Map)
	Len func() uint64
}

func NewQueue() (*Queue) {
	var lock sync.Mutex
	l := list.New()

	x := Queue{
		PushFront: func(message *(class.Map)) *(class.Map) {
			lock.Lock()
			defer lock.Unlock()
			l.PushFront(message)
			return message
		},
		PushBack: func(message *(class.Map)) *(class.Map) {
			lock.Lock()
			defer lock.Unlock()
			l.PushBack(message)
			return message
		},
		Len: func() uint64 {
			lock.Lock()
			defer lock.Unlock()
			length := uint64(l.Len())
			return length
		},
	}

	return &x
}