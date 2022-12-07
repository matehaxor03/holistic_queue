package queue

import (
	"container/list"
	"sync"
	json "github.com/matehaxor03/holistic_json/json"
)


type Queue struct {
	PushFront func(message *(json.Map)) *(json.Map)
	PushBack func(message *(json.Map)) *(json.Map)
	GetAndRemoveFront func() *(json.Map)
	Len func() uint64
}

func NewQueue() (*Queue) {
	var lock sync.Mutex
	l := list.New()

	x := Queue{
		PushFront: func(message *(json.Map)) *(json.Map) {
			lock.Lock()
			defer lock.Unlock()
			l.PushFront(message)
			return message
		},
		PushBack: func(message *(json.Map)) *(json.Map) {
			lock.Lock()
			defer lock.Unlock()
			l.PushBack(message)
			return message
		},
		GetAndRemoveFront: func() *(json.Map) {
			lock.Lock()
			defer lock.Unlock()
			message := l.Front()

			if message == nil {
				return nil
			}
		
			l.Remove(message)
			return message.Value.(*(json.Map))
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