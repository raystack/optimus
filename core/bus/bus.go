package bus

// allows independent components of an application to
// observe events produced by decoupled producers
//
// producer of "someevent"
// 	   bus.Post("someevent", "data")
//
// observer of "someevent"
//     myChan := make(chan string)
//     bus.Listen("someevent", myChan)
//     for {
//         data := <-myChan
//         fmt.Printf("someevent: %s", data)
//     }
//
// make sure these events are unique

import (
	"errors"
	"sync"
)

var (
	ErrNotFound = errors.New("not found")
)

var (
	// mapping of event to listening channels
	eventBus = make(map[string][]chan<- interface{})
	rwMutex  sync.RWMutex
)

// Listen observing the specified event via provided channel
func Listen(event string, out chan interface{}) {
	rwMutex.Lock()
	defer rwMutex.Unlock()
	eventBus[event] = append(eventBus[event], out)
}

// Stop observing the specified event on the channel
func Stop(event string, out chan interface{}) error {
	rwMutex.Lock()
	defer rwMutex.Unlock()

	newEventBus := make([]chan<- interface{}, 0)
	outChans, ok := eventBus[event]
	if !ok {
		return ErrNotFound
	}
	for _, ch := range outChans {
		if ch != out {
			newEventBus = append(newEventBus, ch)
		}
	}
	eventBus[event] = newEventBus

	return nil
}

// Post a notification to the specified event
func Post(event string, data interface{}) error {
	rwMutex.RLock()
	defer rwMutex.RUnlock()

	if listeners, ok := eventBus[event]; ok {
		//push this to all listeners
		for _, out := range listeners {
			out <- data
		}
	} else {
		return ErrNotFound
	}
	return nil
}
