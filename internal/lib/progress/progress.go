package progress

import "fmt"

// Event an event published by certain services
// that notify that a certain progress has been made
type Event interface {
	fmt.Stringer
}

// Observer is an entity that wishes to
// receive progress events
type Observer interface {
	Notify(evt Event)
}

// ObserverChain iterate on all the observers for notify
type ObserverChain struct {
	obs []Observer
}

// Notify each observer
func (chain *ObserverChain) Notify(evt Event) {
	for _, ob := range chain.obs {
		ob.Notify(evt)
	}
}

// Join will add observer to listen for notify events
func (chain *ObserverChain) Join(obs Observer) {
	chain.obs = append(chain.obs, obs)
}
