package set

import (
	"time"

	"github.com/emirpasic/gods/sets/hashset"
	"github.com/emirpasic/gods/sets/treeset"
)

// Set is a data structure implementation of ordered or unordered
// collection of unique elements that should insert/delete in O(1) if unordered
// and insert/delete in O(logn) if ordered
type Set interface {
	Add(items ...interface{})
	Remove(items ...interface{})
	Values() []interface{}
	String() string
	Clear()
	Size() int
	Empty() bool
	Contains(items ...interface{}) bool
}

// Comparator will make type assertion (see IntComparator for example),
// which will panic if a or b are not of the asserted type.
//
// Should return a number:
//    negative , if a < b
//    zero     , if a == b
//    positive , if a > b
type Comparator func(a, b interface{}) int

// NewHashSet instantiates a new empty set of unordered elements
func NewHashSet() *hashset.Set {
	return hashset.New()
}

// NewTreeSetWithTimeComparator instantiates a new empty set of ordered elements that contain time.Time
func NewTreeSetWithTimeComparator() *treeset.Set {
	return treeset.NewWith(timeComparator)
}

func NewTreeSetWith(comp func(a, b interface{}) int) *treeset.Set {
	return treeset.NewWith(comp)
}

func timeComparator(a, b interface{}) int {
	aAsserted := a.(time.Time)
	bAsserted := b.(time.Time)
	switch {
	case aAsserted.After(bAsserted):
		return 1
	case aAsserted.Before(bAsserted):
		return -1
	default:
		return 0
	}
}
