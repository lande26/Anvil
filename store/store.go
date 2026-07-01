// Package store provides the embedded in-memory data store for Anvil.
// It is a stripped-down port of Valkyr's store package, retaining only
// the three data types Anvil needs: strings, hashes, and lists.
// All other Valkyr features (pub/sub, sorted sets, TTL/expiry, RDB
// snapshots, RESP parser, TCP server) are intentionally omitted.
package store

// Store is the top-level aggregate holding all sub-stores.
// It is safe to use concurrently — each sub-store owns its own mutex.
type Store struct {
	Strings *StringStore
	Hashes  *HashStore
	Lists   *ListStore
}

// New creates a new Store with all sub-stores initialized.
func New() *Store {
	return &Store{
		Strings: NewStringStore(),
		Hashes:  NewHashStore(),
		Lists:   NewListStore(),
	}
}
