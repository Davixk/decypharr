package storage

import (
	"sort"
	"strings"

	"github.com/sirrobot01/appendstore"
)

// Case-canonical main-store keys.
//
// WHY THIS EXISTS. The queue store has always lowercased its keys; the main
// store keyed rows by the infohash string exactly as it arrived. One hash seen
// in two cases — an uppercase btih from a magnet, a lowercase hash from a
// provider, older import paths — became TWO main rows ("case twins"), each
// with its own name, placements and folder.
//
// THE RULE, for every hash-keyed main-store operation (Get, Exists, Delete,
// the Mutate* helpers and AddOrUpdate), applied by resolveMainKey:
//
//  1. the EXACT key, if a row has it;
//  2. otherwise the canonical (lowercase) key, if a row has it;
//  3. otherwise a legacy non-lowercase variant of the same hash, from an
//     in-memory index (lexicographically smallest if several, re-checked
//     against the store before use);
//  4. otherwise the canonical key — which is where a NEW row is written.
//
// Exact comes first on purpose. Until the operator merge has run, scanners
// (refresh, repair, conditional deletes, snapshot mutations) address a twin by
// its own stored key; resolving that key to the other twin would stale-fail
// every pass or, worse, write one twin's state into the other. With exact
// first, any key that exists behaves exactly as it always did, while a hash
// arriving in a new case finds the existing row instead of creating a twin.
// After the merge every row is lowercase and the steps collapse to "lowercase".
//
// No hash-keyed operation ever moves a row: a write to a legacy uppercase row
// lands IN PLACE at that key. Rows change key only through MergeCaseTwins.

// canonicalMainKey is the case-canonical form of a main-store key.
func canonicalMainKey(infohash string) string {
	return strings.ToLower(infohash)
}

// isInternalMainKey reports bookkeeping keys (such as __migration_status__)
// that are not entries and are never resolved or canonicalised.
func isInternalMainKey(key string) bool {
	return strings.HasPrefix(key, "__")
}

// resolveMainKey applies the rule above. Callers that write must hold the
// hash's entry mutation stripe so the answer cannot change before they use it.
func (s *Storage) resolveMainKey(infohash string) string {
	if infohash == "" || isInternalMainKey(infohash) {
		return infohash
	}
	if s.entries.Exists(infohash) {
		return infohash
	}
	canonical := canonicalMainKey(infohash)
	if canonical != infohash && s.entries.Exists(canonical) {
		return canonical
	}
	if legacy, ok := s.legacyMainKey(canonical); ok {
		return legacy
	}
	return canonical
}

// canonicalizeEntryIdentity points an entry, and the files that identify it,
// at the key its row is stored under. Files owned by a different hash (shared
// folder aliases) are left alone.
func canonicalizeEntryIdentity(entry *Entry, key string) {
	if entry == nil || key == "" {
		return
	}
	entry.InfoHash = key
	for _, file := range entry.Files {
		if file != nil && file.InfoHash != key && strings.EqualFold(file.InfoHash, key) {
			file.InfoHash = key
		}
	}
}

// rebuildLegacyMainKeys rebuilds the legacy-key index from the store's
// in-memory key index (no disk reads, no decoding).
func (s *Storage) rebuildLegacyMainKeys() {
	index := make(map[string]map[string]struct{})
	_ = s.entries.ForEachMetadata(func(key string, _ *appendstore.Metadata) error {
		if isInternalMainKey(key) {
			return nil
		}
		canonical := canonicalMainKey(key)
		if canonical == key {
			return nil
		}
		bucket := index[canonical]
		if bucket == nil {
			bucket = make(map[string]struct{}, 1)
			index[canonical] = bucket
		}
		bucket[key] = struct{}{}
		return nil
	})
	s.legacyMainKeysMu.Lock()
	s.legacyMainKeys = index
	s.legacyMainKeysMu.Unlock()
}

// rememberLegacyMainKey records a non-lowercase key that now exists in the
// store. Only a restore from a merge journal creates one.
func (s *Storage) rememberLegacyMainKey(key string) {
	canonical := canonicalMainKey(key)
	if canonical == key || isInternalMainKey(key) {
		return
	}
	s.legacyMainKeysMu.Lock()
	defer s.legacyMainKeysMu.Unlock()
	if s.legacyMainKeys == nil {
		s.legacyMainKeys = make(map[string]map[string]struct{})
	}
	bucket := s.legacyMainKeys[canonical]
	if bucket == nil {
		bucket = make(map[string]struct{}, 1)
		s.legacyMainKeys[canonical] = bucket
	}
	bucket[key] = struct{}{}
}

// forgetLegacyMainKey drops a key that was deleted from the store. A stale
// entry would let the next write resolve to, and re-create, a deleted
// uppercase row — the twin bug itself — so every delete path calls this.
func (s *Storage) forgetLegacyMainKey(key string) {
	canonical := canonicalMainKey(key)
	if canonical == key {
		return
	}
	s.legacyMainKeysMu.Lock()
	defer s.legacyMainKeysMu.Unlock()
	bucket := s.legacyMainKeys[canonical]
	if bucket == nil {
		return
	}
	delete(bucket, key)
	if len(bucket) == 0 {
		delete(s.legacyMainKeys, canonical)
	}
}

// legacyMainKey returns the legacy variant a lookup of canonical falls back to.
// Every candidate is confirmed against the store before it is returned, and an
// index entry whose key no longer exists is purged rather than trusted.
func (s *Storage) legacyMainKey(canonical string) (string, bool) {
	s.legacyMainKeysMu.Lock()
	defer s.legacyMainKeysMu.Unlock()
	bucket := s.legacyMainKeys[canonical]
	if len(bucket) == 0 {
		return "", false
	}
	candidates := make([]string, 0, len(bucket))
	for key := range bucket {
		candidates = append(candidates, key)
	}
	sort.Strings(candidates)
	for _, key := range candidates {
		if s.entries.Exists(key) {
			return key, true
		}
		delete(bucket, key)
	}
	delete(s.legacyMainKeys, canonical)
	return "", false
}

// legacyMainKeyCount reports how many legacy keys the index holds.
func (s *Storage) legacyMainKeyCount() int {
	s.legacyMainKeysMu.Lock()
	defer s.legacyMainKeysMu.Unlock()
	count := 0
	for _, bucket := range s.legacyMainKeys {
		count += len(bucket)
	}
	return count
}
