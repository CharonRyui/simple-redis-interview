package redis

import (
	"log"
	"strings"
	"sync"
)

// store owns all Hash data. Returned responses are independent of its maps.
// A single lock covers each complete command, including multi-field writes.
type store struct {
	mu        sync.RWMutex
	hashes    map[string]map[string]string
	journal   journal
	writeErr  error
	usedBytes int64
	maxBytes  int64              // zero means unlimited; this is accounting, not process RSS
	save      func(*store) error // called while holding the write lock
}

func newStore() *store {
	return &store{hashes: make(map[string]map[string]string)}
}

func (s *store) execute(args []string) response {
	if len(args) == 0 {
		return failure("ERR empty command")
	}
	cmd := strings.ToUpper(args[0])
	argc := len(args)
	valid := false
	switch cmd {
	case "SAVE":
		if argc != 1 {
			return failure("ERR wrong number of arguments for 'save' command")
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.writeErr != nil || s.save == nil {
			return failure("MISCONF snapshot unavailable")
		}
		if err := s.save(s); err != nil {
			log.Printf("snapshot failed: %v", err)
			return failure("ERR snapshot save failed")
		}
		return response{kind: '+', text: "OK"}
	case "PING":
		if argc == 1 {
			return response{kind: '+', text: "PONG"}
		}
		if argc == 2 {
			return bulk(args[1])
		}
	case "HSET":
		valid = argc >= 4 && argc%2 == 0
	case "HGET", "HEXISTS":
		valid = argc == 3
	case "HDEL":
		valid = argc >= 3
	case "HGETALL", "HLEN":
		valid = argc == 2
	case "DEL":
		valid = argc >= 2
	default:
		return failure("ERR unknown command")
	}
	if !valid {
		return failure("ERR wrong number of arguments for '" + strings.ToLower(cmd) + "' command")
	}

	switch cmd {
	case "HSET", "HDEL", "DEL":
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.writeErr != nil {
			return failure("MISCONF persistence unavailable; writes disabled until restart")
		}
		if cmd == "HSET" && s.maxBytes > 0 && s.usedBytes+s.hsetDelta(args) > s.maxBytes {
			return failure("OOM storage limit exceeded")
		}
		if s.journal != nil {
			if err := s.journal.append(args); err != nil {
				s.writeErr = err
				log.Printf("AOF write/sync failed; writes disabled: %v", err)
				return failure("MISCONF persistence failed; write not applied to memory; writes disabled until restart")
			}
		}
	default:
		s.mu.RLock()
		defer s.mu.RUnlock()
	}

	hash := s.hashes[args[1]]
	switch cmd {
	case "HSET":
		s.usedBytes += s.hsetDelta(args)
		if hash == nil {
			hash = make(map[string]string)
			s.hashes[args[1]] = hash
		}
		added := 0
		for i := 2; i < argc; i += 2 {
			if _, exists := hash[args[i]]; !exists {
				added++
			}
			hash[args[i]] = args[i+1]
		}
		return integer(added)
	case "HGET":
		if value, exists := hash[args[2]]; exists {
			return bulk(value)
		}
		return response{}
	case "HEXISTS":
		if _, exists := hash[args[2]]; exists {
			return integer(1)
		}
		return integer(0)
	case "HDEL":
		deleted := 0
		for _, field := range args[2:] {
			if value, exists := hash[field]; exists {
				s.usedBytes -= fieldBytes(field, value)
				delete(hash, field)
				deleted++
			}
		}
		if len(hash) == 0 && hash != nil {
			s.usedBytes -= keyBytes(args[1])
			delete(s.hashes, args[1])
		}
		return integer(deleted)
	case "HGETALL":
		items := make([]response, 0, 2*len(hash))
		for field, value := range hash {
			items = append(items, bulk(field), bulk(value))
		}
		return response{kind: '*', items: items}
	case "HLEN":
		return integer(len(hash))
	case "DEL":
		deleted := 0
		for _, key := range args[1:] {
			if hash, exists := s.hashes[key]; exists {
				s.usedBytes -= keyBytes(key)
				for field, value := range hash {
					s.usedBytes -= fieldBytes(field, value)
				}
				delete(s.hashes, key)
				deleted++
			}
		}
		return integer(deleted)
	}
	panic("validated command has no implementation")
}
