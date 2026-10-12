package redis

// Fixed accounting overhead also bounds the number of empty keys/fields. It is
// deliberately independent of Go's map implementation and allocator behavior.
const (
	keyOverhead           = 64
	fieldOverhead         = 64
	DefaultMaxBytes int64 = 64 << 20
)

func keyBytes(key string) int64 { return int64(len(key)) + keyOverhead }
func fieldBytes(field, value string) int64 {
	return int64(len(field)) + int64(len(value)) + fieldOverhead
}

// hsetDelta projects the final command state without mutating data. Only the
// last value of each repeated field counts; transient intermediate values do not.
func (s *store) hsetDelta(args []string) int64 {
	hash := s.hashes[args[1]]
	var delta int64
	if hash == nil {
		delta = keyBytes(args[1])
	}
	seen := make(map[string]struct{}, (len(args)-2)/2)
	for i := len(args) - 2; i >= 2; i -= 2 {
		field, value := args[i], args[i+1]
		if _, ok := seen[field]; ok {
			continue
		}
		seen[field] = struct{}{}
		if old, exists := hash[field]; exists {
			delta += int64(len(value)) - int64(len(old))
		} else {
			delta += fieldBytes(field, value)
		}
	}
	return delta
}
