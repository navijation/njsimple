package keyvaluepair

const (
	tombstoneMask = (uint64)(1) << 63
	keySizeMask   = ^tombstoneMask
)

// Directly serde-able key-value pair. The binary representation is as follows.
// ______________________________________________________________________________
// | 1 bit     |   63 bits  | (key size) bytes | 8 bytes    | (value size) bytes |
// |-----------------------------------------------------------------------------|
// | tombstone |   key size |     key          | value size |      value         |
// |-----------------------------------------------------------------------------|
type StoredKeyValuePair struct {
	keySizeAndTombstone uint64
	ValueSize           uint64
	Key                 []byte
	Value               []byte
}

// Higher-level DTO for passing around key-value pairs conveniently
type KeyValuePair struct {
	Key       []byte
	Value     []byte
	IsDeleted bool
}
