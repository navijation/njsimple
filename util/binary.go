package util

import (
	"encoding/binary"
	"io"

	"github.com/google/uuid"
)

type Word64 [8]byte

func (me Word64) Uint64() uint64 {
	return binary.BigEndian.Uint64(me[:])
}

func (me Word64) FromUint64(v uint64) Word64 {
	binary.BigEndian.PutUint64(me[:], v)
	return me
}

func (me Word64) Read(reader io.Reader) (Word64, error) {
	var word Word64
	_, err := io.ReadAtLeast(reader, word[:], len(word))
	if err != nil {
		return Word64{}, err
	}
	return word, nil
}

func Uint64ToWord64(v uint64) (out Word64) {
	binary.BigEndian.PutUint64(out[:], v)
	return out
}

func Uint64FromWord64(v Word64) uint64 {
	return binary.BigEndian.Uint64(v[:])
}

func ReadUint64(reader io.Reader) (value uint64, n int, _ error) {
	var word Word64
	n, err := io.ReadAtLeast(reader, word[:], len(word))
	if err != nil {
		return 0, n, err
	}
	return Uint64FromWord64(word), n, nil
}

func ReadUint64s(reader io.Reader, vs ...*uint64) (n int, _ error) {
	for _, v := range vs {
		value, dn, err := ReadUint64(reader)
		n += dn
		if err != nil {
			return n, err
		}
		*v = value
	}
	return n, nil
}

func WriteUint64(writer io.Writer, v uint64) (n int, _ error) {
	word := Uint64ToWord64(v)
	return writer.Write(word[:])
}

func WriteUint64s(writer io.Writer, vs ...uint64) (n int, _ error) {
	for _, v := range vs {
		dn, err := WriteUint64(writer, v)
		n += dn
		if err != nil {
			return n, err
		}
	}

	return n, nil
}

func NewRandomUUIDBytes() (out [16]byte) {
	uuidBytes, _ := uuid.Must(uuid.NewRandom()).MarshalBinary()
	copy(out[:], uuidBytes)
	return out
}

func UUIDFromBytes(bytes [16]byte) uuid.UUID {
	return uuid.Must(uuid.FromBytes(bytes[:]))
}
