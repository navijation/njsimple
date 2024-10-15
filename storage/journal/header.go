package journal

import (
	"hash"
	"io"

	"github.com/google/uuid"
	"github.com/navijation/njsimple/util"
)

type journalFileHeader struct {
	id    [16]byte
	start uint64
}

func (me *journalFileHeader) Read(reader io.Reader) error {
	var idWord [16]byte
	_, err := io.ReadAtLeast(reader, idWord[:], len(idWord))
	if err != nil {
		return err
	}

	me.id = uuid.Must(uuid.FromBytes(idWord[:]))

	startWord, err := util.Word64{}.Read(reader)
	if err != nil {
		return err
	}
	me.start = startWord.Uint64()

	return nil
}

func (me *journalFileHeader) WriteTo(writer io.Writer) (n int64, err error) {
	if dn, err := writer.Write(me.id[:]); err != nil {
		return n + int64(dn), err
	} else {
		n += int64(dn)
	}

	// try writing everything
	if dn, err := util.WriteUint64(writer, me.start); err != nil {
		return n + int64(dn), err
	} else {
		n += int64(dn)
	}

	return n, nil
}

func (me *journalFileHeader) WriteHash(h hash.Hash) {
	_, err := me.WriteTo(h)
	util.AssertNoError(err)
}

func (me *journalFileHeader) SizeOf() uint64 {
	return 24
}
