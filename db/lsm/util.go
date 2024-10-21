package lsm

import (
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/navijation/njsimple/util"
)

func (me *LSMDB) appendEntry(ctx *dbCtx, entry io.WriterTo) error {
	ctx.Lock(&me.lock)
	defer ctx.Unlock(&me.lock)

	bytes, _ := util.ToBytes(entry)
	if _, err := me.writeAheadLogs[0].AppendEntry(bytes); err != nil {
		return err
	}
	return nil
}

func (me *LSMDB) checkStateError(ctx *dbCtx) error {
	if !me.isRunning.Load() {
		return fmt.Errorf("database is not running")
	}

	select {
	case <-me.done:
		return fmt.Errorf("database is closed")
	default:
	}

	ctx.RLock(&me.lock)
	defer ctx.RUnlock(&me.lock)

	return me.stateErr
}

func (me *LSMDB) sstablePath(tableNumber uint64) string {
	return filepath.Join(me.path, fmt.Sprintf("sstable_%d.sst", tableNumber))
}

func (me *LSMDB) writeAheadLogPath(writeAheadLogNumber uint64) string {
	return filepath.Join(me.path, fmt.Sprintf("writeahead_log_%d.jrn", writeAheadLogNumber))
}

func getFileNumber(path, prefix, extension string) (num uint64, ok bool) {
	basename := filepath.Base(path)

	withoutExtension, ok := strings.CutSuffix(basename, extension)
	if !ok {
		return 0, false
	}

	withoutPrefix, ok := strings.CutPrefix(withoutExtension, prefix)
	if !ok {
		return 0, false
	}

	number, err := strconv.Atoi(withoutPrefix)
	if err != nil {
		return 0, false
	}

	return uint64(number), true
}
