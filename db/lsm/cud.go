package lsm

import "github.com/navijation/njsimple/storage/keyvaluepair"

func (me *LSMDB) Upsert(key, value []byte) error {
	ctx := &dbCtx{}

	ctx.Lock(&me.lock)
	defer ctx.Unlock(&me.lock)

	if err := me.checkStateErrorSafe(ctx); err != nil {
		return err
	}

	entry := CUDKeyValueEntry{
		StoredKeyValuePair: (&keyvaluepair.KeyValuePair{
			Key:   key,
			Value: value,
		}).ToStoredKeyValuePair(),
	}
	if err := me.appendEntry(ctx, &entry); err != nil {
		return err
	}

	me.processCUDKeyValueEntry(ctx, entry)
	return nil
}

func (me *LSMDB) Delete(key []byte) error {
	ctx := &dbCtx{}

	ctx.Lock(&me.lock)
	defer ctx.Unlock(&me.lock)

	if me.stateErr != nil {
		return me.stateErr
	}

	entry := CUDKeyValueEntry{
		StoredKeyValuePair: (&keyvaluepair.KeyValuePair{
			Key:       key,
			IsDeleted: true,
		}).ToStoredKeyValuePair(),
	}

	if err := me.appendEntry(ctx, &entry); err != nil {
		me.stateErr = err
		return err
	}
	me.processCUDKeyValueEntry(ctx, entry)
	return nil
}

func (me *LSMDB) processCUDKeyValueEntry(ctx *dbCtx, entry CUDKeyValueEntry) {
	ctx.Lock(&me.lock)
	defer ctx.Unlock(&me.lock)

	me.inMemoryIndexes[0].Upsert(entry.StoredKeyValuePair.ToKeyValuePair())
}
