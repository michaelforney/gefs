#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"

int
syncblk(Blk *b)
{
	assert(b->flag & Bfinal);
	lock(b);
	b->flag &= ~(Bqueued|Bdirty);
	unlock(b);
	return pwrite(fs->fd, b->buf, Blksz, b->bp.addr);
}

void
enqueue(Blk *b)
{
	assert(b->flag&Bdirty);
	finalize(b);
	if(syncblk(b) == -1){
		ainc(&fs->broken);
		fprint(2, "write: %r");
		abort();
	}
}


char*
snapshot(Mount *mnt)
{
	char *e;

	mnt->m.op = Oinsert;
//	mnt->m.k[0] = Ksnap;
//	PBIT64(mnt->m.k +  1, fs->nextgen++);
	PBIT32(mnt->m.v +  0, mnt->root.ht);
	PBIT64(mnt->m.v +  4, mnt->root.bp.addr);
	PBIT64(mnt->m.v + 12, mnt->root.bp.hash);
	PBIT64(mnt->m.v + 20, mnt->root.bp.gen);
	PBIT64(mnt->m.v + 28, mnt->dead.addr);
	PBIT64(mnt->m.v + 36, mnt->dead.hash);
	PBIT64(mnt->m.v + 42, mnt->dead.gen);
	if((e = btupsert(&fs->snap, &mnt->m, 1)) != nil)
		return e;
	if(sync() == -1)
		return Eio;
	return 0;
}

int
sync(void)
{
	int i, r;
	Arena *a;
	Blk *b, *s;

	qlock(&fs->snaplk);
	r = 0;
	s = fs->super;
	fillsuper(s);
	enqueue(s);

	for(i = 0; i < fs->narena; i++){
		a = &fs->arenas[i];
		finalize(a->logtl);
		if(syncblk(a->logtl) == -1)
			r = -1;
	}
	for(b = fs->chead; b != nil; b = b->cnext){
		if(!(b->flag & Bdirty))
			continue;
		if(syncblk(b) == -1)
			r = -1;
	}
	if(r != -1)
		r = syncblk(s);

	qunlock(&fs->snaplk);
	return r;
}
