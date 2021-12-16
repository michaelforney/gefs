#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"

vlong
inc64(uvlong *v, uvlong dv)
{
	vlong ov, nv;

	while(1){
		ov = *v;
		nv = ov + dv;
		if(cas64(v, ov, nv))
			return nv;
	}
}

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
opensnap(Tree *t, char *name)
{
	char dbuf[Keymax], buf[Kvmax];
	char *p, *e;
	int n;
	Key k;
	Kvp kv;

	n = strlen(name);
	p = dbuf;
	p[0] = Kdset;			p += 1;
	memcpy(p, name, n);		p += n;
	k.k = dbuf;
	k.nk = p - dbuf;
	if((e = btlookup(&fs->snap, &k, &kv, buf, sizeof(buf))) != nil)
		return e;
	memmove(dbuf, kv.v, kv.nv);
	k.k = dbuf;
	k.nk = kv.nv;
	if((e = btlookup(&fs->snap, &k, &kv, buf, sizeof(buf))) != nil)
		return e;
	p = kv.v;
	t->ht = GBIT32(p);		p += 4;
	t->bp.addr = GBIT64(p);		p += 8;
	t->bp.hash = GBIT64(p);		p += 8;
	t->bp.gen = GBIT64(p);		p += 8;
	t->dp.addr = GBIT64(p);		p += 8;
	t->dp.hash = GBIT64(p);		p += 8;
	t->dp.gen = GBIT64(p);
	return nil;
}

char*
snapshot(Tree *r, char *name, int update)
{
	char dbuf[Keymax], snapbuf[Snapsz], treebuf[Treesz];
	char *p, *e;
	uvlong gen;
	int n;
	Msg m[2];

	n = strlen(name);
	if(update)
		gen = inc64(&fs->nextgen, 0);
	else
		gen = inc64(&fs->nextgen, 1);

	p = dbuf;
	m[0].op = Oinsert;
	p[0] = Kdset;		p += 1;
	memcpy(p, name, n);	p += n;
	m[0].k = dbuf;
	m[0].nk = p - dbuf;

	p = snapbuf;
	p[0] = Ksnap;		p += 1;
	PBIT64(p, gen);		p += 8;
	m[0].v = snapbuf;
	m[0].nv = p - snapbuf;

	m[1].op = Oinsert;
	m[1].k = snapbuf;
	m[1].nk = p - snapbuf;
	p = treebuf;
	PBIT32(p, r->ht);	p += 4;
	PBIT64(p, r->bp.addr);	p += 8;
	PBIT64(p, r->bp.hash);	p += 8;
	PBIT64(p, r->bp.gen);	p += 8;
	PBIT64(p, r->dp.addr);	p += 8;
	PBIT64(p, r->dp.hash);	p += 8;
	PBIT64(p, r->dp.gen);	p += 8;
	m[1].v = treebuf;
	m[1].nv = p - treebuf;
	if((e = btupsert(&fs->snap, m, nelem(m))) != nil)
		return e;
	if(sync() == -1)
		return Eio;
	return 0;
}

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
		finalize(a->log.tail);
		if(syncblk(a->log.tail) == -1)
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

void
quiesce(int tid)
{
	int i, allquiesced;
	Bfree *p, *n;

	lock(&fs->activelk);
	allquiesced = 1;
	fs->active[tid]++;
	for(i = 0; i < fs->nproc; i++){
		/*
		 * Odd parity on quiescence implies
		 * that we're between the exit from
		 * and waiting for the next message
		 * that enters us into the critical
		 * section.
		 */
		if((fs->active[i] & 1) == 0)
			continue;
		if(fs->active[i] == fs->lastactive[i])
			allquiesced = 0;
	}
	if(allquiesced)
		for(i = 0; i < fs->nproc; i++)
			fs->lastactive[i] = fs->active[i];
	unlock(&fs->activelk);
	if(!allquiesced)
		return;

	lock(&fs->freelk);
	p = nil;
	if(fs->freep != nil){
		p = fs->freep->next;
		fs->freep->next = nil;
	}
	unlock(&fs->freelk);

	while(p != nil){
		n = p->next;
		reclaimblk(p->bp);
		p = n;
	}
	fs->freep = fs->freehd;
}
