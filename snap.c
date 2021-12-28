#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"

uvlong
inc64(uvlong *v, uvlong dv)
{
	vlong ov, nv;

	while(1){
		ov = *v;
		nv = ov + dv;
		if(cas64((u64int*)v, ov, nv))
			return ov;
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

Tree*
opensnap(char *name)
{
	char dbuf[Keymax], buf[Kvmax];
	char *p, *e;
	vlong id;
	Tree *t;
	u32int h;
	int n;
	Key k;
	Kvp kv;

	qlock(&fs->snaplk);
	n = strlen(name);
	p = dbuf;
	p[0] = Klabel;			p += 1;
	memcpy(p, name, n);		p += n;
	k.k = dbuf;
	k.nk = p - dbuf;
	if((e = btlookup(&fs->snap, &k, &kv, buf, sizeof(buf))) != nil)
		goto Error;
	id = GBIT64(kv.v + 1);
	h = ihash(id) % Nttab;
	for(t = fs->ttab[h]; t != nil; t = t->hnext){
		if(t->gen == id){
			ainc(&t->memref);
			qunlock(&fs->snaplk);
			return t;
		}
	}
	if((t = mallocz(sizeof(Tree), 1)) == nil){
		fprint(2, "opensnap: %s\n", e);
		goto Error;
	}
	memmove(dbuf, kv.v, kv.nv);
	k.k = dbuf;
	k.nk = kv.nv;
	if((e = btlookup(&fs->snap, &k, &kv, buf, sizeof(buf))) != nil){
		fprint(2, "opensnap: %s\n", e);
		goto Error;
	}
	if(unpacktree(t, kv.v, kv.nv) == nil)
		goto Error;
	qunlock(&fs->snaplk);
	return t;
Error:
	qunlock(&fs->snaplk);
	return nil;
}

void
closesnap(Tree *t)
{
	Tree *te, **p;
	u32int h;

	if(adec(&t->memref) != 0)
		return;

	qlock(&fs->snaplk);
	h = ihash(t->gen) % Nttab;
	p = &fs->ttab[h];
	for(te = fs->ttab[h]; te != nil; te = te->hnext){
		if(te == t)
			break;
		p = &te->hnext;
	}
	assert(te != nil);
	*p = te->hnext;
	free(te);
}

static char*
modifysnap(char *name, vlong gen, vlong next, int del)
{
	char dbuf[Keymax], sbuf[Snapsz], nbuf[Snapsz];
	char *p, *e;
	int n, nm;
	Msg m[2];

	nm = 0;

	p = sbuf;
	p[0] = Ksnap;		p += 1;
	PBIT64(p, gen);		p += 8;
	m[nm].op = del ? Ounrefsnap : Orefsnap;
	m[nm].k = sbuf;
	m[nm].nk = p - sbuf;

	p = nbuf;
	p[0] = Ksnap;		p += 1;
	PBIT64(p, next);	p += 8;
	m[nm].v = nbuf;
	m[nm].nv = p - nbuf;

	nm++;
	if(name != nil){
		p = dbuf;
		n = strlen(name);
		m[nm].op = del ? Odelete : Oinsert;
		p[0] = Klabel;		p += 1;
		memcpy(p, name, n);	p += n;
		m[nm].k = dbuf;
		m[nm].nk = p - dbuf;
		m[nm].v = m[nm-1].k;
		m[nm].nv = m[nm-1].nk;
		nm++;
	}
	if((e = btupsert(&fs->snap, m, nm)) != nil)
		return e;
	return nil;
}

char*
deletesnap(Tree *snap, Tree *from)
{
fprint(2, "deleting snap at %B from %B\n", snap->bp, from->bp);
	return nil;
}

char*
labelsnap(char *name, vlong gen)
{
	return modifysnap(name, gen, -1, 0);
}

char*
unlabelsnap(vlong gen, char *name)
{
	return modifysnap(name, gen, -1, 1);
}

char*
refsnap(vlong gen)
{
	return modifysnap(nil, gen, -1, 0);
}

char*
unrefsnap(vlong gen, vlong next)
{
	return modifysnap(nil, gen, next, 1);
}

void
freedead(Bptr bp, void *)
{
	reclaimblk(bp);
}

char*
freesnap(Tree *snap, Tree *next)
{
	Oplog dl;
	int i;
	assert(snap->gen != next->gen);
	assert(next->prev[0] == snap->gen);

	scandead(&next->dead[0], freedead, nil);
	for(i = 0; i < Ndead-1; i++){
		next->prev[i] = snap->prev[i];
		dl = snap->dead[i];
		if(i < Ndead-1)
			if(graft(&dl, &next->dead[i+1]) == -1)
				return Efs;
		next->dead[i] = dl;
	}
	return nil;
}

char*
newsnap(Tree *t, vlong *genp, vlong *oldp)
{
	char kbuf[Snapsz], vbuf[Treesz];
	char *p, *e;
	uvlong gen, old;
	Msg m;
	int i;

	qlock(&fs->snaplk);
	gen = inc64(&fs->nextgen, 1);
	for(i = 0; i < Ndead; i++){
		if(t->dead[i].tail != nil){
			finalize(t->dead[i].tail);
			syncblk(t->dead[i].tail);
			putblk(t->dead[i].tail);
			t->dead[i].tail = nil;
		}
	}

	/* shift deadlist down */
	if(t->dead[Ndead-1].tail != nil)
		putblk(t->dead[Ndead-1].tail);
	old = t->gen;
	for(i = Ndead-1; i >= 0; i--){
		t->prev[i] = i == 0 ? t->gen : t->prev[i-1];
		t->dead[i].head.addr = -1;
		t->dead[i].head.hash = -1;
		t->dead[i].head.gen = -1;
		t->dead[i].tail = nil;
	}
	t->gen = gen;

	p = kbuf;
	p[0] = Ksnap;	p += 1;
	PBIT64(p, gen);	p += 8;
	m.op = Oinsert;
	m.k = kbuf;
	m.nk = p - kbuf;
	p = packtree(vbuf, sizeof(vbuf), t, 0);
	m.v = vbuf;
	m.nv = p - vbuf;
	if((e = btupsert(&fs->snap, &m, 1)) != nil){
		qunlock(&fs->snaplk);
		return e;
	}

	*genp = gen;
	*oldp = old;
	qunlock(&fs->snaplk);
	return nil;
}
