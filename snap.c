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
	clrflag(b, Bqueued|Bdirty);
	return pwrite(fs->fd, b->buf, Blksz, b->bp.addr);
}

void
enqueue(Blk *b)
{
	assert(b->flag & Bdirty);
	finalize(b);
	if(syncblk(b) == -1){
		ainc(&fs->broken);
		fprint(2, "write: %r");
		abort();
	}
}

Tree*
openlabel(char *name)
{
	char dbuf[Keymax], buf[Kvmax];
	char *p, *e;
	vlong id;
	Tree *t;
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
	if(kv.nv != Snapsz)
		goto Error;

	id = GBIT64(kv.v + 1);
	for(t = fs->osnap; t != nil; t = t->snext){
		if(t->gen == id){
			ainc(&t->memref);
			qunlock(&fs->snaplk);
			return t;
		}
	}
	if((t = mallocz(sizeof(Tree), 1)) == nil){
		fprint(2, "open %s: %s\n", name, e);
		goto Error;
	}
	memset(&t->lk, 0, sizeof(t->lk));

	memmove(dbuf, kv.v, kv.nv);
	k.k = dbuf;
	k.nk = kv.nv;
	if((e = btlookup(&fs->snap, &k, &kv, buf, sizeof(buf))) != nil){
		fprint(2, "open %s: %s\n", name, e);
		goto Error;
	}
	if(unpacktree(t, kv.v, kv.nv) == nil)
		goto Error;
	t->memref = 1;
	t->snext = fs->osnap;
	fs->osnap = t;
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
	int i;

	if(adec(&t->memref) != 0)
		return;

	for(i = Ndead-1; i >= 0; i--){
		if(t->dead[i].tail == nil)
			continue;
		syncblk(t->dead[i].tail);
//FIXME		putblk(t->dead[i].tail);
	}

	p = &fs->osnap;
	for(te = fs->osnap; te != nil; te = te->snext){
		if(te == t)
			break;
		p = &te->snext;
	}
	assert(te != nil);
	*p = te->snext;
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
	dprint("reclaimed deadlist: %B\n", bp);
	reclaimblk(bp);
}

void
redeadlist(Bptr bp, void *pt)
{
	killblk(pt, bp);
}

char*
freesnap(Tree *snap, Tree *next)
{
	Oplog dl;
	int i;

	assert(snap->gen != next->gen);
	assert(next->prev[0] == snap->gen);

fprint(2, "next tree\n");
showtreeroot(2, next);
fprint(2, "snap tree\n");
showtreeroot(2, snap);

	dl = next->dead[Ndead-1];
	scandead(&next->dead[0], freedead, nil);
	for(i = 0; i < Ndead-2; i++){
		if(graft(&snap->dead[i], &next->dead[i+1]) == -1)
			return Efs;
		next->prev[i] = snap->prev[i];
		next->dead[i] = snap->dead[i];
	}
	for(; i < Ndead; i++){
		next->prev[i] = snap->prev[i];
		next->dead[i] = snap->dead[i];
	}
	scandead(&dl, redeadlist, next);

fprint(2, "transferred\n");
showtreeroot(2, next);
fprint(2, "==================================\n");
	return nil;
}

int
savesnap(Tree *t)
{
	char kbuf[Snapsz], vbuf[Treesz];
	char *p, *e;
	Msg m;
	int i;
	for(i = 0; i < Ndead; i++){
		if(t->dead[i].tail != nil){
			finalize(t->dead[i].tail);
			syncblk(t->dead[i].tail);
		}
	}
	p = kbuf;
	p[0] = Ksnap;		p += 1;
	PBIT64(p, t->gen);	p += 8;
	m.op = Oinsert;
	m.k = kbuf;
	m.nk = p - kbuf;
	p = packtree(vbuf, sizeof(vbuf), t);
	m.v = vbuf;
	m.nv = p - vbuf;
	if((e = btupsert(&fs->snap, &m, 1)) != nil){
		fprint(2, "error snapshotting: %s\n", e);
		return -1;
	}
	return 0;
}


Tree*
newsnap(Tree *t)
{
	vlong gen;
	Tree *r;
	int i;

	if(savesnap(t) == -1)
		return nil;

	if((r = malloc(sizeof(Tree))) == nil)
		return nil;
	gen = inc64(&fs->nextgen, 1);
	memset(&r->lk, 0, sizeof(r->lk));
	r->snext = fs->osnap;
	r->memref = 1;
	r->ref = 0;
	r->ht = t->ht;
	r->bp = t->bp;
	r->gen = gen;
	/* shift deadlist down */
	for(i = Ndead-1; i >= 0; i--){
		r->prev[i] = i == 0 ? t->gen : t->prev[i-1];
		r->dead[i].head.addr = -1;
		r->dead[i].head.hash = -1;
		r->dead[i].head.gen = -1;
		r->dead[i].tail = nil;
	}
	fs->osnap = r;

	return r;
}
