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
	p[0] = Klabel;			p += 1;
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
	if(unpacktree(t, kv.v, kv.nv) == nil)
		return Efs;
	return nil;
}

static char*
modifysnap(vlong gen, char *name, int del)
{
	char dbuf[Keymax], sbuf[Snapsz];
	char *p, *e;
	int n, nm;
	Msg m[2];

	p = sbuf;
	nm = 0;
	p[0] = Ksnap;		p += 1;
	PBIT64(p, gen);		p += 8;
	m[nm].op = del ? Ounrefsnap : Orefsnap;
	m[nm].k = sbuf;
	m[nm].nk = p - sbuf;
	m[nm].v = nil;
	m[nm].nv = 0;
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

int
snapfreebp(Bptr)
{
	return 0;
}

int
movedead(Bptr)
{
	return 0;
}

char*
deletesnap(Tree *s)
{
	Tree p;
	char *e;
	int i;

	scandead(s->dead[0].head, snapfreebp);
//	for(i = 1; i < Ndead-1; i++){
//		if((e = opensnap(&p, s->prev[i])) != nil)
//			return e;
//		graftdead(s, s->prev[i], &s->dead[i]);
//	}
	scandead(s->dead[Ndead-1].head, movedead);
	return nil;
}

char*
labelsnap(vlong gen, char *name)
{
	return modifysnap(gen, name, 0);
}

char*
unlabelsnap(vlong gen, char *name)
{
	return modifysnap(gen, name, 1);
}

char*
refsnap(vlong gen)
{
	return modifysnap(gen, nil, 0);
}

char*
unrefsnap(vlong gen)
{
	return modifysnap(gen, nil, 1);
}

char*
snapshot(Tree *t, vlong *genp, vlong *oldp)
{
	char kbuf[Snapsz], vbuf[Treesz];
	char *p, *e;
	uvlong gen;
	Msg m;
	int i;

	gen = inc64(&fs->nextgen, 1);
	p = kbuf;
	p[0] = Ksnap;	p += 1;
	PBIT64(p, gen);	p += 8;
	m.op = Oinsert;
	m.k = kbuf;
	m.nk = p - kbuf;

	for(i = 0; i < Ndead; i++){
		if(t->dead[i].tail != nil){
			finalize(t->dead[i].tail);
			syncblk(t->dead[i].tail);
			putblk(t->dead[i].tail);
		}
	}

	p = packtree(vbuf, sizeof(vbuf), t);
	m.v = vbuf;
	m.nv = p - vbuf;
	if((e = btupsert(&fs->snap, &m, 1)) != nil)
		return e;
	if(sync() == -1)
		return Eio;
	/* shift deadlist down */
	if(t->dead[Ndead-1].tail != nil)
		putblk(t->dead[Ndead-1].tail);
	for(i = Ndead-1; i >= 0; i--){
		t->prev[i] = i == 0 ? gen : t->prev[i-1];
		t->dead[i].head.addr = -1;
		t->dead[i].head.hash = -1;
		t->dead[i].head.gen = -1;
		t->dead[i].tail = nil;
	}
	*genp = gen;
	*oldp = t->prev[0];
	return nil;
}
