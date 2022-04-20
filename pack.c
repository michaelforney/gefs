#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"

char*
unpack8(int *err, char *p, char *e, void *v)
{
	if (e - p < 1 || *err){
		*err = 1;
		return p;
	}
	*(uchar*)v = p[0];
	return p+1;
}

char*
unpack16(int *err, char *p, char *e, void *v)
{
	if (e - p < 2 || *err){
		*err = 1;
		return p;
	}
	*(ushort*)v = GBIT16(p);
	return p+2;
}

char*
unpack32(int *err, char *p, char *e, void *v)
{
	if (e - p < 4 || *err){
		*err = 1;
		return p;
	}
	*(uint*)v = GBIT32(p);
	return p+4;
}

char*
unpack64(int *err, char *p, char *e, void *v)
{
	if (e - p < 8 || *err){
		*err = 1;
		return p;
	}
	*(uvlong*)v = GBIT64(p);
	return p+8;
}

/* Terminated so we can use them directly in C */
char*
unpackstr(int *err, char *p, char *e, char **s)
{
	int n;

	if (e - p < 3 || *err){
		*err = 1;
		return p;
	}
	n = GBIT16(p);
	if(e - p < n + 3 || p[n+2] != 0){
		*err = 1;
		return p;
	}
	*s = p+2;
	return p+3+n;
}

char*
pack8(int *err, char *p, char *e, uchar v)
{
	if (e - p < 1 || *err){
		*err = 1;
		return p;
	}
	p[0] = v;
	return p+1;
}

char*
pack16(int *err, char *p, char *e, ushort v)
{
	if (e - p < 2 || *err){
		*err = 1;
		return p;
	}
	PBIT16(p, v);
	return p+2;
}

char*
pack32(int *err, char *p, char *e, uint v)
{
	if (e - p < 4 || *err){
		*err = 1;
		return p;
	}
	PBIT32(p, v);
	return p+4;
}

char*
pack64(int *err, char *p, char *e, uvlong v)
{
	if (e - p < 8 || *err){
		*err = 1;
		return p;
	}
	PBIT64(p, v);
	return p+8;
}

/* Terminated so we can use them directly in C */
char*
packstr(int *err, char *p, char *e, char *s)
{
	int n;

	n = strlen(s);
	if (e - p < n+3 || *err){
		*err = 1;
		return p;
	}
	PBIT16(p+0, n);
	memcpy(p+2, s, n);
	p[2+n] = 0;
	return p+3+n;
}
		
int
dir2kv(vlong up, Xdir *d, Kvp *kv, char *buf, int nbuf)
{
	char *ek, *ev, *eb;

	if((ek = packdkey(buf, nbuf, up, d->name)) == nil)
		return -1;
	kv->k = buf;
	kv->nk = ek - buf;
	eb = buf + nbuf;
	if((ev = packdval(ek, eb - ek, d)) == nil)
		return -1;
	kv->v = ek;
	kv->nv = ev - ek;
	return 0;
}

char*
packdkey(char *p, int sz, vlong up, char *name)
{
	char *ep;
	int err;

	err = 0;
	ep = p + sz;
	p = pack8(&err, p, ep, Kent);
	p = pack64(&err, p, ep, up);
	if(name != nil)
		p = packstr(&err, p, ep, name);
	if(err)
		return nil;
	return p;
}

char*
unpackdkey(char *p, int sz, vlong *up)
{
	char t, *ep, *name;
	int err;

	err = 0;
	ep = p + sz;
	p = unpack8(&err, p, ep, &t);
	p = unpack64(&err, p, ep, up);
	p = unpackstr(&err, p, ep, &name);
	if(err || t != Kent || p != ep)
		return nil;
	return name;
}

char*
packsuper(char *p, int sz, vlong up)
{
	char *ep;
	int err;

	err = 0;
	ep = p + sz;
	p = pack8(&err, p, ep, Ksuper);
	p = pack64(&err, p, ep, up);
	if(err)
		return nil;
	return p;
}

char*
packdval(char *p, int sz, Xdir *d)
{
	char *e;
	int err;

	err = 0;
	e = p + sz;
	p = pack64(&err, p, e, d->qid.path);
	p = pack32(&err, p, e, d->qid.vers);
	p = pack8(&err, p, e, d->qid.type);
	p = pack32(&err, p, e, d->mode);
	p = pack64(&err, p, e, d->atime);
	p = pack64(&err, p, e, d->mtime);
	p = pack64(&err, p, e, d->length);
	p = pack32(&err, p, e, d->uid);
	p = pack32(&err, p, e, d->gid);
	p = pack32(&err, p, e, d->muid);
	if(err)
		abort();
	return p;
}

int
kv2dir(Kvp *kv, Xdir *d)
{
	char *k, *ek, *v, *ev;
	int err;

	memset(d, 0, sizeof(Xdir));
	err = 0;
	k = kv->k + 9;
	ek = kv->k + kv->nk;
	k = unpackstr(&err, k, ek, &d->name);
	if(err){
		werrstr("key too small [%d]", kv->nk);
		return -1;
	}

	v = kv->v;
	ev = v + kv->nv;
	v = unpack64(&err, v, ev, &d->qid.path);
	v = unpack32(&err, v, ev, &d->qid.vers);
	v = unpack8(&err, v, ev, &d->qid.type);
	v = unpack32(&err, v, ev, &d->mode);
	v = unpack64(&err, v, ev, &d->atime);
	v = unpack64(&err, v, ev, &d->mtime);
	v = unpack64(&err, v, ev, &d->length);
	v = unpack32(&err, v, ev, &d->uid);
	v = unpack32(&err, v, ev, &d->gid);
	v = unpack32(&err, v, ev, &d->muid);
	if(err){
		werrstr("val too small [%s]", d->name);
		return -1;
	}
	if(k != ek){
		werrstr("invalid path");
		return -1;
	}
	if(v != ev){
		werrstr("stat full of fuck");
		return -1;
	}
	return 0;
}

int
dir2statbuf(Xdir *d, char *buf, int nbuf)
{
	int sz, nn, nu, ng, nm, ret;
	vlong atime, mtime;
	User *u, *g, *m;
	char *p;

	ret = -1;
	rlock(&fs->userlk);
	if((u = uid2user(d->uid)) == nil)
		goto Out;
	if((g = uid2user(d->gid)) == nil)
		goto Out;
	if((m = uid2user(d->muid)) == nil)
		goto Out;

	p = buf;
	nn = strlen(d->name);
	nu = strlen(u->name);
	ng = strlen(g->name);
	nm = strlen(m->name);
	atime = (d->atime+Nsec/2)/Nsec;
	mtime = (d->mtime+Nsec/2)/Nsec;
	sz = STATFIXLEN + nn + nu + ng + nm;
	if(sz > nbuf)
		goto Out;
	
	PBIT16(p, sz-2);		p += 2;
	PBIT16(p, -1 /*type*/);		p += 2;
	PBIT32(p, -1 /*dev*/);		p += 4;
	PBIT8(p, d->qid.type);		p += 1;
	PBIT32(p, d->qid.vers);		p += 4;
	PBIT64(p, d->qid.path);		p += 8;
	PBIT32(p, d->mode);		p += 4;
	PBIT32(p, atime);		p += 4;
	PBIT32(p, mtime);		p += 4;
	PBIT64(p, d->length);		p += 8;

	PBIT16(p, nn);			p += 2;
	memcpy(p, d->name, nn);		p += nn;
	PBIT16(p, nu);			p += 2;
	memcpy(p, u->name, nu);		p += nu;
	PBIT16(p, ng);			p += 2;
	memcpy(p, g->name, ng);		p += ng;
	PBIT16(p, nm);			p += 2;
	memcpy(p, m->name, nm);		p += nm;
	assert(p - buf == sz);
	ret = sz;
Out:
	runlock(&fs->userlk);
	return ret;	
}

int
kv2statbuf(Kvp *kv, char *buf, int nbuf)
{
	Xdir d;

	if(kv2dir(kv, &d) == -1)
		return -1;
	return dir2statbuf(&d, buf, nbuf);
}

int
kv2qid(Kvp *kv, Qid *q)
{
	char *v, *ev;
	int err;

	err = 0;
	v = kv->v;
	ev = v + kv->nv;
	v = unpack64(&err, v, ev, &q->path);
	v = unpack32(&err, v, ev, &q->vers);
	unpack8(&err, v, ev, &q->type);
	if(err){
		werrstr("kv too small");
		return -1;
	}
	return 0;
}

char*
packlabel(char *p, int sz, char *name)
{
	int n;

	n = strlen(name);
	assert(sz >= n+1);
	p[0] = Klabel;		p += 1;
	memcpy(p, name, n);	p += n;
	return p;
}

char*
packsnap(char *p, int sz, vlong id)
{
	assert(sz >= Snapsz);
	p[0] = Ksnap;		p += 1;
	PBIT64(p, id);		p += 8;
	return p;
}

char*
packbp(char *p, int sz, Bptr *bp)
{
	assert(sz >= Ptrsz);
	PBIT64(p, bp->addr);	p += 8;
	PBIT64(p, bp->hash);	p += 8;
	PBIT64(p, bp->gen);	p += 8;
	return p;
}

Bptr
unpackbp(char *p, int sz)
{
	Bptr bp;

	assert(sz >= Ptrsz);
	bp.addr = GBIT64(p);	p += 8;
	bp.hash = GBIT64(p);	p += 8;
	bp.gen = GBIT64(p);
	return bp;
}

Tree*
unpacktree(Tree *t, char *p, int sz)
{
	Dlist *dl;
	Bptr *bp;
	int i;

	assert(sz >= Treesz);
	memset(t, 0, sizeof(Tree));
	t->ref = GBIT32(p);		p += 4;
	t->ht = GBIT32(p);		p += 4;
	t->gen = GBIT64(p);		p += 8;
	t->bp.addr = GBIT64(p);		p += 8;
	t->bp.hash = GBIT64(p);		p += 8;
	t->bp.gen = GBIT64(p);		p += 8;
	for(i = 0; i < Ndead; i++){
		dl = &t->dead[i];
		bp = &dl->head;
		dl->prev = GBIT64(p);	p += 8;
		bp->addr = GBIT64(p);	p += 8;
		bp->hash = GBIT64(p);	p += 8;
		bp->gen = -1;
		t->dead[i].ins	= nil;	/* loaded on demand */
	}
	return t;
}

char*
packtree(char *p, int sz, Tree *t)
{
	Dlist *dl;
	Bptr bp;
	int i;

	assert(sz >= Treesz);
	PBIT32(p, t->ref);	p += 4;
	PBIT32(p, t->ht);	p += 4;
	PBIT64(p, t->gen);	p += 8;
	PBIT64(p, t->bp.addr);	p += 8;
	PBIT64(p, t->bp.hash);	p += 8;
	PBIT64(p, t->bp.gen);	p += 8;
	for(i = 0; i < Ndead; i++){
		dl = &t->dead[i];
		bp = dl->head;
		if(dl->ins != nil){
			assert(checkflag(dl->ins, Bfinal));
			bp = dl->ins->bp;
		}
		PBIT64(p, dl->prev);	p += 8;
		PBIT64(p, bp.addr);	p += 8;
		PBIT64(p, bp.hash);	p += 8;
	}
	return p;
}

char*
packarena(char *p, int sz, Arena *a, Fshdr *fi)
{
	assert(sz == Blksz);
	memcpy(p, "gefs0001", 8);	p += 8;
	PBIT32(p, Blksz);		p += 4;
	PBIT32(p, Bufspc);		p += 4;
	PBIT32(p, fi->snap.ht);		p += 4;
	PBIT64(p, fi->snap.bp.addr);	p += 8;
	PBIT64(p, fi->snap.bp.hash);	p += 8;
	PBIT32(p, fi->narena);		p += 4;
	PBIT64(p, fi->arenasz);		p += 8;
	PBIT64(p, fi->nextqid);		p += 8;
	PBIT64(p, fi->nextgen);		p += 8;
	PBIT64(p, a->head.addr);	p += 8;	/* freelist addr */
	PBIT64(p, a->head.hash);	p += 8;	/* freelist hash */
	PBIT64(p, a->size);		p += 8;	/* arena size */
	PBIT64(p, a->used);		p += 8;	/* arena used */
	return p;
}

char*
unpackarena(Arena *a, Fshdr *fi, char *p, int sz)
{
	assert(sz == Blksz);
	memset(a, 0, sizeof(*a));
	memset(fi, 0, sizeof(*fi));
	if(memcmp(p, "gefs0001", 8) != 0){
		werrstr("wrong block header %.8s\n", p);
		return nil;
	}
	p += 8;
	fi->blksz = GBIT32(p);		p += 4;
	fi->bufspc = GBIT32(p);		p += 4;
	fi->snap.ht = GBIT32(p);	p += 4;
	fi->snap.bp.addr = GBIT64(p);	p += 8;
	fi->snap.bp.hash = GBIT64(p);	p += 8;
	fi->snap.bp.gen = -1;		p += 0;
	fi->narena = GBIT32(p);		p += 4;
	fi->arenasz = GBIT64(p);	p += 8;
	fi->nextqid = GBIT64(p);	p += 8;
	fi->nextgen = GBIT64(p);	p += 8;
	a->head.addr = GBIT64(p);	p += 8;
	a->head.hash = GBIT64(p);	p += 8;
	a->head.gen = -1;		p += 0;
	a->size = GBIT64(p);		p += 8;
	a->used = GBIT64(p);		p += 8;
	a->tail = nil;
	return p;
}
