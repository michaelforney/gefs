#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>
#include <bio.h>

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
dir2kv(vlong up, Dir *d, Kvp *kv, char *buf, int nbuf)
{
	char *k, *ek, *v, *ev, *eb;
	int err;

	err = 0;
	k = buf;
	ek = buf;
	eb = buf + nbuf;
	ek = pack8(&err, ek, eb, Kent);
	ek = pack64(&err, ek, eb, up);
	ek = packstr(&err, ek, eb, d->name);

	v = ek;
	ev = ek;
	ev = pack64(&err, ev, eb, d->qid.path);
	ev = pack32(&err, ev, eb, d->qid.vers);
	ev = pack8(&err, ev, eb, d->qid.type);
	ev = pack32(&err, ev, eb, d->mode);
	ev = pack64(&err, ev, eb, (vlong)d->atime*Nsec);
	ev = pack64(&err, ev, eb, (vlong)d->mtime*Nsec);
	ev = pack64(&err, ev, eb, d->length);
	ev = packstr(&err, ev, eb, d->uid);
	ev = packstr(&err, ev, eb, d->gid);
	ev = packstr(&err, ev, eb, d->muid);
	if(err){
		werrstr("stat too big: %.*s...", 32, d->name);
		return -1;
	}
	kv->k = k;
	kv->nk = ek - k;
	kv->v = v;
	kv->nv = ev - v;
	return 0;
}

int
name2dkey(vlong up, char *name, Key *k, char *buf, int nbuf)
{
	char *ek, *eb;
	int err;

	err = 0;
	ek = buf;
	eb = buf + nbuf;
	ek = pack8(&err, ek, eb, Kent);
	ek = pack64(&err, ek, eb, up);
	ek = packstr(&err, ek, eb, name);
	if(err)
		return -1;
	k->k = buf;
	k->nk = ek - buf;
	return k->nk;
}

int
kv2dir(Kvp *kv, Dir *d)
{
	char *k, *ek, *v, *ev;
	vlong atime, mtime;
	int err;

	memset(d, 0, sizeof(Dir));
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
	v = unpack64(&err, v, ev, &atime);
	v = unpack64(&err, v, ev, &mtime);
	v = unpack64(&err, v, ev, &d->length);
	v = unpackstr(&err, v, ev, &d->uid);
	v = unpackstr(&err, v, ev, &d->gid);
	v = unpackstr(&err, v, ev, &d->muid);
	if(err){
//		print("fucked: %P\n", kv);
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
	d->atime = (atime+Nsec/2)/Nsec;
	d->mtime = (mtime+Nsec/2)/Nsec;
	return 0;
}

int
kv2statbuf(Kvp *kv, char *buf, int nbuf)
{
	Dir d;
	int n;

	if(kv2dir(kv, &d) == -1)
		return -1;
	if((n = convD2M(&d, (uchar*)buf, nbuf)) <= BIT16SZ)
		return -1;
	return n;	
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

char*
packdkey(char *p, int sz, vlong up, char *name)
{
	char *ep;
	int err;

	err = 0;
	ep = p + sz;
	p = pack8(&err, p, ep, Kent);
	p = pack64(&err, p, ep, up);
	p = packstr(&err, p, ep, name);
	if(err)
		return 0;
	return p;
}

Tree*
unpacktree(Tree *t, char *p, int sz)
{
	int i, j;
	Bptr bp, head;
	Blk *b;

	assert(sz >= Treesz);
	memset(t, 0, sizeof(Tree));
	t->ref = GBIT32(p);		p += 4;
	t->ht = GBIT32(p);		p += 4;
	t->bp.addr = GBIT64(p);		p += 8;
	t->bp.hash = GBIT64(p);		p += 8;
	t->bp.gen = GBIT64(p);		p += 8;
	for(i = 0; i < Ndead; i++){
		t->prev[i] = GBIT64(p);	p += 8;
		head.addr = GBIT64(p);	p += 8;
		head.hash = GBIT64(p);	p += 8;
		head.gen = -1;
		bp.addr = GBIT64(p);	p += 8;
		bp.hash = GBIT64(p);	p += 8;
		bp.gen = -1;
		if(bp.addr == -1)
			continue;
		if((b = getblk(bp, 0)) == nil){
			for(j = 0; j < i; j++)
				putblk(t->dead[j].tail);
			return nil;
		}
		t->dead[i].head = head;
		t->dead[i].tail	= b;
		cacheblk(b);		
	}
	return t;
}

char*
packtree(char *p, int sz, Tree *t)
{
	vlong tladdr, tlhash;
	Bptr head;
	Blk *tl;
	int i;

	assert(sz >= Treesz);
	PBIT32(p, t->ref);	p += 4;
	PBIT32(p, t->ht);	p += 4;
	PBIT64(p, t->bp.addr);	p += 8;
	PBIT64(p, t->bp.hash);	p += 8;
	PBIT64(p, t->bp.gen);	p += 8;
	for(i = 0; i < Ndead; i++){
		tladdr = -1;
		tlhash = -1;
		if(t->dead[i].tail != nil){
			tl = t->dead[i].tail;
			lock(tl);
			assert(tl->flag & Bfinal);
			unlock(tl);
			tladdr = tl->bp.addr;
			tlhash = tl->bp.hash;
		}
		head = t->dead[i].head;
		PBIT64(p, t->prev[i]);	p += 8;
		PBIT64(p, head.addr);	p += 8;
		PBIT64(p, head.hash);	p += 8;
		PBIT64(p, tladdr);	p += 8;
		PBIT64(p, tlhash);	p += 8;
	}
	return p;
}
