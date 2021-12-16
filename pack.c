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
packbp(char *p, Bptr *bp)
{
	PBIT64(p + 0, bp->addr);
	PBIT64(p + 8, bp->hash);
	PBIT64(p + 16, bp->gen);
	return p + 24;
}

Bptr
unpackbp(char *p)
{
	Bptr bp;

	bp.addr = GBIT64(p + 0);
	bp.hash = GBIT64(p + 8);
	bp.gen = GBIT64(p + 16);
	return bp;
}
