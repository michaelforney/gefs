#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>
#include <bio.h>

#include "dat.h"
#include "fns.h"

static char*	clearb(Fid*, vlong, vlong);

// FIXME: hack. We sync way too often.
static char*
updatesnap(Fid *f)
{
	Tree *t, *n;
	char *e;

	t = f->mnt->root;
	qlock(&fs->snaplk);
	if((n = newsnap(t)) == nil){
		fprint(2, "snap: save %s: %s\n", f->mnt->name, "create snap");
		abort();
	}
	if((e = labelsnap(f->mnt->name, t->gen)) != nil){
		fprint(2, "snap: save %s: %s\n", f->mnt->name, e);
		abort();
	}
	if(t->prev[0] != -1){
		if((e = unrefsnap(t->prev[0], t->gen)) != nil){
			fprint(2, "snap: unref old: %s\n", e);
			abort();
		}
	}
	f->mnt->root = n;
	closesnap(t);
	qunlock(&fs->snaplk);
	sync();
	return nil;
}

static int
okname(char *name)
{
	int i;

	if(name[0] == 0)
		return -1;
	if(strcmp(name, ".") == 0 || strcmp(name, "..") == 0)
		return -1;
	for(i = 0; i < Maxname; i++){
		if(name[i] == 0)
			return 0;
		if((name[i]&0xff) < 0x20 || name[i] == '/')
			return -1;
	}
	return -1;
}

Chan*
mkchan(int size)
{
	Chan *c;

	if((c = mallocz(sizeof(Chan) + size*sizeof(void*), 1)) == nil)
		sysfatal("create channel");
	c->size = size;
	c->avail = size;
	c->count = 0;
	c->rp = c->args;
	c->wp = c->args;
	return c;

}

Fmsg*
chrecv(Chan *c)
{
	void *a;
	long v;

	v = c->count;
	if(v == 0 || cas(&c->count, v, v-1) == 0)
		semacquire(&c->count, 1);
	lock(&c->rl);
	a = *c->rp;
	if(++c->rp >= &c->args[c->size])
		c->rp = c->args;
	unlock(&c->rl);
	semrelease(&c->avail, 1);
	return a;
}

void
chsend(Chan *c, Fmsg *m)
{
	long v;

	v = c->avail;
	if(v == 0 || cas(&c->avail, v, v-1) == 0)
		semacquire(&c->avail, 1);
	lock(&c->wl);
	*c->wp = m;
	if(++c->wp >= &c->args[c->size])
		c->wp = c->args;
	unlock(&c->wl);
	semrelease(&c->count, 1);

}

void
fshangup(int fd, char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	vfprint(2, fmt, ap);
	va_end(ap);
	close(fd);
	abort();
}

static void
respond(Fmsg *m, Fcall *r)
{
	uchar buf[Max9p];
	int w, n;

	r->tag = m->tag;
	dprint("→ %F\n", r);
	if((n = convS2M(r, buf, sizeof(buf))) == 0)
		abort();
	qlock(m->wrlk);
	w = write(m->fd, buf, n);
	qunlock(m->wrlk);
	if(w != n)
		fshangup(m->fd, "failed write");
	free(m);
}

static void
rerror(Fmsg *m, char *fmt, ...)
{
	char buf[128];
	va_list ap;
	Fcall r;

	va_start(ap, fmt);
	vsnprint(buf, sizeof(buf), fmt, ap);
	va_end(ap);
	r.type = Rerror;
	r.ename = buf;
	respond(m, &r);
}


static char*
lookup(Fid *f, Key *k, Kvp *kv, char *buf, int nbuf, int lk)
{
	char *e;

	if(f->mnt == nil)
		return Eattach;
	if(lk)
		rlock(f->dent);
	e = btlookup(f->mnt->root, k, kv, buf, nbuf);
	if(lk)
		runlock(f->dent);
	return e;
}

/*
 * Clears all blocks in that intersect with
 * the range listed.
 */
static char*
clearb(Fid *f, vlong o, vlong sz)
{
	char *e, buf[Offksz];
	Msg m;

	o &= ~(Blksz - 1);
	for(; o < sz; o += Blksz){
		m.k = buf;
		m.nk = sizeof(buf);
		m.op = Oclearb;
		m.k[0] = Kdat;
		PBIT64(m.k+1, f->qpath);
		PBIT64(m.k+9, o);
		m.v = nil;
		m.nv = 0;
		if((e = btupsert(f->mnt->root, &m, 1)) != nil)
			return e;
	}
	return nil;
}

static int
readb(Fid *f, char *d, vlong o, vlong n, int sz)
{
	char *e, buf[17], kvbuf[17+32];
	vlong fb, fo;
	Bptr bp;
	Blk *b;
	Key k;
	Kvp kv;

	if(o >= sz)
		return 0;

	fb = o & ~(Blksz-1);
	fo = o & (Blksz-1);

	k.k = buf;
	k.nk = sizeof(buf);
	k.k[0] = Kdat;
	PBIT64(k.k+1, f->qpath);
	PBIT64(k.k+9, fb);

	e = lookup(f, &k, &kv, kvbuf, sizeof(kvbuf), 0);
	if(e != nil && e != Eexist){
		werrstr(e);
		return -1;
	}

	bp = unpackbp(kv.v, kv.nv);
	if((b = getblk(bp, GBraw)) == nil)
		return -1;
	if(fo+n > Blksz)
		n = Blksz-fo;
	if(b != nil){
		memcpy(d, b->buf+fo, n);
		putblk(b);
	}else
		memset(d, 0, n);
	return n;
}

static int
writeb(Fid *f, Msg *m, Bptr *ret, char *s, vlong o, vlong n, vlong sz)
{
	char buf[Kvmax];
	vlong fb, fo;
	Blk *b, *t;
	Bptr bp;
	Kvp kv;

	fb = o & ~(Blksz-1);
	fo = o & (Blksz-1);

	m->k[0] = Kdat;
	PBIT64(m->k+1, f->qpath);
	PBIT64(m->k+9, fb);


	b = newblk(Traw);
	if(b == nil)
		return -1;
	if(fb < sz && (fo != 0 || n != Blksz)){
		if(lookup(f, m, &kv, buf, sizeof(buf), 0) != nil)
			return -1;
		bp = unpackbp(kv.v, kv.nv);
		if((t = getblk(bp, GBraw)) == nil)
			return -1;
		memcpy(b->buf, t->buf, Blksz);
		freeblk(f->mnt->root, t);
		putblk(t);
	}
	if(fo+n > Blksz)
		n = Blksz-fo;
	memcpy(b->buf+fo, s, n);
	enqueue(b);

	packbp(m->v, m->nv, &b->bp);
	*ret = b->bp;
	putblk(b);
	return n;
}

static Dent*
getdent(vlong pqid, Xdir *d)
{
	Dent *de;
	char *e;
	u32int h;

	h = ihash(d->qid.path) % Ndtab;
	lock(&fs->dtablk);
	for(de = fs->dtab[h]; de != nil; de = de->next){
		if(de->qid.path == d->qid.path){
			ainc(&de->ref);
			unlock(&fs->dtablk);
			return de;
		}
	}

	if((de = mallocz(sizeof(Dent), 1)) == nil)
		return nil;
	de->Xdir = *d;
	de->ref = 1;
	de->qid = d->qid;
	de->length = d->length;
	de->k = de->buf;
	de->nk = 9 + strlen(d->name) + 1;

	if((e = packdkey(de->buf, sizeof(de->buf), pqid, d->name)) == nil)
		return nil;
	de->nk = e - de->buf;
	de->next = fs->dtab[h];
	fs->dtab[h] = de;

	unlock(&fs->dtablk);
	return de;
}

static void
clunkmount(Mount *mnt)
{
	if(mnt != nil && adec(&mnt->ref) == 0)
		free(mnt);
}

static void
clunkdent(Dent *de)
{
	Dent *e, **pe;
	u32int h;

	if(adec(&de->ref) == 0){
		h = ihash(de->qid.path) % Ndtab;
		lock(&fs->dtablk);
		pe = &fs->dtab[h];
		for(e = fs->dtab[h]; e != nil; e = e->next){
			if(e == de){
				*pe = e->next;
				unlock(&fs->dtablk);
				free(de);
				return;
			}
			pe = &e->next;
		}
		abort();
	}
}

void
showfid(int fd, char**, int)
{
	int i;
	Fid *f;

	lock(&fs->fidtablk);
	fprint(fd, "fids:---\n");
	for(i = 0; i < Nfidtab; i++)
		for(f = fs->fidtab[i]; f != nil; f = f->next){
			rlock(f->dent);
			fprint(fd, "\tfid[%d]: %d [refs=%ld, k=%K, qid=%Q]\n",
				i, f->fid, f->dent->ref, &f->dent->Key, f->dent->qid);
			runlock(f->dent);
		}
	unlock(&fs->fidtablk);
}

static Fid*
getfid(u32int fid)
{
	u32int h;
	Fid *f;

	h = ihash(fid) % Nfidtab;
	lock(&fs->fidtablk);
	for(f = fs->fidtab[h]; f != nil; f = f->next)
		if(f->fid == fid)
			break;
	unlock(&fs->fidtablk);
	ainc(&f->ref);
	return f;
}

static void
putfid(Fid *f)
{
	if(adec(&f->ref) != 0)
		return;
	clunkmount(f->mnt);
	clunkdent(f->dent);
	free(f);
}

static Fid*
dupfid(int new, Fid *f)
{
	Fid *n, *o;
	u32int h;

	h = ihash(new) % Nfidtab;
	if((n = malloc(sizeof(Fid))) == nil)
		return nil;

	*n = *f;
	n->fid = new;
	n->ref = 2; /* one for dup, one for clunk */
	n->mode = -1;
	n->next = nil;
	if(n->mnt != nil)
		ainc(&n->mnt->ref);

	lock(&fs->fidtablk);
	ainc(&n->dent->ref);
	for(o = fs->fidtab[h]; o != nil; o = o->next)
		if(o->fid == new)
			break;
	if(o == nil){
		n->next = fs->fidtab[h];
		fs->fidtab[h] = n;
	}
	unlock(&fs->fidtablk);

	if(o != nil){
		fprint(2, "fid in use: %d == %d", o->fid, new);
		abort();
		free(n);
		return nil;
	}
	return n;
}

static void
clunkfid(Fid *fid)
{
	Fid *f, **pf;
	u32int h;

	lock(&fs->fidtablk);
	h = ihash(fid->fid) % Nfidtab;
	pf = &fs->fidtab[h];
	for(f = fs->fidtab[h]; f != nil; f = f->next){
		if(f == fid){
			assert(adec(&f->ref) != 0);
			*pf = f->next;
			break;
		}
		pf = &f->next;
	}
	unlock(&fs->fidtablk);
}

static Fmsg*
readmsg(int fd, int max)
{
	char szbuf[4];
	int sz;
	Fmsg *m;

	if(readn(fd, szbuf, 4) != 4)
		return nil;
	sz = GBIT32(szbuf);
	if(sz > max)
		return nil;
	if((m = malloc(sizeof(Fmsg)+sz)) == nil)
		return nil;
	if(readn(fd, m->buf+4, sz-4) != sz-4){
		werrstr("short read: %r");
		free(m);
		return nil;
	}
	m->fd = fd;
	m->sz = sz;
	PBIT32(m->buf, sz);
	return m;
}

static void
fsversion(Fmsg *m, int *msz)
{
	Fcall r;

	memset(&r, 0, sizeof(Fcall));
	if(strcmp(m->version, "9P2000") == 0){
		if(m->msize < *msz)
			*msz = m->msize;
		r.type = Rversion;
		r.msize = *msz;
	}else{
		r.type = Rversion;
		r.version = "unknown";
	}
	respond(m, &r);
}

static void
fsauth(Fmsg *m)
{
	Fcall r;

	r.type = Rerror;
	r.ename = "unimplemented auth";
	respond(m, &r);
}

int
ingroup(int uid, int gid)
{
	User *u, *g;
	int i, in;

	rlock(&fs->userlk);
	in = 0;
	u = uid2user(uid);
	g = uid2user(gid);
	if(u != nil && g != nil)
		for(i = 0; i < g->nmemb; i++)
			if(u->id == g->memb[i])
				in = 1;
	runlock(&fs->userlk);
	return in;
}

int
mode2bits(int req)
{
	int m;

	m = 0;
	switch(req&0xf){
	case OREAD:	m = DMREAD;		break;
	case OWRITE:	m = DMWRITE;		break;
	case ORDWR:	m = DMREAD|DMWRITE;	break;
	case OEXEC:	m = DMREAD|DMEXEC;	break;
	}
	if(req&OTRUNC)
		m |= DMWRITE;
	return m;
}

int
fsaccess(Mount *mnt, int fmode, int fuid, int fgid, int m)
{
	/* uid none gets only other permissions */
	if(mnt->uid != 0) {
		if(mnt->uid == fuid)
			if((m & (fmode>>6)) == m)
				return 0;
		if(ingroup(mnt->uid, fgid))
			if((m & (fmode>>3)) == m)
				return 0;
	}
	if(m & fmode) {
		if((fmode & DMDIR) && (m == DMEXEC))
			return 0;
		if(!ingroup(mnt->uid, 9999))
			return 0;
	}
	return -1;
}

static void
fsattach(Fmsg *m, int iounit)
{
	char *e, *p, dbuf[Kvmax], kvbuf[Kvmax];
	Mount *mnt;
	Dent *de;
	User *u;
	Fcall r;
	Xdir d;
	Kvp kv;
	Key dk;
	Fid f;

	if((mnt = mallocz(sizeof(Mount), 1)) == nil){
		rerror(m, Emem);
		return;
	}
	if((mnt->name = strdup(m->aname)) == nil){
		rerror(m, Emem);
		return;
	}
	rlock(&fs->userlk);
	if((u = name2user("glenda")) == nil){
		rerror(m, Enouser);
		runlock(&fs->userlk);
		return;
	}
	mnt->uid = u->id;
	runlock(&fs->userlk);

	if((mnt->root = openlabel(m->aname)) == nil){
		rerror(m, Enosnap);
		return;
	}

	if((p = packdkey(dbuf, sizeof(dbuf), -1ULL, "")) == nil){
		rerror(m, Elength);
		return;
	}
	dk.k = dbuf;
	dk.nk = p - dbuf;
	if((e = btlookup(mnt->root, &dk, &kv, kvbuf, sizeof(kvbuf))) != nil){
		rerror(m, e);
		return;
	}
	if(kv2dir(&kv, &d) == -1){
		rerror(m, Efs);
		return;
	}
	if((de = getdent(-1, &d)) == nil){
		rerror(m, Efs);
		return;
	}

	/*
	 * A bit of a hack; we're duping a fid
	 * that doesn't exist, so we'd be off
	 * by one on the refcount. Adjust to
	 * compensate for the dup.
	 */
	adec(&de->ref);

	memset(&f, 0, sizeof(Fid));
	f.fid = NOFID;
	f.mnt = mnt;
	f.qpath = d.qid.path;
	f.mode = -1;
	f.iounit = iounit;
	f.dent = de;
	f.duid = mnt->uid;
	f.dgid = d.gid;
	f.dmode = d.mode;
	if(dupfid(m->fid, &f) == nil){
		rerror(m, Enomem);
		return;
	}

	r.type = Rattach;
	r.qid = d.qid;
	respond(m, &r);
	return;
}

void
fswalk(Fmsg *m)
{
	char *p, *e, kbuf[Maxent], kvbuf[Kvmax];
	int duid, dgid, dmode;
	vlong up, prev;
	Fid *o, *f;
	Dent *dent;
	Fcall r;
	Xdir d;
	Kvp kv;
	Key k;
	int i;

	if((o = getfid(m->fid)) == nil){
		rerror(m, Efid);
		return;
	}
	if(o->mode != -1){
print("use walk\n");
		rerror(m, Einuse);
		return;
	}
	e = nil;
	up = o->qpath;
	prev = o->qpath;
	d = *o->dent;
	duid = d.uid;
	dgid = d.gid;
	dmode = d.mode;
	r.type = Rwalk;
	for(i = 0; i < m->nwname; i++){
		if((p = packdkey(kbuf, sizeof(kbuf), prev, m->wname[i])) == nil){
			rerror(m, Elength);
			putfid(o);
			return;
		}
		k.k = kbuf;
		k.nk = p - kbuf;
		if((e = lookup(o, &k, &kv, kvbuf, sizeof(kvbuf), 0)) != nil){
			break;
		}
		duid = d.uid;
		dgid = d.gid;
		dmode = d.mode;
		if(kv2dir(&kv, &d) == -1){
			rerror(m, Efs);
			putfid(o);
			return;
		}
		up = prev;
		prev = d.qid.path;
		r.wqid[i] = d.qid;
	}
	r.nwqid = i;
	if(i == 0 && m->nwname != 0){
		rerror(m, e);
		putfid(o);
		return;
	}
	f = o;
	if(m->fid != m->newfid && i == m->nwname){
		if((f = dupfid(m->newfid, o)) == nil){
			rerror(m, Emem);
			putfid(o);
			return;
		}
		putfid(o);
	}
	if(i > 0){
		dent = getdent(up, &d);
		if(dent == nil){
			if(m->fid != m->newfid)
				clunkfid(f);
			rerror(m, Enomem);
			putfid(f);
			return;
		}
		if(i == m->nwname){
			f->qpath = r.wqid[i-1].path;
			f->dent = dent;
			f->duid = duid;
			f->dgid = dgid;
			f->dmode = dmode;
		}
	}
	respond(m, &r);
	putfid(f);
}

void
fsstat(Fmsg *m)
{
	char *err, buf[STATMAX], kvbuf[Kvmax];
	Fcall r;
	Fid *f;
	Kvp kv;
	int n;

	if((f = getfid(m->fid)) == nil){
		rerror(m, Efid);
		return;
	}
	if((err = btlookup(f->mnt->root, f->dent, &kv, kvbuf, sizeof(kvbuf))) != nil){
		rerror(m, err);
		putfid(f);
		return;
	}
	if((n = kv2statbuf(&kv, buf, sizeof(buf))) == -1){
		rerror(m, "stat: %r");
		putfid(f);
		return;
	}
	r.type = Rstat;
	r.stat = (uchar*)buf;
	r.nstat = n;
	respond(m, &r);
	putfid(f);
}

void
fswstat(Fmsg *m)
{
	char *p, *e, strs[65535], rnbuf[Kvmax], opbuf[Kvmax], kvbuf[Kvmax];
	int op, nm, sync;
	vlong up;
	Fcall r;
	Dent *de;
	Msg mb[3];
	Xdir o;
	Dir d;
	Fid *f;
	Kvp kv;
	Key k;

	nm = 0;
	sync = 1;
	if((f = getfid(m->fid)) == nil){
		rerror(m, Efid);
		return;
	}
	if(f->dent->qid.type != QTDIR && f->dent->qid.type != QTFILE){
		rerror(m, Efid);
		goto Out;
	}
	if(convM2D(m->stat, m->nstat, &d, strs) <= BIT16SZ){
		rerror(m, Edir);
		goto Out;
	}
	de = f->dent;
	k = f->dent->Key;
	rlock(de);
	if(fsaccess(f->mnt, de->mode, de->uid, de->gid, DMWRITE) == -1){
		rerror(m, Eperm);
		runlock(de);
		return;
	}

	/* A nop qid change is allowed. */
	if(d.qid.path != ~0 || d.qid.vers != ~0){
		if(d.qid.path != de->qid.path){
			rerror(m, Ewstatp);
			goto Out;
		}
		if(d.qid.vers != de->qid.vers){
			rerror(m, Ewstatv);
			goto Out;
		}
		sync = 0;
	}

	/*
	 * rename: verify name is valid, same name renames are nops.
	 * this is inserted into the tree as a pair of delete/create
	 * messages.
	 */
	if(d.name != nil && *d.name != '\0'){
		if(okname(d.name) == -1){
			rerror(m, Ename);
			goto Out;
		}
		/* renaming to the same name is a nop. */
		mb[nm].op = Odelete;
		mb[nm].Key = f->dent->Key;
		nm++;
		if((e = btlookup(f->mnt->root, de, &kv, kvbuf, sizeof(kvbuf))) != nil){
			rerror(m, e);
			goto Out;
		}
		if(kv2dir(&kv, &o) == -1){
			rerror(m, "stat: %r");
			goto Out;
		}
		o.name = d.name;
		mb[nm].op = Oinsert;
		up = GBIT64(f->dent->k+1);
		if(dir2kv(up, &o, &mb[nm], rnbuf, sizeof(rnbuf)) == -1){
			rerror(m, Efs);
			goto Out;
		}
		sync = 0;
		k = mb[nm].Key;
		nm++;
	}
	runlock(de);

	wlock(de);
	p = opbuf+1;
	op = 0;
	mb[nm].Key = k;
	mb[nm].op = Owstat;
	de->qid.vers++;
	if(d.length != ~0){
		op |= Owsize;
		de->length = d.length;
		PBIT64(p, d.length);
		p += 8;
		sync = 0;
	}
	if(d.mode != ~0){
		op |= Owmode;
		de->mode = d.mode;
		PBIT32(p, d.mode);
		p += 4;
		sync = 0;
	}
	if(d.mtime != ~0){
		op |= Owmtime;
		de->mtime = d.mtime;
		PBIT64(p, (vlong)d.mtime*Nsec);
		p += 8;
		sync = 0;
	}
	op |= Owmuid;
	de->muid = f->mnt->uid;
	PBIT32(p, f->mnt->uid);
	p += 4;
	wunlock(de);

	opbuf[0] = op;
	mb[nm].v = opbuf;
	mb[nm].nv = p - opbuf;
	nm++;

	if(sync){
		rerror(m, Eimpl);
	}else{
		if((e = btupsert(f->mnt->root, mb, nm)) != nil){
			rerror(m, e);
			goto Out;
		}
		if((e = updatesnap(f)) != nil){
			rerror(m, e);
			goto Out;
		}
		r.type = Rwstat;
		respond(m, &r);
	}

Out:
	putfid(f);
}


void
fsclunk(Fmsg *m)
{
	Fcall r;
	Fid *f;

	if((f = getfid(m->fid)) == nil){
		rerror(m, Efid);
		return;
	}

	lock(f);
	if(f->scan != nil){
		btdone(f->scan);
		f->scan = nil;
	}
	unlock(f);

	clunkfid(f);
	r.type = Rclunk;
	respond(m, &r);
	putfid(f);
}

void
fscreate(Fmsg *m)
{
	char *e, buf[Kvmax];
	Dent *de;
	Fcall r;
	Msg mb;
	Fid *f;
	Xdir d;

	if(okname(m->name) == -1){
		rerror(m, Ename);
		return;
	}
	if((f = getfid(m->fid)) == nil){
		rerror(m, Efid);
		return;
	}
	if(m->perm & (DMMOUNT|DMAUTH)){
		rerror(m, "unknown permission");
		return;
	}
	de = f->dent;
	rlock(de);
	if(fsaccess(f->mnt, de->mode, de->uid, de->gid, DMWRITE) == -1){
		rerror(m, Eperm);
		runlock(de);
		return;
	}
	runlock(de);

	d.qid.type = 0;
	if(m->perm & DMDIR)
		d.qid.type |= QTDIR;
	if(m->perm & DMAPPEND)
		d.qid.type |= QTAPPEND;
	if(m->perm & DMEXCL)
		d.qid.type |= QTEXCL;
	if(m->perm & DMTMP)
		d.qid.type |= QTTMP;
	d.qid.path = inc64(&fs->nextqid, 1);
	d.qid.vers = 0;
	d.mode = m->perm;
	d.name = m->name;
	d.atime = nsec();
	d.mtime = d.atime;
	d.length = 0;
	d.uid = f->mnt->uid;
	d.gid = f->dgid;
	d.muid = f->mnt->uid;

	mb.op = Oinsert;
	if(dir2kv(f->qpath, &d, &mb, buf, sizeof(buf)) == -1){
		rerror(m, Efs);
		putfid(f);
		return;
	}
	if((e = btupsert(f->mnt->root, &mb, 1)) != nil){
		rerror(m, e);
		putfid(f);
		return;
	}
	de = getdent(f->qpath, &d);
	if(de == nil){
		if(m->fid != m->newfid)
			clunkfid(f);
		rerror(m, Enomem);
		putfid(f);
		return;
	}

	lock(f);
	if(f->mode != -1){
		unlock(f);
		clunkdent(de);
print("use create\n");
		rerror(m, Einuse);
		putfid(f);
		return;
	}
	f->mode = mode2bits(m->mode);
	f->qpath = d.qid.path;
	f->dent = de;
	wlock(de);
	if((e = clearb(f, 0, de->length)) != nil){
		unlock(f);
		clunkdent(de);
		rerror(m, e);
		putfid(f);
		return;
	}
	de->length = 0;
	wunlock(de);
	unlock(f);

	r.type = Rcreate;
	r.qid = d.qid;
	r.iounit = f->iounit;
	if((e = updatesnap(f)) != nil){
		rerror(m, e);
		putfid(f);
		return;
	}
	respond(m, &r);
	putfid(f);
}

char*
candelete(Fid *f)
{
	char *e, pfx[Dpfxsz];
	int done;
	Scan *s;

	if(f->dent->qid.type == QTFILE)
		return nil;
	if((s = mallocz(sizeof(Scan), 1)) == nil)
		return Enomem;

	pfx[0] = Kent;
	PBIT64(pfx+1, f->qpath);
	if((e = btscan(f->mnt->root, s, pfx, sizeof(pfx))) != nil){
		btdone(s);
		free(s);
		return e;
	}
	done = 0;
	if((e = btnext(s, &s->kv, &done)) != nil)
		return e;
	btdone(s);
	if(done)
		return nil;
	return Enempty;
}

void
fsremove(Fmsg *m)
{
	Fcall r;
	Msg mb;
	Fid *f;
	char *e;

	if((f = getfid(m->fid)) == nil){
		rerror(m, Efid);
		return;
	}

	rlock(f->dent);
	if((e = candelete(f)) != nil){
		runlock(f->dent);
		rerror(m, e);
		clunkfid(f);
		return;
	}
	if(fsaccess(f->mnt, f->dmode, f->duid, f->dgid, OWRITE) == -1){
		rerror(m, Eperm);
		runlock(f->dent);
		return;
	}
	mb.op = Odelete;
	mb.k = f->dent->k;
	mb.nk = f->dent->nk;
	mb.nv = 0;
	if((e = btupsert(f->mnt->root, &mb, 1)) != nil){
		runlock(f->dent);
		rerror(m, e);
		clunkfid(f);
		return;
	}
	if(f->dent->qid.type == QTFILE){
		if((e = clearb(f, 0, f->dent->length)) != nil){
			runlock(f->dent);
			rerror(m, e);
			clunkfid(f);
			return;
		}
	}
	runlock(f->dent);

	if((e = updatesnap(f)) != nil){
		rerror(m, e);
		clunkfid(f);
		return;
	}
	r.type = Rremove;
	respond(m, &r);
	clunkfid(f);
}

void
fsopen(Fmsg *m)
{
	char *e, buf[Kvmax];
	Fcall r;
	Xdir d;
	Fid *f;
	Kvp kv;
	int mb;

	mb = mode2bits(m->mode);
	if((f = getfid(m->fid)) == nil){
		rerror(m, Efid);
		return;
	}
	if((e = lookup(f, f->dent, &kv, buf, sizeof(buf), 0)) != nil){
		rerror(m, e);
		putfid(f);
		return;
	}
	if(kv2dir(&kv, &d) == -1){
		rerror(m, Efs);
		putfid(f);
		return;
	}
	if(fsaccess(f->mnt, d.mode, d.uid, d.gid, mb) == -1){
		rerror(m, Eperm);
		putfid(f);
		return;
	}
	wlock(f->dent);
	f->dent->length = d.length;
	wunlock(f->dent);
	r.type = Ropen;
	r.qid = d.qid;
	r.iounit = f->iounit;

	lock(f);
	if(f->mode != -1){
		rerror(m, Einuse);
print("in use open\n");
		unlock(f);
		putfid(f);
		return;
	}
	f->mode = mode2bits(m->mode);
//	if((f->mode & DMEXEC)){
//		lock(&fs->root.lk);
//		f->root = fs->root;
//		refblk(fs->root.bp);
//		unlock(&fs->root.lk);
//	}
	if(f->mode & OTRUNC){
		wlock(f->dent);
//		freeb(f->dent, 0, dent->length);
		f->dent->length = 0;
		wunlock(f->dent);
	}
	unlock(f);
	respond(m, &r);
	putfid(f);
}

char*
fsreaddir(Fmsg *m, Fid *f, Fcall *r)
{
	char pfx[Dpfxsz], *p, *e;
	int n, ns, done;
	Scan *s;

	s = f->scan;
	if(s != nil && s->offset != 0 && s->offset != m->offset)
		return Edscan;
	if(s == nil || m->offset == 0){
		if((s = mallocz(sizeof(Scan), 1)) == nil)
			return Enomem;

		pfx[0] = Kent;
		PBIT64(pfx+1, f->qpath);
		if((e = btscan(f->mnt->root, s, pfx, sizeof(pfx))) != nil){
			free(r->data);
			btdone(s);
			return e;
		}

		lock(f);
		if(f->scan != nil)
			btdone(f->scan);
		f->scan = s;
		unlock(f);
	}
	if(s->done){
		r->count = 0;
		return nil;
	}
	p = r->data;
	n = m->count;
	if(s->overflow){
		if((ns = kv2statbuf(&s->kv, p, n)) == -1)
			return Edscan;
		s->overflow = 0;
		p += ns;
		n -= ns;
	}
	while(1){
		if((e = btnext(s, &s->kv, &done)) != nil)
			return e;
		if(done)
			break;
		if((ns = kv2statbuf(&s->kv, p, n)) == -1){
			s->overflow = 1;
			break;
		}
		p += ns;
		n -= ns;
	}
	r->count = p - r->data;
	return nil;
}

char*
fsreadfile(Fmsg *m, Fid *f, Fcall *r)
{
	vlong n, c, o;
	char *p;
	Dent *e;

	r->type = Rread;
	r->count = 0;
	e = f->dent;
	rlock(e);
	if(m->offset > e->length){
		runlock(e);
		return nil;
	}
	if((r->data = malloc(m->count)) == nil){
		runlock(e);
		return Enomem;
	}
	p = r->data;
	c = m->count;
	o = m->offset;
	if(m->offset + m->count > e->length)
		c = e->length - m->offset;
	while(c != 0){
		n = readb(f, p, o, c, e->length);
		if(n == -1){
			fprint(2, "read %K [%Q]@%lld+%lld: %r\n", &e->Key, e->qid, o, c);
			runlock(e);
			return Efs;
		}
		r->count += n;
		if(n == 0)
			break;
		p += n;
		o += n;
		c -= n;
	}
	runlock(e);
	return nil;
}

void
fsread(Fmsg *m)
{
	char *e;
	Fcall r;
	Fid *f;

	if((f = getfid(m->fid)) == nil){
		rerror(m, Efid);
		return;
	}
	r.type = Rread;
	r.count = 0;
	if((r.data = malloc(m->count)) == nil){
		rerror(m, Emem);
		putfid(f);
		return;
	}
	if(f->dent->qid.type & QTDIR)
		e = fsreaddir(m, f, &r);
	else
		e = fsreadfile(m, f, &r);
	if(e != nil){
		rerror(m, e);
		putfid(f);
		return;
	}
	respond(m, &r);
	free(r.data);
	putfid(f);
}


void
fswrite(Fmsg *m)
{
	char sbuf[Wstatmax], kbuf[4][Offksz], vbuf[4][Ptrsz];
	char *p, *e;
	vlong n, o, c;
	int i, j;
	Bptr bp[4];
	Msg kv[4];
	Fcall r;
	Fid *f;

	if((f = getfid(m->fid)) == nil){
		rerror(m, Efid);
		return;
	}
	if(!(f->mode & DMWRITE)){
print("f->mode: %x\n", f->mode);
		rerror(m, Einuse);
		putfid(f);
		return;
	}

	wlock(f->dent);
	p = m->data;
	o = m->offset;
	c = m->count;
	for(i = 0; i < nelem(kv)-1 && c != 0; i++){
		kv[i].op = Oinsert;
		kv[i].k = kbuf[i];
		kv[i].nk = sizeof(kbuf[i]);
		kv[i].v = vbuf[i];
		kv[i].nv = sizeof(vbuf[i]);
		n = writeb(f, &kv[i], &bp[i], p, o, c, f->dent->length);
		if(n == -1){
			for(j = 0; j < i; j++)
				freebp(f->mnt->root, bp[i]);
			wunlock(f->dent);
			rerror(m, "%r");
			putfid(f);
			abort();
			return;
		}
		p += n;
		o += n;
		c -= n;
	}

	p = sbuf;
	kv[i].op = Owstat;
	kv[i].k = f->dent->k;
	kv[i].nk = f->dent->nk;
	n = m->offset+m->count;
	*p++ = 0;
	if(n > f->dent->length){
		sbuf[0] |= Owsize;
		PBIT64(p, n);
		p += 8;
		f->dent->length = m->offset+m->count;
	}
	sbuf[0] |= Owmuid;
	PBIT32(p, f->mnt->uid);
	p += 4;

	kv[i].v = sbuf;
	kv[i].nv = p - sbuf;
	if((e = btupsert(f->mnt->root, kv, i+1)) != nil){
		rerror(m, e);
		putfid(f);
		abort();
		return;
	}
	wunlock(f->dent);

	if((e = updatesnap(f)) != nil){
		rerror(m, e);
		putfid(f);
		return;
	}

	r.type = Rwrite;
	r.count = m->count;
 	respond(m, &r);
	putfid(f);
}

void
runfs(int wid, void *pfd)
{
	int fd, msgmax, versioned;
	char err[128];
	QLock *wrlk;
	Fcall r;
	Fmsg *m;

	fd = (uintptr)pfd;
	msgmax = Max9p;
	versioned = 0;
	if((wrlk = mallocz(sizeof(QLock), 1)) == nil)
		fshangup(fd, "alloc wrlk: %r");
	while(1){
		if((m = readmsg(fd, msgmax)) == nil){
			fshangup(fd, "truncated message: %r");
			return;
		}
		quiesce(wid);
		if(convM2S(m->buf, m->sz, m) == 0){
			fshangup(fd, "invalid message: %r");
			return;
		}
		if(m->type != Tversion && !versioned){
			fshangup(fd, "version required");
			return;
		}
		m->wrlk = wrlk;
		versioned = 1;
		dprint("← %F\n", &m->Fcall);
		switch(m->type){
		/* sync setup */
		case Tversion:	fsversion(m, &msgmax);	break;
		case Tauth:	fsauth(m);		break;
		case Tclunk:	fsclunk(m);		break;
		case Topen:	fsopen(m);		break;
		case Tattach:	fsattach(m, msgmax);	break;

		/* mutators */
		case Tflush:	chsend(fs->wrchan, m);	break;
		case Tcreate:	chsend(fs->wrchan, m);	break;
		case Twrite:	chsend(fs->wrchan, m);	break;
		case Twstat:	chsend(fs->wrchan, m);	break;
		case Tremove:	chsend(fs->wrchan, m);	break;

		/* reads */
		case Twalk:	chsend(fs->rdchan, m);	break;
		case Tread:	chsend(fs->rdchan, m);	break;
		case Tstat:	chsend(fs->rdchan, m);	break;
		default:
			fprint(2, "unknown message %F\n", &m->Fcall);
			snprint(err, sizeof(err), "unknown message: %F", &m->Fcall);
			r.type = Rerror;
			r.ename = err;
			respond(m, &r);
			break;
		}
		quiesce(wid);
	}
}

void
runwrite(int wid, void *)
{
	Fmsg *m;

	while(1){
		m = chrecv(fs->wrchan);
		quiesce(wid);
		switch(m->type){
		case Tflush:	rerror(m, Eimpl);	break;
		case Tcreate:	fscreate(m);	break;
		case Twrite:	fswrite(m);	break;
		case Twstat:	fswstat(m);	break;
		case Tremove:	fsremove(m);	break;
		}
		quiesce(wid);
	}
}

void
runread(int wid, void *)
{
	Fmsg *m;

	while(1){
		m = chrecv(fs->rdchan);
		quiesce(wid);
		switch(m->type){
		case Twalk:	fswalk(m);	break;
		case Tread:	fsread(m);	break;
		case Tstat:	fsstat(m);	break;
		}
		quiesce(wid);
	}
}
