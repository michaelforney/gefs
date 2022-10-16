#include <u.h>
#include <libc.h>
#include <auth.h>
#include <fcall.h>
#include <pool.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"

static char*	clearb(Fid*, vlong, vlong);

static char*
updatemount(Mount *mnt)
{
	Tree *t, *n;
	char *e;

	t = mnt->root;
	if(!t->dirty)
		return nil;
	if((n = newsnap(t)) == nil){
		fprint(2, "snap: save %s: %s\n", mnt->name, "create snap");
		abort();
	}
	if((e = labelsnap(mnt->name, t->gen)) != nil){
		fprint(2, "snap: save %s: %s\n", mnt->name, e);
		abort();
	}
	if(t->dead[0].prev != -1){
		if((e = unrefsnap(t->dead[0].prev, t->gen)) != nil){
			fprint(2, "snap: unref old: %s\n", e);
			abort();
		}
	}
	lock(mnt);
	mnt->root = n;
	unlock(mnt);
	closesnap(t);
	return nil;
}

static void
snapfs(int fd, char *old, char *new)
{
	Tree *t, *u;
	char *e;

	u = openlabel(new);
	if((t = openlabel(old)) == nil){
		fprint(fd, "snap: open %s: does not exist\n", old);
		return;
	}
	if((e = labelsnap(new, t->gen)) != nil){
		fprint(fd, "snap: label %s: %s\n", new, e);
		return;
	}
	if(u != nil){
		if((e = unrefsnap(u->gen, -1)) != nil){
			fprint(fd, "snap: unref %s: %s\n", new, e);
			return;
		}
	}
	if(u != nil)
		closesnap(u);
	closesnap(t);
	/* we probably want explicit snapshots to get synced */
	sync();
	fprint(fd, "snap taken: %s\n", new);
}

static void
freemsg(Fmsg *m)
{
	free(m->a);
	free(m);
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

void*
chrecv(Chan *c, int block)
{
	void *a;
	long v;

	v = c->count;
	if(v == 0 || cas(&c->count, v, v-1) == 0){
		if(block)
			semacquire(&c->count, 1);
		else
			return nil;
	}
	lock(&c->rl);
	a = *c->rp;
	if(++c->rp >= &c->args[c->size])
		c->rp = c->args;
	unlock(&c->rl);
	semrelease(&c->avail, 1);
	return a;
}

void
chsend(Chan *c, void *m)
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

static void
fshangup(int fd, char *fmt, ...)
{
	char buf[ERRMAX];
	va_list ap;

	va_start(ap, fmt);
	vsnprint(buf, sizeof(buf), fmt, ap);
	va_end(ap);
	fprint(2, "%s\n", buf);
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
	w = write(m->conn->fd, buf, n);
	if(w != n)
		fshangup(m->conn->fd, Eio);
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
lookup(Fid *f, Key *k, Kvp *kv, char *buf, int nbuf)
{
	char *e;
	Tree *r;

	if(f->mnt == nil)
		return Eattach;
	lock(f->mnt);
	r = f->mnt->root;
	ainc(&r->memref);
	unlock(f->mnt);
	e = btlookup(r, k, kv, buf, nbuf);
	closesnap(r);
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
readb(Fid *f, char *d, vlong o, vlong n, vlong sz)
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
	if(fo+n > Blksz)
		n = Blksz-fo;

	k.k = buf;
	k.nk = sizeof(buf);
	k.k[0] = Kdat;
	PBIT64(k.k+1, f->qpath);
	PBIT64(k.k+9, fb);

	e = lookup(f, &k, &kv, kvbuf, sizeof(kvbuf));
	if(e != nil){
		if(e != Eexist){
			werrstr(e);
			return -1;
		}
		memset(d, 0, n);
		return n;
	}

	bp = unpackbp(kv.v, kv.nv);
	if((b = getblk(bp, GBraw)) == nil)
		return -1;
	memcpy(d, b->buf+fo, n);
	dropblk(b);
	return n;
}

static int
writeb(Fid *f, Msg *m, Bptr *ret, char *s, vlong o, vlong n, vlong sz)
{
	char *e, buf[Kvmax];
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
	t = nil;
	if(fb < sz && (fo != 0 || n != Blksz)){
		e = lookup(f, m, &kv, buf, sizeof(buf));
		if(e == nil){
			bp = unpackbp(kv.v, kv.nv);
			if((t = getblk(bp, GBraw)) == nil)
				return -1;
			memcpy(b->buf, t->buf, Blksz);
			freeblk(f->mnt->root, t);
			dropblk(t);
		}else if(e != Eexist){
			werrstr("%s", e);
			return -1;
		}
	}
	if(fo+n > Blksz)
		n = Blksz-fo;
	memcpy(b->buf+fo, s, n);
	if(t == nil){
		if(fo > 0)
			memset(b->buf, 0, fo);
		if(fo+n < Blksz)
			memset(b->buf+fo+n, 0, Blksz-fo-n);
	}
	enqueue(b);

	packbp(m->v, m->nv, &b->bp);
	*ret = b->bp;
	dropblk(b);
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
			goto Out;
		}
	}

	if((de = mallocz(sizeof(Dent), 1)) == nil)
		goto Out;
	de->Xdir = *d;
	de->ref = 1;
	de->qid = d->qid;
	de->length = d->length;

	if((e = packdkey(de->buf, sizeof(de->buf), pqid, d->name)) == nil){
		free(de);
		de = nil;
		goto Out;
	}
	de->k = de->buf;
	de->nk = e - de->buf;
	de->name = de->buf + 11;
	de->next = fs->dtab[h];
	fs->dtab[h] = de;

Out:
	unlock(&fs->dtablk);
	return de;
}

static Mount *
getmount(char *name)
{
	Mount *mnt;
	Tree *t;

	lock(&fs->mountlk);
	for(mnt = fs->mounts; mnt != nil; mnt = mnt->next)
		if(strcmp(name, mnt->name) == 0){
			ainc(&mnt->ref);
			goto Out;
		}
	if((mnt = mallocz(sizeof(*mnt), 1)) == nil)
		goto Out;
	mnt->ref = 1;
	if((mnt->name = strdup(name)) == nil){
		free(mnt);
		mnt = nil;
		goto Out;
	}
	if((t = openlabel(name)) == nil){
		werrstr("%s", Enosnap);
		free(mnt->name);
		free(mnt);
		mnt = nil;
		goto Out;
	}
	mnt->root = newsnap(t);
	closesnap(t);
	if(mnt->root == nil){
		free(mnt->name);
		free(mnt);
		mnt = nil;
		goto Out;
	}

	mnt->next = fs->mounts;
	fs->mounts = mnt;

Out:
	unlock(&fs->mountlk);
	return mnt;
}

static void
clunkmount(Mount *mnt)
{
	Mount *me, **p;

	if(mnt == nil)
		return;
	lock(&fs->mountlk);
	if(adec(&mnt->ref) == 0){
		for(p = &fs->mounts; (me = *p) != nil; p = &me->next){
			if(me == mnt)
				break;
		}
		assert(me != nil);
		*p = me->next;
		free(mnt->name);
		free(mnt);
	}
	unlock(&fs->mountlk);
}

static void
clunkdent(Dent *de)
{
	Dent *e, **pe;
	u32int h;

	if(de == nil)
		return;
	if(de->qid.type == QTAUTH && adec(&de->ref) == 0){
		free(de);
		return;
	}
	lock(&fs->dtablk);
	if(adec(&de->ref) != 0)
		goto Out;
	h = ihash(de->qid.path) % Ndtab;
	pe = &fs->dtab[h];
	for(e = fs->dtab[h]; e != nil; e = e->next){
		if(e == de)
			break;
		pe = &e->next;
	}
	assert(e != nil);
	*pe = e->next;
	free(de);
Out:
	unlock(&fs->dtablk);
}

void
showfid(int fd, char**, int)
{
	int i;
	Fid *f;
	Conn *c;

	for(c = fs->conns; c != nil; c = c->next){
		lock(&c->fidtablk);
		fprint(fd, "fids:%d\n", c->fd);
		for(i = 0; i < Nfidtab; i++)
			for(f = c->fidtab[i]; f != nil; f = f->next){
				rlock(f->dent);
				fprint(fd, "\tfid[%d]: %d [refs=%ld, k=%K, qid=%Q]\n",
					i, f->fid, f->dent->ref, &f->dent->Key, f->dent->qid);
				runlock(f->dent);
			}
		unlock(&c->fidtablk);
	}
}

static Fid*
getfid(Conn *c, u32int fid)
{
	u32int h;
	Fid *f;

	h = ihash(fid) % Nfidtab;
	lock(&c->fidtablk);
	for(f = c->fidtab[h]; f != nil; f = f->next)
		if(f->fid == fid){
			ainc(&f->ref);
			break;
		}
	unlock(&c->fidtablk);
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
dupfid(Conn *c, u32int new, Fid *f)
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

	lock(&c->fidtablk);
	for(o = c->fidtab[h]; o != nil; o = o->next)
		if(o->fid == new)
			break;
	if(o == nil){
		n->next = c->fidtab[h];
		c->fidtab[h] = n;
	}
	unlock(&c->fidtablk);

	if(o != nil){
		fprint(2, "fid in use: %d == %d\n", o->fid, new);
		abort();
		free(n);
		return nil;
	}
	if(n->mnt != nil)
		ainc(&n->mnt->ref);
	ainc(&n->dent->ref);
	return n;
}

static void
clunkfid(Conn *c, Fid *fid)
{
	Fid *f, **pf;
	u32int h;

	lock(&c->fidtablk);
	h = ihash(fid->fid) % Nfidtab;
	pf = &c->fidtab[h];
	for(f = c->fidtab[h]; f != nil; f = f->next){
		if(f == fid){
			assert(adec(&f->ref) != 0);
			*pf = f->next;
			break;
		}
		pf = &f->next;
	}
	assert(f != nil);
	unlock(&c->fidtablk);
}

static int
readmsg(Conn *c, Fmsg **pm)
{
	char szbuf[4];
	int sz, n;
	Fmsg *m;

	n = readn(c->fd, szbuf, 4);
	if(n <= 0){
		*pm = nil;
		return n;
	}
	if(n != 4){
		werrstr("short read: %r");
		return -1;
	}
	sz = GBIT32(szbuf);
	if(sz > c->iounit){
		werrstr("message size too large");
		return -1;
	}
	if((m = malloc(sizeof(Fmsg)+sz)) == nil)
		return -1;
	if(readn(c->fd, m->buf+4, sz-4) != sz-4){
		werrstr("short read: %r");
		free(m);
		return -1;
	}
	m->conn = c;
	m->sz = sz;
	m->a = nil;
	PBIT32(m->buf, sz);
	*pm = m;
	return 0;
}

static void
fsversion(Fmsg *m)
{
	Fcall r;
	char *p;

	memset(&r, 0, sizeof(Fcall));
	p = strchr(m->version, '.');
	if(p != nil)
		*p = '\0';
	r.type = Rversion;
	r.msize = Max9p;
	if(strcmp(m->version, "9P2000") == 0){
		if(m->msize < r.msize)
			r.msize = m->msize;
		r.version = "9P2000";
		m->conn->versioned = 1;
		m->conn->iounit = r.msize;
	}else{
		r.version = "unknown";
		m->conn->versioned = 0;
	}
	respond(m, &r);
}

void
authfree(AuthRpc *auth)
{
	AuthRpc *rpc;

	if(rpc = auth){
		close(rpc->afd);
		auth_freerpc(rpc);
	}
}

AuthRpc*
authnew(void)
{
	static char *keyspec = "proto=p9any role=server";
	AuthRpc *rpc;
	int fd;

	if(access("/mnt/factotum", 0) < 0)
		if((fd = open("/srv/factotum", ORDWR)) >= 0)
			mount(fd, -1, "/mnt", MBEFORE, "");
	if((fd = open("/mnt/factotum/rpc", ORDWR)) < 0)
		return nil;
	if((rpc = auth_allocrpc(fd)) == nil){
		close(fd);
		return nil;
	}
	if(auth_rpc(rpc, "start", keyspec, strlen(keyspec)) != ARok){
		authfree(rpc);
		return nil;
	}
	return rpc;
}

static void
fsauth(Fmsg *m)
{
	Dent *de;
	Fcall r;
	Fid f;

	if(fs->noauth){
		rerror(m, Eauth);
		return;
	}
	if((de = mallocz(sizeof(Dent), 1)) == nil){
		rerror(m, Enomem);
		return;
	}
	memset(de, 0, sizeof(Dent));
	de->ref = 0;
	de->qid.type = QTAUTH;
	de->qid.path = inc64(&fs->nextqid, 1);
	de->qid.vers = 0;
	de->length = 0;
	de->k = nil;
	de->nk = 0;

	memset(&f, 0, sizeof(Fid));
	f.fid = NOFID;
	f.mnt = nil;
	f.qpath = de->qid.path;
	f.pqpath = de->qid.path;
	f.mode = -1;
	f.iounit = m->conn->iounit;
	f.dent = de;
	f.uid = -1;
	f.duid = -1;
	f.dgid = -1;
	f.dmode = 0600;
	f.auth = authnew();
	if(dupfid(m->conn, m->afid, &f) == nil){
		rerror(m, Enomem);
		free(de);
		return;
	}
	r.type = Rauth;
	r.aqid = de->qid;
	respond(m, &r);
}

static int
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

static int
groupleader(int uid, int gid)
{
	User *g;
	int i, lead;

	lead = 0;
	rlock(&fs->userlk);
	g = uid2user(gid);
	if(g != nil){
		if(g->lead == 0){
			for(i = 0; i < g->nmemb; i++)
				if(g->memb[i] == uid){
					lead = 1;
					break;
				}
		}else if(uid == g->lead)
			lead = 1;
	}
	runlock(&fs->userlk);
	return lead;

}

static int
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

static int
fsaccess(Fid *f, ulong fmode, int fuid, int fgid, int m)
{
	/* uid none gets only other permissions */
	if(f->uid != 0) {
		if(f->uid == fuid)
			if((m & (fmode>>6)) == m)
				return 0;
		if(ingroup(f->uid, fgid))
			if((m & (fmode>>3)) == m)
				return 0;
	}
	if(m & fmode) {
		if((fmode & DMDIR) && (m == DMEXEC))
			return 0;
		if(!ingroup(f->uid, 9999))
			return 0;
	}
	return -1;
}

static void
fsattach(Fmsg *m)
{
	char *e, *p, *n, dbuf[Kvmax], kvbuf[Kvmax];
	Mount *mnt;
	Dent *de;
	User *u;
	Fcall r;
	Xdir d;
	Kvp kv;
	Key dk;
	Fid f;
	int uid;

	de = nil;
	if(m->aname[0] == '\0')
		m->aname = "main";
	if((mnt = getmount(m->aname)) == nil){
		rerror(m, "%r");
		goto Out;
	}

	rlock(&fs->userlk);
	n = (forceuser == nil) ? m->uname : forceuser;
	if((u = name2user(n)) == nil){
		runlock(&fs->userlk);
		rerror(m, Enouser);
		goto Out;
	}
	uid = u->id;
	runlock(&fs->userlk);

	if((p = packdkey(dbuf, sizeof(dbuf), -1ULL, "")) == nil){
		rerror(m, Elength);
		goto Out;
	}
	dk.k = dbuf;
	dk.nk = p - dbuf;
	if((e = btlookup(mnt->root, &dk, &kv, kvbuf, sizeof(kvbuf))) != nil){
		rerror(m, e);
		goto Out;
	}
	if(kv2dir(&kv, &d) == -1){
		rerror(m, Efs);
		goto Out;
	}
	if((de = getdent(-1, &d)) == nil){
		rerror(m, Efs);
		goto Out;
	}

	memset(&f, 0, sizeof(Fid));
	f.fid = NOFID;
	f.mnt = mnt;
	f.qpath = d.qid.path;
	f.pqpath = d.qid.path;
	f.mode = -1;
	f.iounit = m->conn->iounit;
	f.dent = de;
	f.uid = uid;
	f.duid = d.uid;
	f.dgid = d.gid;
	f.dmode = d.mode;
	if(dupfid(m->conn, m->fid, &f) == nil){
		rerror(m, Enomem);
		goto Out;
	}

	r.type = Rattach;
	r.qid = d.qid;
	respond(m, &r);

Out:
	clunkdent(de);
	clunkmount(mnt);
}

static char*
findparent(Fid *f, vlong *qpath, char **name, char *buf, int nbuf)
{
	char *p, *e, kbuf[Keymax];
	Kvp kv;
	Key k;

	if((p = packsuper(kbuf, sizeof(kbuf), f->pqpath)) == nil)
		return Elength;
	k.k = kbuf;
	k.nk = p - kbuf;
	if((e = lookup(f, &k, &kv, buf, nbuf)) != nil)
		return e;
	if((*name = unpackdkey(kv.v, kv.nv, qpath)) == nil)
		return Efs;
	return nil;
}

static void
fswalk(Fmsg *m)
{
	char *p, *e, *name, kbuf[Maxent], kvbuf[Kvmax];
	int duid, dgid, dmode;
	vlong up, prev;
	Fid *o, *f;
	Dent *dent;
	Fcall r;
	Xdir d;
	Kvp kv;
	Key k;
	int i;

	if((o = getfid(m->conn, m->fid)) == nil){
		rerror(m, Efid);
		return;
	}
	if(o->mode != -1){
		rerror(m, Einuse);
		putfid(o);
		return;
	}
	e = nil;
	up = o->qpath;
	prev = o->qpath;
	rlock(o->dent);
	d = *o->dent;
	runlock(o->dent);
	duid = d.uid;
	dgid = d.gid;
	dmode = d.mode;
	r.type = Rwalk;
	for(i = 0; i < m->nwname; i++){
		if(fsaccess(o, d.mode, d.uid, d.gid, DMEXEC) != 0){
			rerror(m, Eperm);
			putfid(o);
			return;
		}
		name = m->wname[i];
		if(strcmp(m->wname[i], "..") == 0){
			if((e = findparent(o, &prev, &name, kbuf, sizeof(kbuf))) != nil){
				rerror(m, e);
				putfid(o);
				return;
			}
		}
		if((p = packdkey(kbuf, sizeof(kbuf), prev, name)) == nil){
			rerror(m, Elength);
			putfid(o);
			return;
		}
		k.k = kbuf;
		k.nk = p - kbuf;
		if((e = lookup(o, &k, &kv, kvbuf, sizeof(kvbuf))) != nil)
			break;
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
		if((f = dupfid(m->conn, m->newfid, o)) == nil){
			rerror(m, Enomem);
			putfid(o);
			return;
		}
		putfid(o);
	}
	if(i > 0 && i == m->nwname){
		dent = getdent(up, &d);
		if(dent == nil){
			if(f != o)
				clunkfid(m->conn, f);
			rerror(m, Enomem);
			putfid(f);
			return;
		}
		f->qpath = r.wqid[i-1].path;
		f->pqpath = up;
		f->dent = dent;
		f->duid = duid;
		f->dgid = dgid;
		f->dmode = dmode;
	}
	respond(m, &r);
	putfid(f);
}

static void
fsstat(Fmsg *m)
{
	char buf[STATMAX];
	Fcall r;
	Fid *f;
	int n;

	if((f = getfid(m->conn, m->fid)) == nil){
		rerror(m, Efid);
		return;
	}
	rlock(f->dent);
	n = dir2statbuf(f->dent, buf, sizeof(buf));
	runlock(f->dent);
	if(n == -1){
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

static void
fswstat(Fmsg *m)
{
	char *p, *e, strs[65535], rnbuf[Kvmax], opbuf[Kvmax];
	int op, nm, rename;
	Fcall r;
	Dent *de;
	Msg mb[2];
	Xdir n;
	Dir d;
	Fid *f;
	Key k;
	User *u;

	rename = 0;
	if((f = getfid(m->conn, m->fid)) == nil){
		rerror(m, Efid);
		return;
	}
	de = f->dent;
	wlock(de);
	if(de->qid.type & QTAUTH){
		rerror(m, Emode);
		goto Out;
	}
	if(convM2D(m->stat, m->nstat, &d, strs) <= BIT16SZ){
		rerror(m, Edir);
		goto Out;
	}

	n = de->Xdir;
	n.qid.vers++;
	p = opbuf+1;
	op = 0;

	/* check validity of updated fields and construct Owstat message */
	if(d.qid.path != ~0 || d.qid.vers != ~0){
		if(d.qid.path != de->qid.path){
			rerror(m, Ewstatp);
			goto Out;
		}
		if(d.qid.vers != de->qid.vers){
			rerror(m, Ewstatv);
			goto Out;
		}
	}
	if(*d.name != '\0'){
		if(strcmp(d.name, de->name) != 0){
			rename = 1;
			if(okname(d.name) == -1){
				rerror(m, Ename);
				goto Out;
			}
			n.name = d.name;
		}
	}
	if(d.length != ~0){
		if(d.length < 0){
			rerror(m, Ewstatl);
			goto Out;
		}
		if(d.length != de->length){
			n.length = d.length;
			op |= Owsize;
			PBIT64(p, n.length);
			p += 8;
		}
	}
	if(d.mode != ~0){
		if((d.mode^de->mode) & DMDIR){
			rerror(m, Ewstatd);
			goto Out;
		}
		if(d.mode & ~(DMDIR|DMAPPEND|DMEXCL|DMTMP|0777)){
			rerror(m, Ewstatb);
			goto Out;
		}
		if(d.mode != de->mode){
			n.mode = d.mode;
			n.qid.type = d.mode>>24;
			op |= Owmode;
			PBIT32(p, n.mode);
			p += 4;
		}
	}
	if(d.mtime != ~0){
		n.mtime = d.mtime*Nsec;
		if(n.mtime != de->mtime){
			op |= Owmtime;
			PBIT64(p, n.mtime);
			p += 8;
		}
	}
	if(*d.uid != '\0'){
		rlock(&fs->userlk);
		u = name2user(d.uid);
		if(u == nil){
			runlock(&fs->userlk);
			rerror(m, Enouser);
			goto Out;
		}
		n.uid = u->id;
		runlock(&fs->userlk);
		if(n.uid != de->uid){
			op |= Owuid;
			PBIT32(p, n.uid);
			p += 4;
		}
	}
	if(*d.gid != '\0'){
		rlock(&fs->userlk);
		u = name2user(d.gid);
		if(u == nil){
			runlock(&fs->userlk);
			rerror(m, Enouser);
			goto Out;
		}
		n.gid = u->id;
		runlock(&fs->userlk);
		if(n.gid != de->gid){
			op |= Owgid;
			PBIT32(p, n.gid);
			p += 4;
		}
	}
	op |= Owmuid;
	n.muid = f->uid;
	PBIT32(p, n.muid);
	p += 4;

	/* check permissions */
	if(rename){
		if(fsaccess(f, f->dmode, f->duid, f->dgid, DMWRITE) == -1){
			rerror(m, Eperm);
			goto Out;
		}
	}
	if(op & Owsize){
		if(fsaccess(f, de->mode, de->uid, de->gid, DMWRITE) == -1){
			rerror(m, Eperm);
			goto Out;
		}
	}
	if(op & (Owmode|Owmtime)){
		if(f->uid != de->uid && !groupleader(f->uid, de->gid)){
			rerror(m, Ewstato);
			goto Out;
		}
	}
	if(op & Owuid){
		/* FIXME: should allow during bootstrapping */
		rerror(m, Ewstatu);
		goto Out;
	}
	if(op & Owgid){
		if(!(f->uid == de->uid && ingroup(f->uid, n.gid))
		&& !(groupleader(f->uid, de->gid) && groupleader(f->uid, n.gid))){
			rerror(m, Ewstatg);
			goto Out;
		}
	}

	/* update directory entry */
	nm = 0;
	if(rename){
		mb[nm].op = Odelete;
		mb[nm].Key = de->Key;
		mb[nm].v = nil;
		mb[nm].nv = 0;
		nm++;

		mb[nm].op = Oinsert;
		if(dir2kv(f->pqpath, &n, &mb[nm], rnbuf, sizeof(rnbuf)) == -1){
			rerror(m, Efs);
			goto Out;
		}
		k = mb[nm].Key;
		nm++;
	}else{
		opbuf[0] = op;
		mb[nm].op = Owstat;
		mb[nm].Key = de->Key;
		mb[nm].v = opbuf;
		mb[nm].nv = p - opbuf;
		nm++;
	}
	if((e = btupsert(f->mnt->root, mb, nm)) != nil){
		rerror(m, e);
		goto Out;
	}

	de->Xdir = n;
	if(rename)
		cpkey(de, &k, de->buf, sizeof(de->buf));

	r.type = Rwstat;
	respond(m, &r);

Out:
	wunlock(de);
	putfid(f);
}


static void
fsclunk(Fmsg *m)
{
	Fcall r;
	Fid *f;

	if((f = getfid(m->conn, m->fid)) == nil){
		rerror(m, Efid);
		return;
	}

	lock(f);
	if(f->scan != nil){
		btdone(f->scan);
		free(f->scan);
		f->scan = nil;
	}
	unlock(f);

	clunkfid(m->conn, f);
	r.type = Rclunk;
	respond(m, &r);
	putfid(f);
}

static void
fscreate(Fmsg *m)
{
	char *p, *e, buf[Kvmax], upkbuf[Keymax], upvbuf[Inlmax];
	Dent *de;
	Fcall r;
	Msg mb[2];
	Fid *f;
	Xdir d;
	int nm;

	if(okname(m->name) == -1){
		rerror(m, Ename);
		return;
	}
	if((f = getfid(m->conn, m->fid)) == nil){
		rerror(m, Efid);
		return;
	}
	if(m->perm & (DMMOUNT|DMAUTH)){
		rerror(m, "unknown permission");
		putfid(f);
		return;
	}
	de = f->dent;
	rlock(de);
	if(fsaccess(f, de->mode, de->uid, de->gid, DMWRITE) == -1){
		runlock(de);
		rerror(m, Eperm);
		putfid(f);
		return;
	}
	d.gid = de->gid;
	runlock(de);

	nm = 0;
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
	if(m->perm & DMDIR)
		d.mode &= ~0777 | de->mode & 0777;
	else
		d.mode &= ~0666 | de->mode & 0666;
	d.name = m->name;
	d.atime = nsec();
	d.mtime = d.atime;
	d.length = 0;
	d.uid = f->uid;
	d.muid = f->uid;

	mb[nm].op = Oinsert;
	if(dir2kv(f->qpath, &d, &mb[nm], buf, sizeof(buf)) == -1){
		rerror(m, Efs);
		putfid(f);
		return;
	}
	nm++;

	if(m->perm & DMDIR){
		mb[nm].op = Oinsert;
		if((p = packsuper(upkbuf, sizeof(upkbuf), d.qid.path)) == nil)
			sysfatal("ream: pack super");
		mb[nm].k = upkbuf;
		mb[nm].nk = p - upkbuf;
		if((p = packdkey(upvbuf, sizeof(upvbuf), f->qpath, d.name)) == nil)
			sysfatal("ream: pack super");
		mb[nm].v = upvbuf;
		mb[nm].nv = p - upvbuf;
		nm++;
	}
	if((e = btupsert(f->mnt->root, mb, nm)) != nil){
		rerror(m, e);
		putfid(f);
		return;
	}
	de = getdent(f->qpath, &d);
	if(de == nil){
		if(m->fid != m->newfid)
			clunkfid(m->conn, f);
		rerror(m, Enomem);
		putfid(f);
		return;
	}else{
		wlock(de);
		de->length = 0;
		wunlock(de);
	}

	lock(f);
	if(f->mode != -1){
		unlock(f);
		clunkdent(de);
		rerror(m, Einuse);
		putfid(f);
		return;
	}
	f->mode = mode2bits(m->mode);
	f->pqpath = f->qpath;
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
	respond(m, &r);
	putfid(f);
}

static char*
candelete(Fid *f)
{
	char *e, pfx[Dpfxsz];
	int done;
	Scan *s;

	if(f->dent->qid.type == QTFILE)
		return nil;
	if((s = mallocz(sizeof(Scan), 1)) == nil)
		return Enomem;

	packdkey(pfx, sizeof(pfx), f->qpath, nil);
	if((e = btscan(f->mnt->root, s, pfx, sizeof(pfx))) != nil)
		goto Out;
	done = 0;
	if((e = btnext(s, &s->kv, &done)) != nil)
		goto Out;
	if(!done)
		e = Enempty;
Out:
	btdone(s);
	free(s);
	return e;
}

static void
fsremove(Fmsg *m)
{
	Fcall r;
	Msg mb;
	Fid *f;
	char *e;

	if((f = getfid(m->conn, m->fid)) == nil){
		rerror(m, Efid);
		return;
	}
	clunkfid(m->conn, f);

	rlock(f->dent);
	if((e = candelete(f)) != nil)
		goto Error;
	if(fsaccess(f, f->dmode, f->duid, f->dgid, DMWRITE) == -1){
		e = Eperm;
		goto Error;
	}
	mb.op = Odelete;
	mb.k = f->dent->k;
	mb.nk = f->dent->nk;
	mb.nv = 0;
	if((e = btupsert(f->mnt->root, &mb, 1)) != nil)
		goto Error;
	if(f->dent->qid.type == QTFILE){
		e = clearb(f, 0, f->dent->length);
		if(e != nil)
			goto Error;
	}

	runlock(f->dent);
	r.type = Rremove;
	respond(m, &r);
	putfid(f);
	return;

Error:
	runlock(f->dent);
	rerror(m, e);
	putfid(f);
}

static void
fsopen(Fmsg *m)
{
	char *p, *e, buf[Kvmax];
	int mbits;
	Fcall r;
	Xdir d;
	Fid *f;
	Kvp kv;
	Msg mb;

	mbits = mode2bits(m->mode);
	if((f = getfid(m->conn, m->fid)) == nil){
		rerror(m, Efid);
		return;
	}
	if((e = lookup(f, f->dent, &kv, buf, sizeof(buf))) != nil){
		rerror(m, e);
		putfid(f);
		return;
	}
	if(kv2dir(&kv, &d) == -1){
		rerror(m, Efs);
		putfid(f);
		return;
	}
	if(fsaccess(f, d.mode, d.uid, d.gid, mbits) == -1){
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
		unlock(f);
		putfid(f);
		return;
	}
	f->mode = mode2bits(m->mode);
//	if(!fs->rdonly && (m->mode == OEXEC)){
//		lock(&fs->root.lk);
//		f->root = fs->root;
//		refblk(fs->root.bp);
//		unlock(&fs->root.lk);
//	}
	if(m->mode & OTRUNC){
		wlock(f->dent);
		f->dent->muid = f->uid;
		f->dent->qid.vers++;
		f->dent->length = 0;

		mb.op = Owstat;
		p = buf;
		p[0] = Owsize|Owmuid;	p += 1;
		PBIT64(p, 0);		p += 8;
		PBIT32(p, f->uid);	p += 4;
		mb.k = f->dent->k;
		mb.nk = f->dent->nk;
		mb.v = buf;
		mb.nv = p - buf;
		clearb(f, 0, f->dent->length);
		if((e = btupsert(f->mnt->root, &mb, 1)) != nil){
			wunlock(f->dent);
			rerror(m, e);
			putfid(f);
			return;
		}
		wunlock(f->dent);
	}
	unlock(f);
	respond(m, &r);
	putfid(f);
}

static char*
readauth(Fmsg *m, Fid *f, Fcall *r)
{
	AuthInfo *ai;
	AuthRpc *rpc;
	User *u;

	if((rpc = f->auth) == nil)
		return Etype;

	switch(auth_rpc(rpc, "read", nil, 0)){
	default:
		return Eauthp;
	case ARdone:
		if((ai = auth_getinfo(rpc)) == nil)
			goto Phase;
		rlock(&fs->userlk);
		u = name2user(ai->cuid);
		auth_freeAI(ai);
		if(u == nil){
			runlock(&fs->userlk);
			return Enouser;
		}
		f->uid = u->id;
		runlock(&fs->userlk);
		return nil;
	case ARok:
		if(m->count < rpc->narg)
			return Eauthd;
		memmove(r->data, rpc->arg, rpc->narg);
		r->count = rpc->narg;
		return nil;
	case ARphase:
	Phase:
		return Ephase;
	}
}

static char*
readdir(Fmsg *m, Fid *f, Fcall *r)
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

		packdkey(pfx, sizeof(pfx), f->qpath, nil);
		if((e = btscan(f->mnt->root, s, pfx, sizeof(pfx))) != nil){
			btdone(s);
			free(s);
			return e;
		}

		lock(f);
		if(f->scan != nil){
			btdone(f->scan);
			free(f->scan);
		}
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
		if((ns = kv2statbuf(&s->kv, p, n)) == -1){
			r->count = 0;
			return nil;
		}
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

static char*
readfile(Fmsg *m, Fid *f, Fcall *r)
{
	vlong n, c, o;
	char *p;
	Dent *e;

	e = f->dent;
	rlock(e);
	if(m->offset > e->length){
		runlock(e);
		return nil;
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

static void
fsread(Fmsg *m)
{
	char *e;
	Fcall r;
	Fid *f;

	if((f = getfid(m->conn, m->fid)) == nil){
		rerror(m, Efid);
		return;
	}
	r.type = Rread;
	r.count = 0;
	if((r.data = malloc(m->count)) == nil){
		rerror(m, Enomem);
		putfid(f);
		return;
	}
	if(f->dent->qid.type & QTAUTH)
		e = readauth(m, f, &r);
	else if(f->dent->qid.type & QTDIR)
		e = readdir(m, f, &r);
	else
		e = readfile(m, f, &r);
	if(e != nil)
		rerror(m, e);
	else
		respond(m, &r);
	free(r.data);
	putfid(f);
}

static char*
writeauth(Fmsg *m, Fid *f, Fcall *r)
{
	AuthRpc *rpc;

	if((rpc = f->auth) == nil)
		return Etype;
	if(auth_rpc(rpc, "write", m->data, m->count) != ARok)
		return Ebotch;
	r->type = Rwrite;
	r->count = m->count;
	return nil;

}


static void
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

	if((f = getfid(m->conn, m->fid)) == nil){
		rerror(m, Efid);
		return;
	}
	if(!(f->mode & DMWRITE)){
		rerror(m, Einuse);
		putfid(f);
		return;
	}
	if(f->dent->qid.type == QTAUTH){
		e = writeauth(m, f, &r);
		if(e != nil)
			rerror(m, e);
		else
			respond(m, &r);
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
			fprint(2, "%r");
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
	sbuf[0] |= Owmtime;
	f->dent->mtime = nsec();
	PBIT64(p, f->dent->mtime);
	p += 8;
	sbuf[0] |= Owmuid;
	PBIT32(p, f->uid);
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

	r.type = Rwrite;
	r.count = m->count;
 	respond(m, &r);
	putfid(f);
}

void
runfs(int, void *pfd)
{
	Conn *c;
	int fd;
	char err[128];
	Fcall r;
	Fmsg *m;

	fd = (uintptr)pfd;
	if((c = mallocz(sizeof(*c), 1)) == nil){
		fshangup(fd, "malloc: %r");
		return;
	}
	c->fd = fd;
	c->iounit = Max9p;
	lock(&fs->connlk);
	c->next = fs->conns;
	fs->conns = c;
	unlock(&fs->connlk);
	while(1){
		if(readmsg(c, &m) < 0){
			fshangup(fd, "read message: %r");
			return;
		}
		if(m == nil)
			break;
		if(convM2S(m->buf, m->sz, m) == 0){
			fshangup(fd, "invalid message: %r");
			return;
		}
		if(m->type != Tversion && !c->versioned){
			fshangup(fd, "version required");
			return;
		}
		dprint("← %F\n", &m->Fcall);
		switch(m->type){
		/* sync setup */
		case Tversion:	fsversion(m);	break;
		case Tauth:	fsauth(m);	break;
		case Tclunk:	fsclunk(m);		break;

		/* mutators */
		case Tcreate:	chsend(fs->wrchan, m);	break;
		case Twrite:	chsend(fs->wrchan, m);	break;
		case Twstat:	chsend(fs->wrchan, m);	break;
		case Tremove:	chsend(fs->wrchan, m);	break;

		/* reads */
		case Tattach:	chsend(fs->rdchan, m);	break;
		case Tflush:	chsend(fs->rdchan, m);	break;
		case Twalk:	chsend(fs->rdchan, m);	break;
		case Tread:	chsend(fs->rdchan, m);	break;
		case Tstat:	chsend(fs->rdchan, m);	break;

		/* both */
		case Topen:
			if(m->mode & OTRUNC)
				chsend(fs->wrchan, m);
			else

				chsend(fs->rdchan, m);
			break;

		default:
			fprint(2, "unknown message %F\n", &m->Fcall);
			snprint(err, sizeof(err), "unknown message: %F", &m->Fcall);
			r.type = Rerror;
			r.ename = err;
			respond(m, &r);
			break;
		}
	}
}

void
runwrite(int wid, void *)
{
	Mount *mnt;
	Fmsg *m;
	int ao;

	while(1){
		m = chrecv(fs->wrchan, 1);
		epochstart(wid);
		ao = (m->a == nil) ? AOnone : m->a->op;
		switch(ao){
		case AOnone:
			if(fs->rdonly){
				rerror(m, Erdonly);
				return;
			}
			if(fs->broken){
				rerror(m, Efs);
				return;
			}
			switch(m->type){
			case Tcreate:	fscreate(m);	break;
			case Twrite:	fswrite(m);	break;
			case Twstat:	fswstat(m);	break;
			case Tremove:	fsremove(m);	break;
			case Topen:	fsopen(m);	break;
			}
			break;
		case AOsync:
			if(m->a->fd != -1)
				fprint(m->a->fd, "syncing [readonly: %d]\n", m->a->halt);
			if(m->a->halt)
				ainc(&fs->rdonly);
			for(mnt = fs->mounts; mnt != nil; mnt = mnt->next)
				updatemount(mnt);
			sync();
			if(m->a->fd != -1)
				fprint(m->a->fd, "sync done\n");
			freemsg(m);
			break;
		case AOsnap:
			snapfs(m->a->fd, m->a->old, m->a->new);
			freemsg(m);
			break;
		}
		epochend(wid);
	}
}

void
runread(int wid, void *)
{
	Fmsg *m;

	while(1){
		m = chrecv(fs->rdchan, 1);
		epochstart(wid);
		switch(m->type){
		case Tflush:	rerror(m, Eimpl);	break;
		case Tattach:	fsattach(m);	break;
		case Twalk:	fswalk(m);	break;
		case Tread:	fsread(m);	break;
		case Tstat:	fsstat(m);	break;
		case Topen:	fsopen(m);	break;
		}
		epochend(wid);
	}
}

void
runtasks(int, void *)
{
	Fmsg *m;
	Amsg *a;

	while(1){
		sleep(5000);
		m = mallocz(sizeof(Fmsg), 1);
		a = mallocz(sizeof(Amsg), 1);
		if(m == nil || a == nil){
			fprint(2, "alloc sync msg: %r\n");
			free(m);
			free(a);
			return;
		}
		a->op = AOsync;
		a->halt = 0;
		a->fd = -1;
		m->a = a;
		chsend(fs->wrchan, m);	
	}
}
