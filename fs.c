#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>
#include <bio.h>
#include <pool.h>

#include "dat.h"
#include "fns.h"

int
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

static vlong
nextqid(void)
{
	vlong q;

	lock(&fs->qidlk);
	q = fs->nextqid++;
	unlock(&fs->qidlk);
	return q;
}

static char*
fslookup(Fid *f, Key *k, Kvp *kv, Blk **bp, int lk)
{
	char *e;
	Blk *b;

	if(f->root.bp.addr == -1)
		b = getroot(&fs->root, nil);
	else
		b = getblk(f->root.bp, 0);
	if(b == nil)
		return Efs;

	if(lk)
		rlock(f->dent);
	e = btlookupat(b, k, kv, bp);
	if(lk)
		runlock(f->dent);
	putblk(b);
	return e;
}

static Dent*
getdent(vlong root, vlong pqid, Dir *d)
{
	Dent *e;
	char *ek, *eb;
	u32int h;
	int err;

	h = (ihash(d->qid.path) ^ ihash(root)) % Ndtab;
	lock(&fs->dtablk);
	for(e = fs->dtab[h]; e != nil; e = e->next){
		if(e->qid.path == d->qid.path && e->rootb == root){
			dprint("found %p [%K]\n", e, &e->Key);
			ainc(&e->ref);
			unlock(&fs->dtablk);
			return e;
		}
	}

	err = 0;
	if((e = mallocz(sizeof(Dent), 1)) == nil)
		return nil;
	e->ref = 1;
	e->qid = d->qid;
	e->rootb = root;
	e->k = e->buf;
	e->nk = 9 + strlen(d->name) + 1;

	ek = e->buf;
	eb = ek + sizeof(e->buf);
	ek = pack8(&err, ek, eb, Kent);
	ek = pack64(&err, ek, eb, pqid);
	ek = packstr(&err, ek, eb, d->name);
	e->nk = ek - e->buf;
	e->next = fs->dtab[h];
	fs->dtab[h] = e;
	dprint("created %p [%K]\n", e, &e->Key);

	unlock(&fs->dtablk);
	return e;
}

static void
clunkdent(Dent *de)
{
	Dent *e, **pe;
	u32int h;

	if(adec(&de->ref) == 0){
		h = (ihash(de->qid.path) ^ ihash(de->rootb)) % Ndtab;
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
showfids(int fd)
{
	int i;
	Fid *f;

	lock(&fs->fidtablk);
	fprint(fd, "fids:---\n");
	for(i = 0; i < Nfidtab; i++)
		for(f = fs->fidtab[i]; f != nil; f = f->next){
			rlock(f->dent);
			fprint(fd, "\tfid[%d]: %d [refs=%ld, k=%K]\n", i, f->fid, f->dent->ref, &f->dent->Key);
			runlock(f->dent);
		}
	unlock(&fs->fidtablk);
}

Fid*
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
	return f;
}

Fid*
dupfid(int new, Fid *f)
{
	Fid *n, *o;
	u32int h;

	h = ihash(new) % Nfidtab;
	if((n = malloc(sizeof(Fid))) == nil)
		return nil;

	*n = *f;
	n->fid = new;
	n->mode = -1;
	n->next = nil;

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
		werrstr("fid in use: %d == %d", o->fid, new);
		abort();
		free(n);
		return nil;
	}
	return n;
}

void
clunkfid(Fid *fid)
{
	Fid *f, **pf;
	u32int h;

	lock(&fs->fidtablk);
	h = ihash(fid->fid) % Nfidtab;
	pf = &fs->fidtab[h];
	for(f = fs->fidtab[h]; f != nil; f = f->next){
		if(f == fid){
			*pf = f->next;
			goto Found;
		}
		pf = &f->next;
	}
	abort();
Found:
	clunkdent(fid->dent);
	unlock(&fs->fidtablk);
	free(fid);
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

Fmsg*
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

void
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

void
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

void
fsauth(Fmsg *m)
{
	Fcall r;

	r.type = Rerror;
	r.ename = "unimplemented auth";
	respond(m, &r);
}

void
fsattach(Fmsg *m, int iounit)
{
	char *p, *ep, buf[128];
	int err;
	Dent *e;
	Fcall r;
	Kvp kv;
	Key dk;
	Blk *b;
	Fid f;
	Dir d;

	err = 0;
	p = buf;
	ep = buf + sizeof(buf);
	p = pack8(&err, p, ep, Kent);
	p = pack64(&err, p, ep, -1ULL);
	p = packstr(&err, p, ep, "");
	dk.k = buf;
	dk.nk = p - buf;
	if(btlookup(&fs->root, &dk, &kv, &b) != nil){
		rerror(m, Efs);
		return;
	}
	r.type = Rattach;
	if(kv2dir(&kv, &d) == -1){
		rerror(m, Efs);
		putblk(b);
		return;
	}
	if((e = getdent(-1, -1, &d)) == nil){
		rerror(m, Efs);
		putblk(b);
		return;
	}
	putblk(b);

	/*
	 * A bit of a hack; we're duping a fid
	 * that doesn't exist, so we'd be off
	 * by one on the refcount. Adjust to
	 * compensate for the dup.
	 */
	adec(&e->ref);

	memset(&f, 0, sizeof(Fid));
	f.fid = NOFID;
	f.qpath = d.qid.path;
	f.mode = -1;
	f.root.bp.addr = -1;
	f.root.bp.hash = -1;
	f.iounit = iounit;
	f.dent = e;
	if(dupfid(m->fid, &f) == nil){
		rerror(m, Enomem);
		return;
	}
	r.qid = d.qid;
	respond(m, &r);
	return;
}

void
fswalk(Fmsg *m)
{
	char *p, *e, *estr, kbuf[Maxent];
	int i, nwalk, err;
	vlong up, prev;
	Fid *o, *f;
	Dent *dent;
	Fcall r;
	Blk *b;
	Kvp kv;
	Key k;
	Dir d;

	if((o = getfid(m->fid)) == nil){
		rerror(m, Efid);
		return;
	}
	if(o->mode != -1){
		rerror(m, Einuse);
		return;
	}
	err = 0;
	estr = nil;
	nwalk = 0;
	up = o->qpath;
	prev = o->qpath;
	r.type = Rwalk;
	r.nwqid = 0;
	for(i = 0; i < m->nwname; i++){
		up = prev;
		p = kbuf;
		e = p + sizeof(kbuf);
		p = pack8(&err, p, e, Kent);
		p = pack64(&err, p, e, up);
		p = packstr(&err, p, e, m->wname[i]);
		if(err){
			rerror(m, "bad walk: %r");
			return;
		}
		k.k = kbuf;
		k.nk = p - kbuf;
//showfs("walking");
//dprint("looking up %K\n", &k);
		if((estr = fslookup(o, &k, &kv, &b, 0)) != nil){
			break;
		}
		if(kv2dir(&kv, &d) == -1){
			rerror(m, Efs);
			putblk(b);
			return;
		}
		nwalk = i;
		prev = d.qid.path;
		putblk(b);
		r.wqid[r.nwqid] = d.qid;
		r.nwqid++;
	}
	if(i == 0 && m->nwname != 0){
		rerror(m, estr);
		return;
	}
	f = o;
	if(m->fid != m->newfid && i == m->nwname){
		if((f = dupfid(m->newfid, o)) == nil){
			rerror(m, "%r");
			return;
		}
	}
	if(i > 0){
		d.name = m->wname[nwalk];
		d.qid = m->wqid[nwalk];
		dent = getdent(f->root.bp.addr, up, &d);
		if(dent == nil){
			if(m->fid != m->newfid)
				clunkfid(f);
			rerror(m, Enomem);
			return;
		}
		if(i == m->nwname){
			f->qpath = r.wqid[i-1].path;
			f->dent = dent;
		}
	}
	respond(m, &r);
}

void
fsstat(Fmsg *m)
{
	char *err, buf[STATMAX];
	Fcall r;
	Fid *f;
	Kvp kv;
	Blk *b;
	int n;

	if((f = getfid(m->fid)) == nil){
		rerror(m, "no such fid");
		return;
	}
	print("stat %K\n", &f->dent->Key);
	if((err = btlookup(&fs->root, f->dent, &kv, &b)) != nil){
		rerror(m, err);
		return;
	}
	if((n = kv2statbuf(&kv, buf, sizeof(buf))) == -1){
		rerror(m, "stat: %r");
		return;
	}
	r.type = Rstat;
	r.stat = (uchar*)buf;
	r.nstat = n;
	respond(m, &r);
	putblk(b);
}

void
fswstat(Fmsg *m)
{
	USED(m);
	rerror(m, "wstat unimplemented");
}

void
fsclunk(Fmsg *m)
{
	Fcall r;
	Fid *f;

	if((f = getfid(m->fid)) == nil){
		rerror(m, "no such fid");
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
}

void
fscreate(Fmsg *m)
{
	char buf[Kvmax];
	Dent *dent;
	Fcall r;
	Msg mb;
	Fid *f;
	Dir d;

	if(okname(m->name) == -1){
		rerror(m, Ename);
		return;
	}
	if((f = getfid(m->fid)) == nil){
		rerror(m, "no such fid");
		return;
	}
	if(m->perm & (DMMOUNT|DMAUTH)){
		rerror(m, "unknown permission");
		return;
	}
	d.qid.type = 0;
	if(m->perm & DMDIR)
		d.qid.type |= QTDIR;
	if(m->perm & DMAPPEND)
		d.qid.type |= QTAPPEND;
	if(m->perm & DMEXCL)
		d.qid.type |= QTEXCL;
	if(m->perm & DMTMP)
		d.qid.type |= QTTMP;
	d.qid.path = nextqid();
	d.qid.vers = 0;
	d.mode = m->perm;
	d.name = m->name;
	d.atime = nsec();
	d.mtime = d.atime;
	d.length = 0;
	d.uid = "glenda";
	d.gid = "glenda";
	d.muid = "glenda";
	mb.op = Oinsert;
	if(dir2kv(f->qpath, &d, &mb, buf, sizeof(buf)) == -1){
		rerror(m, "%r");
		return;
	}
	if(btupsert(&fs->root, &mb, 1) == -1){
		rerror(m, "%r");
		return;
	}
	dent = getdent(f->root.bp.addr, f->qpath, &d);
	if(dent == nil){
		if(m->fid != m->newfid)
			clunkfid(f);
		rerror(m, Enomem);
		return;
	}

	lock(f);
	if(f->mode != -1){
		unlock(f);
		clunkdent(dent);
		rerror(m, Einuse);
		return;
	}
	f->mode = m->mode;
	f->qpath = d.qid.path;
	f->dent = dent;
	wlock(f->dent);
//	freeb(dent, 0, dent->length);
	dent->length = 0;
	wunlock(f->dent);
	unlock(f);

	r.type = Rcreate;
	r.qid = d.qid;
	r.iounit = f->iounit;
	respond(m, &r);
}

void
fsremove(Fmsg *m)
{
	Fcall r;
	Msg mb;
	Fid *f;

	if((f = getfid(m->fid)) == nil){
		rerror(m, "no such fid");
		return;
	}

	rlock(f->dent);
	mb.op = Odelete;
	mb.k = f->dent->k;
	mb.nk = f->dent->nk;
	mb.nv = 0;
//showfs("preremove");
	if(btupsert(&fs->root, &mb, 1) == -1){
		runlock(f->dent);
		rerror(m, "remove: %r");
		return;
	}
	runlock(f->dent);
	clunkfid(f);

	r.type = Rremove;
	respond(m, &r);
}

int
fsaccess(Dir*, int)
{
	/* all is permitted */
	return 0;
}

void
fsopen(Fmsg *m)
{
	Fcall r;
	char *e;
	Dir d;
	Fid *f;
	Blk *b;
	Kvp kv;

	if((f = getfid(m->fid)) == nil){
		rerror(m, Efid);
		return;
	}
	if((e = fslookup(f, f->dent, &kv, &b, 0)) != nil){
		rerror(m, e);
		return;
	}
	if(kv2dir(&kv, &d) == -1){
		rerror(m, Efs);
		putblk(b);
	}
	if(fsaccess(&d, m->mode) == -1){
		rerror(m, Eperm);
		putblk(b);
		return;
	}
	wlock(f->dent);
	f->dent->length = d.length;
	wunlock(f->dent);
	r.type = Ropen;
	r.qid = d.qid;
	r.iounit = f->iounit;
	putblk(b);

	lock(f);
	if(f->mode != -1){
		rerror(m, Einuse);
		unlock(f);
		return;
	}
	f->mode = m->mode;
	if((f->mode & 0x7) == OEXEC){
		lock(&fs->root.lk);
		f->root = fs->root;
//		refblk(fs->root.bp);
		unlock(&fs->root.lk);
	}
	if(f->mode & OTRUNC){
		wlock(f->dent);
//		freeb(f->dent, 0, dent->length);
		f->dent->length = 0;
		wunlock(f->dent);
	}
	unlock(f);
	respond(m, &r);
}

char*
fsreaddir(Fmsg *m, Fid *f, Fcall *r)
{
	char pfx[9], *p, *e;
	int n, ns, done;
	Tree *t;
	Scan *s;

	s = f->scan;
	if(s != nil && s->offset != 0 && s->offset != m->offset)
		return Edscan;
	if(s == nil || m->offset == 0){
		print("scan starting\n");
		if((s = mallocz(sizeof(Scan), 1)) == nil)
			return Enomem;

		pfx[0] = Kent;
		PBIT64(pfx+1, f->qpath);
		t = (f->root.bp.addr != -1) ? &f->root : &fs->root;
		if((e = btscan(t, s, pfx, sizeof(pfx))) != nil){
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
		fprint(2, "early done...\n");
		r->count = 0;
		return nil;
	}
	p = r->data;
	n = m->count;
	if(s->overflow){
		if((ns = convD2M(&s->dir, (uchar*)p, n)) <= BIT16SZ)
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
		if((ns = convD2M(&s->dir, (uchar*)p, n)) <= BIT16SZ){
			s->overflow = 1;
			break;
		}
		p += ns;
		n -= ns;
	}
	r->count = p - r->data;
	return nil;
}

int
readb(Fid *f, char *d, vlong o, vlong n, int sz)
{
	char *e, buf[17];
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

	e = fslookup(f, &k, &kv, &b, 0);
	if(e != nil && e != Eexist){
		fprint(2, "!!! error: %s", e);
		werrstr(e);
		return -1;
	}
	fprint(2, "\treadb: key=%K, val=%P\n", &k, &kv);
	bp = unpackbp(kv.v);
	putblk(b);

	if((b = getblk(bp, GBraw)) == nil)
		return -1;
	if(fo+n > Blksz)
		n = Blksz-fo;
	if(b != nil){
		fprint(2, "\tcopying %lld to resp %p\n", n, d);
		memcpy(d, b->buf+fo, n);
		putblk(b);
	}else
		memset(d, 0, n);
	return n;
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
			fprint(2, "read: %r\n");
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
	fprint(2, "\n");
	r.type = Rread;
	r.count = 0;
	if((r.data = malloc(m->count)) == nil){
		rerror(m, Emem);
		return;
	}
	fprint(2, "\nread{{{{\n");
	if(f->dent->qid.type & QTDIR)
		e = fsreaddir(m, f, &r);
	else
		e = fsreadfile(m, f, &r);
	if(e != nil){
		rerror(m, e);
		return;
	}else{
		respond(m, &r);
		free(r.data);
	}
	fprint(2, "\n}}}read\n");
}

int
writeb(Fid *f, Msg *m, char *s, vlong o, vlong n, vlong sz)
{
	vlong fb, fo;
	Bptr bp;
	Blk *b, *t;
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
		fprint(2, "\tappending to block %B\n", b->bp);
		if(fslookup(f, m, &kv, &t, 0) != nil){
			putblk(b);
			return -1;
		}
		bp = unpackbp(kv.v);
		putblk(t);

		if((t = getblk(bp, GBraw)) == nil){
			putblk(b);
			return -1;
		}
		memcpy(b->buf, t->buf, Blksz);
		freeblk(t);
		putblk(t);
	}
	if(fo+n > Blksz)
		n = Blksz-fo;
	memcpy(b->buf+fo, s, n);
	enqueue(b);

	bp.gen = fs->nextgen;
	assert(b->flag & Bfinal);
	packbp(m->v, &b->bp);
	putblk(b);
	checkfs();
	poolcheck(mainmem);
	return n;
}

void
fswrite(Fmsg *m)
{
	char sbuf[8], offbuf[4][Ptrsz+Offksz], *p;
	vlong n, o, c;
	Msg kv[4];
	Fcall r;
	Fid *f;
	int i;

	if((f = getfid(m->fid)) == nil){
		rerror(m, Efid);
		return;
	}
	if((f->mode&0x7) != OWRITE){
		dprint("f->mode: %x\n", f->mode);
		rerror(m, Einuse);
		return;
	}

	wlock(f->dent);
	p = m->data;
	o = m->offset;
	c = m->count;
	for(i = 0; i < nelem(kv)-1 && c != 0; i++){
		kv[i].op = Oinsert;
		kv[i].k = offbuf[i];
		kv[i].nk = Offksz;
		kv[i].v = offbuf[i]+Offksz;
		kv[i].nv = 16;
		n = writeb(f, &kv[i], p, o, c, f->dent->length);
		if(n == -1){
			// badwrite(f, i);
			// FIXME: free pages
			wunlock(f->dent);
			rerror(m, "%r");
			return;
		}
		p += n;
		o += n;
		c -= n;
	}

	kv[i].op = Owstat;
	kv[i].k = f->dent->k;
	kv[i].nk = f->dent->nk;
	kv[i].v = sbuf;
	kv[i].nv = 0;
	if(m->offset+m->count > f->dent->length){
		kv[i].op |= Owsize;
		kv[i].nv += 8;
		PBIT64(kv[i].v, m->offset+m->count);
		f->dent->length = m->offset+m->count;
	}
	if(btupsert(&fs->root, kv, i+1) == -1){
		fprint(2, "upsert: %r\n");
		abort();
	}
	wunlock(f->dent);

	r.type = Rwrite;
	r.count = m->count;
	respond(m, &r);
}

void
runfs(void *pfd)
{
	int fd, msgmax;
	char err[128];
	QLock *wrlk;
	Fcall r;
	Fmsg *m;

	fd = (uintptr)pfd;
	msgmax = Max9p;
	if((wrlk = mallocz(sizeof(QLock), 1)) == nil)
		fshangup(fd, "alloc wrlk: %r");
	while(1){
		if((m = readmsg(fd, msgmax)) == nil){
			fshangup(fd, "truncated message: %r");
			return;
		}
		if(convM2S(m->buf, m->sz, m) == 0){
			fshangup(fd, "invalid message: %r");
			return;
		}
/*
		if(m->type != Tversion && !versioned){
			fshangup(fd, "version required");
			return;
		}
*/
		m->wrlk = wrlk;
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
	}
}

void
runwrite(void *)
{
	Fmsg *m;

	while(1){
		m = chrecv(fs->wrchan);
		switch(m->type){
		case Tflush:	rerror(m, "unimplemented flush");	break;
		case Tcreate:	fscreate(m);	break;
		case Twrite:	fswrite(m);	break;
		case Twstat:	fswstat(m);	break;
		case Tremove:	fsremove(m);	break;
		}
	}
}

void
runread(void *)
{
	Fmsg *m;

	while(1){
		m = chrecv(fs->rdchan);
		switch(m->type){
		case Twalk:	fswalk(m);	break;
		case Tread:	fsread(m);	break;
		case Tstat:	fsstat(m);	break;
		}
	}
}
