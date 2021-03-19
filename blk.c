#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"

static vlong	blkalloc_lk(Arena*);
static Blk	*cacheblk(Blk*);
static Blk	*lookupblk(vlong);

Blk*
readblk(vlong bp, int flg)
{
	Blk *b;
	vlong off, rem, n;

	assert(bp != -1);
	if((b = malloc(sizeof(Blk))) == nil)
		return nil;
	off = bp;
	rem = Blksz;
	while(rem != 0){
		n = pread(fs->fd, b->buf, rem, off);
		if(n <= 0){
			free(b);
			return nil;
		}
		off += n;
		rem -= n;
	}
	memset(&b->RWLock, 0, sizeof(RWLock));
	b->type = (flg&GBraw) ? Traw : GBIT16(b->buf+0);
	b->off = bp;
	b->cnext = nil;
	b->cprev = nil;
	b->hnext = nil;
	b->data = b->buf + 10;
	switch(b->type){
	default:
		if(flg&GBraw)
			break;
		fprint(2, "invalid block @%llx\n", bp);
		abort();
		break;
	case Tarena:
	case Traw:
	case Tsuper:
	case Tlog:
		break;
	case Tpivot:
		b->nval = GBIT16(b->buf+2);
		b->valsz = GBIT16(b->buf+4);
		b->nbuf = GBIT16(b->buf+6);
		b->bufsz = GBIT16(b->buf+8);
		break;
	case Tleaf:
		b->nval = GBIT16(b->buf+2);
		b->valsz = GBIT16(b->buf+4);
		break;
	}
	return b;
}

static Arena*
pickarena(vlong hint)
{
	long n;

	n = -1; /* shut up, ken */
	if(hint > 0 || hint < fs->narena)
		n = hint / fs->arenasz;
	else if(hint == -1)
		n = ainc(&fs->nextarena) % fs->narena;
	else
		abort();
	return &fs->arenas[n];
}

static Arena*
getarena(vlong b)
{
	int i;

	i = b / fs->arenasz;
	if(i < 0 || i >= fs->narena){
		werrstr("out of range block %lld", b);
		abort();
//		return nil;
	}
	return &fs->arenas[i];
}

static int
freerange(Avltree *t, vlong off, vlong len)
{
	Arange *r, *s, q;

	assert(len % Blksz == 0);

	q.off = off;
	q.len = len;
	r = (Arange*)avllookup(t, &q.Avl, -1);
	if(r == nil){
		if((r = calloc(1, sizeof(Arange))) == nil)
			return -1;
		r->off = off;
		r->len = len;
		avlinsert(t, r);
	}else if(off+len == r->off){
		r->len += len;
		r->off -= len;
	}else if(off == r->off + r->len){
		r->len += len;
	}else{
		r = (Arange*)avlnext(r);
		if(r != nil && off+len == r->off){
			r->off -= len;
			r->len += len;
		} else {
			if((r = calloc(1, sizeof(Arange))) == nil)
				return -1;
			r->off = off;
			r->len = len;
			avlinsert(t, r);
		}
	}

	/* merge with previous */
	s = (Arange*)avlprev(r);
	if(s != nil && s->off + s->len == r->off){
		s->off += r->len;
		avldelete(t, r);
	}
	/* merge with next */
	s = (Arange*)avlnext(r);
	if(s != nil && r->off + r->len == s->off){
		r->off += r->len;
		avldelete(t, s);
	}
	return 0;
}

int
grabrange(Avltree *t, vlong off, vlong len)
{
	Arange *r, *s, q;

	assert(len % Blksz == 0);
	q.off = off;
	q.len = len;
	r = (Arange*)avllookup(t, &q.Avl, -1);
	if(r == nil || off + len > r->off + r->len){
		fprint(2, "no blocks: %llx+%llx in:\n", off, len);
		for(r = (Arange*)avlmin(t); r != nil; r = (Arange*)avlnext(r))
			fprint(2, "\t%llx+%llx\n", r->off, r->len);
		abort();
		return -1;
	}
	if(off == r->off)
		r->off += len;
	else if(off == r->off + r->len)
		r->len -= len;
	else if(off + len > r->off && off > r->off + r->len){
		if((s = malloc(sizeof(Arange))) == nil)
			return -1;
		s->off = off;
		s->len = r->len - (off - r->off) - len;;
		r->len = off - r->off;
		avlinsert(t, s);
	}
	if(r->len == 0){
		avldelete(t, r);
		free(r);
	}
	return 0;
}

int
synclog(Blk *b, vlong link, vlong sz)
{

	if(sz == -1)
		sz = b->logsz;
	PBIT64(b->data+8, link);
	PBIT64(b->data+16, sz);
	finalize(b);
fprint(2, "synclog: @%llx\n", b->off);
showfree("synclog");
	return pwrite(fs->fd, b->buf, Blksz, b->off);
}

Blk*
logappend(Arena *a, Blk *lb, vlong off, vlong len, int op, vlong graft)
{
	Blk *pb;
	vlong o, n;
	char *p;

	assert(off % Blksz == 0);
	if(lb == nil || lb->logsz+16+24 > Blkspc){
		pb = lb;
		if((o = blkalloc_lk(a)) == -1)
			return nil;
		if((lb = mallocz(sizeof(Blk), 1)) == nil)
			return nil;
		lb->data = lb->buf + Hdrsz;
		lb->flag |= Bdirty;
		lb->type = Tlog;
		lb->off = o;
		lb->logsz = 0;
		if(synclog(lb, graft, 0) == -1)
			return nil;

		a->logtl = lb;
		if(pb != nil)
			if(synclog(pb, lb->off, -1) == -1)
				return nil;
	}
	n = 8;
	p = lb->data + 24 + lb->logsz;
	if(len == Blksz && op == LgAlloc)
		op = LgAlloc1;
	off |= op;
	PBIT64(p, off);
	if(op == LgAlloc || op == LgFree){
		PBIT64(p+8, len);
		n += 8;
	}
	lb->logsz += n;
	return lb;
}

/*
 * Logs an allocation. Must be called
 * with arena lock held. Duplicates some/c
 * of the work in allocblk to prevent
 * recursion.
 */
int
logalloc(Arena *a, vlong off, int op)
{
	Blk *b;

	if((b = logappend(a, a->logtl, off, Blksz, op, -1)) == nil)
		return -1;
	if(a->logtl == nil){
		a->log = b->off;
		a->logtl = b;
	}
	return 0;
}

int
loadlog(Arena *a)
{
	Blk *b;
	vlong bp, ent, off, len;
	uvlong bh;
	char *p, *d;
	int op, i, n;

	bp = a->log;
	while(bp != -1){
		dprint("log block: %llx\n", bp);
		if((b = readblk(bp, 0)) == nil)
			return -1;
		p = b->data;
		bh = GBIT64(p + 0);
		bp = GBIT64(p + 8);
		b->logsz = GBIT64(p + 16);
		/* the hash covers the log and offset */
		if(bh != siphash(p+8, Blkspc-8)){
			werrstr("corrupt log");
			return -1;
		}
		for(i = 0; i < b->logsz; i += n){
			d = p + i + 24;
			ent = GBIT64(d);
			op = ent & 0xff;
			off = ent & ~0xff;
			switch(op){
			case LgFlush:
				dprint("log@%d: flush: %llx\n", i, off>>8);
				n = 8;
				lock(&fs->genlk);
				fs->gen = off >> 8;
				unlock(&fs->genlk);
				break;
			case LgAlloc1:
				n = 8;
				dprint("log@%d alloc1: %llx\n", i, off);
				if(grabrange(a->free, off & ~0xff, Blksz) == -1)
					return -1;
				break;
			case LgAlloc:
				n = 16;
				len = GBIT64(d+8);
				dprint("log@%d alloc: %llx+%llx\n", i, off, len);
				if(grabrange(a->free, off & ~0xff, len) == -1)
					return -1;
				break;
			case LgFree:
				n = 16;
				len = GBIT64(d+8);
				dprint("log@%d free: %llx+%llx\n", i, off, len);
				if(freerange(a->free, off & ~0xff, len) == -1)
					return -1;
				break;
			case LgRef1:
			case LgUnref1:
				n = 8;
				fprint(2, "unimplemented ref op at log@%d: log op %d\n", i, op);
				break;
			default:
				n = 0;
				dprint("log@%d: log op %d\n", i, op);
				abort();
				break;
			}
		}
	}
	return 0;
}

int
compresslog(Arena *a)
{
	Arange *r;
	vlong *log, *p, bp, hd, hh, graft;
	int i, n, sz;
	Blk *pb, *ab, *b;

showfree("precompress");
fprint(2, "compress start\n");
	/*
	 * Sync the current log to disk. While
	 * compressing the log, nothing else is
	 * using this arena, so any allocs come
	 * from the log compression.
	 *
	 * A bit of copy paste from newblk,
	 * because otherwise we have bootstrap
	 * issues.
	 *
	 * Set up the head of the new log as
	 * an empty block.
	 */
	if((bp = blkalloc_lk(a)) == -1)
		return -1;
	if((b = mallocz(sizeof(Blk), 1)) == nil)
		return -1;
	b->type = Tlog;
	b->flag = Bdirty;
	b->off = bp;
	b->ref = 1;
	b->data = b->buf + Hdrsz;
checkfs();
	if(synclog(b, -1, 0) == -1)
		return -1;
checkfs();

	/*
	 * When reaming or loading, we may not
	 * have set up the log.
	 */
checkfs();
	if(a->logtl != nil)
		synclog(a->logtl, b->off, -1);
checkfs();
	graft = b->off;
	a->logtl = b;

	/*
	 * Prepare what we're writing back.
	 * Arenas must be sized so that we can
	 * keep the merged log in memory for
	 * a rewrite.
	 */
	n = 0;
	sz = 512;
checkfs();
	if((log = malloc(2*sz*sizeof(vlong))) == nil)
		return -1;
checkfs();
	for(r = (Arange*)avlmin(a->free); r != nil; r = (Arange*)avlnext(r)){
		if(n == sz){
			sz *= 2;
			if((p = realloc(log, 2*sz*sizeof(vlong))) == nil){
				free(log);
				return -1;
			}
			log = p;
		}
		log[2*n+0] = r->off;
		log[2*n+1] = r->len;
		n++;
	}
	if((b = newblk(Tlog)) == nil){
		free(log);
		return -1;
	}
checkfs();
	pb = b;
	hd = b->off;
	hh = -1;
	PBIT64(b->data +  8, graft);	/* link */
	PBIT64(b->data + 16, 0ULL);	/* count */
checkfs();

	for(i = 0; i < n; i++){
		if((b = logappend(a, b, log[2*i], log[2*i+1], LgFree, graft)) == nil)
			return -1;
checkfs();
		if(b != pb){
checkfs();
			synclog(pb, b->off, -1);
checkfs();
			if(pb->off == hd)
				hh = blkhash(pb);
			b = pb;
		}
	}
	free(log);
checkfs();
	if(synclog(b, graft, -1) == -1)
		return -1;
checkfs();
	if(pb->off == hd)
		hh = blkhash(pb);
checkfs();

	a->log = hd;
	a->logh = hh;
	ab = a->b;
	PBIT64(ab->data + 0, hd);
	PBIT64(ab->data + 8, hh);
checkfs();
	pwrite(fs->fd, ab->buf, Blksz, ab->off);
checkfs();
fprint(2, "compress done\n");
	return 0;
}
/*
 * Allocate from an arena, with lock
 * held. May be called recursively, to
 * alloc space for the alloc log.
 */
static vlong
blkalloc_lk(Arena *a)
{
	Avltree *t;
	Arange *r;
	vlong b;

	t = a->free;
	r = (Arange*)t->root;
	if(r == nil){
		unlock(a);
		return -1;
	}

	/*
	 * A bit of sleight of hand here:
	 * while we're changing the sorting
	 * key, but we know it won't change
	 * the sort order because the tree
	 * covers disjoint ranges
	 */
	b = r->off;
	r->len -= Blksz;
	r->off += Blksz;
	if(r->len == 0){
		avldelete(t, r);
		free(r);
	}
	return b;
}

int
blkrelease(vlong b)
{
	Arena *a;
	int r;

	r = -1;
	a = getarena(b);
	lock(a);
	if(freerange(a->free, b, Blksz) == -1)
		goto out;
	if(logalloc(a, b, LgFree) == -1)
		goto out;
	r = 0;
out:
	unlock(a);
	return r;
}

vlong
blkalloc(vlong hint)
{
	Arena *a;
	vlong b;
	int tries;

	tries = 0;
again:
	a = pickarena(hint);
	if(a == nil || tries == fs->narena){
		werrstr("no empty arenas");
		return -1;
	}
	lock(a);
	/*
	 * TODO: there's an extreme edge case
	 * here.
	 *
	 * If the file system has room to alloc
	 * a data block but no log block, then
	 * we end up with it in a stuck state.
	 * The fix is to reserve alloc blocks,
	 * so that we're guaranteed to be able
	 * to log an alloc if the disk is working
	 * correctly.
	 */
	tries++;
	if((b = blkalloc_lk(a)) == -1){
		unlock(a);
		goto again;
	}
	if(logalloc(a, b, LgAlloc) == -1){
		unlock(a);
		return -1;
	}
	unlock(a);
	return b;
}

Blk*
newblk(int t)
{
	Blk *b;
	vlong bp;

	if((bp = blkalloc(-1)) == -1)
		return nil;
	if((b = mallocz(sizeof(Blk), 1)) == nil)
		return nil;
	b->type = t;
	b->flag = Bdirty;
	b->off = bp;
	b->ref = 1;
	b->data = b->buf + Hdrsz;
	return cacheblk(b);
}

static Blk*
lookupblk(vlong off)
{
	Bucket *bkt;
	u32int h;
	Blk *b;

	h = ihash(off);

	bkt = &fs->cache[h % fs->cmax];
	lock(bkt);
	for(b = bkt->b; b != nil; b = b->hnext)
		if(b->off == off)
			break;
	if(b != nil)
		pinblk(b);
	unlock(bkt);
	return b;
}

static Blk*
cacheblk(Blk *b)
{
	Bucket *bkt;
	Blk *e, *c;
	u32int h;

	/* FIXME: better hash. */
	assert(b->off != 0);
	h = ihash(b->off);
//	dprint("cache %lld (h=%xm, bkt=%d) => %p\n", b->off, h%fs->cmax, h, b);
	ainc(&b->ref);
	bkt = &fs->cache[h % fs->cmax];
	lock(bkt);
	for(e = bkt->b; e != nil; e = e->hnext){
		if(b == e)
			goto found;
		assert(b->off != e->off);
	}
	bkt->b = b;
found:
	ainc(&b->ref);
	unlock(bkt);

	lock(&fs->lrulk);
	if(b == fs->chead)
		goto Cached;
	if(b == fs->ctail)
		fs->ctail = b->cprev;

	if(b->cnext != nil)
		b->cnext->cprev = b->cprev;
	if(b->cprev != nil)
		b->cprev->cnext = b->cnext;
	if(fs->ctail == nil)
		fs->ctail = b;
	if(fs->chead != nil)
		fs->chead->cprev = b;
	if(fs->ctail == nil)
		fs->ctail = b;
	b->cnext = fs->chead;
	b->cprev = nil;
	fs->chead = b;
	if((b->flag&Bcache) == 0){
		b->flag |= Bcache;
		fs->ccount++;
		ainc(&b->ref);
	}
	c=0;
	USED(c);
/*
	for(c = fs->ctail; c != nil && fs->ccount >= fs->cmax; c = fs->ctail){
		fs->ctail = c->cprev;
		fs->ccount--; 
		putblk(c);
	}
*/
Cached:
	unlock(&fs->lrulk);
	return b;
}

static void
cachedel(Blk *del)
{
	Bucket *bkt;
	Blk *b, **p;
	u32int h;

	/* FIXME: better hash. */
	h = ihash(del->off);

	bkt = &fs->cache[h % fs->cmax];
	lock(bkt);
	p = &bkt->b;
	for(b = bkt->b; b != nil; b = b->hnext){
		if(b == del){
			*p = del->hnext;
			break;
		}
		p = &b->hnext;
	}
	unlock(bkt);

	lock(&fs->lrulk);
	if(b->cnext != nil)
		b->cnext->cprev = b->cprev;
	if(b->cprev != nil)
		b->cprev->cnext = b->cnext;
	if(fs->ctail == b)
		fs->ctail = b->cprev;
	if(fs->chead == nil)
		fs->chead = b;
	unlock(&fs->lrulk);
}

void
enqueue(Blk *b)
{
	if(pwrite(fs->fd, b->buf, Blksz, b->off) == -1){
		ainc(&fs->broken);
		fprint(2, "write: %r");
		return;
	}
	wlock(b);
	b->flag &= ~(Bqueued|Bdirty|Bfinal);
	wunlock(b);

}

void
fillsuper(Blk *b)
{
	char *p;

	assert(b->type == Tsuper);
	p = b->data;
	memcpy(p +  0, "gefs0001", 8);
	PBIT32(p +  8, 0); /* dirty */
	PBIT32(p + 12, Blksz);
	PBIT32(p + 16, Bufspc);
	PBIT32(p + 20, Hdrsz);
	PBIT32(p + 24, fs->height);
	PBIT64(p + 32, fs->rootb);
	PBIT64(p + 40, fs->rooth);
	PBIT32(p + 48, fs->narena);
	PBIT64(p + 56, fs->arenasz);
	PBIT64(p + 64, fs->gen);
	PBIT64(p + 72, fs->nextqid);
}

void
finalize(Blk *b)
{
	vlong h;

//	assert((b->flag & Bfinal) == 0);
	b->flag |= Bfinal;
	PBIT16(b->buf, b->type);
	switch(b->type){
	default:
	case Tnone:
		abort();
		break;
	case Tpivot:
		PBIT16(b->buf+2, b->nval);
		PBIT16(b->buf+4, b->valsz);
		PBIT16(b->buf+6, b->nbuf);
		PBIT16(b->buf+8, b->bufsz);
		break;
	case Tleaf:
		PBIT16(b->buf+2, b->nval);
		PBIT16(b->buf+4, b->valsz);
		break;
	case Tlog:
		h = siphash(b->data + 8, Blkspc-8);
		PBIT64(b->data, h);
	case Tsuper:
	case Tarena:
	case Traw:
		break;
	}
}

Blk*
getblk(vlong bp, uvlong bh)
{
	Blk *b;

	if((b = lookupblk(bp)) == nil){
		if((b = readblk(bp, 0)) == nil)
			return nil;
		if(siphash(b->buf, Blksz) != bh){
			werrstr("corrupt block %llx", bp);
			return nil;
		}
	}
	return cacheblk(b);
}

Blk*
dupblk(vlong bp, uvlong bh)
{
	USED(bp, bh);
	return nil;
}

Blk*
pinblk(Blk *b)
{
	ainc(&b->ref);
	return b;
}

int
refblk(Blk *b)
{
	Arena *a;
	int r;

	a = getarena(b->off);
	lock(a);
	r = logalloc(a, b->off, LgRef1);
	unlock(a);
	return r;
}

int
unrefblk(Blk *b)
{
	Arena *a;
	int r;

	a = getarena(b->off);
	lock(a);
	r = logalloc(a, b->off, LgUnref1);
	unlock(a);
	return r;
}

ushort
blkfill(Blk *b)
{
	switch(b->type){
	case Tpivot:
		return 2*b->nbuf + b->bufsz +  2*b->nval + b->valsz;
	case Tleaf:
		return 2*b->nval + b->valsz;
	default:
		fprint(2, "invalid block @%lld\n", b->off);
		abort();
	}
	return 0; // shut up kencc
}

void
putblk(Blk *b)
{
	if(b == nil)
		return;
	if((b->flag & (Bdirty|Bqueued)) == Bdirty)
		enqueue(b);
	if(adec(&b->ref) == 0){
		cachedel(b);
		free(b);
	}
}

void
freeblk(Blk *b)
{
	Arena *a;

	assert(b->ref == 1 && b->flag & (Bdirty|Bqueued) == Bdirty);
	a = getarena(b->off);
	lock(a);
	/*
	 * TODO: what to do if we fail to log a free here??
	 * This is already an error path!
	 */
	logalloc(a, b->off, LgRef1);
	unlock(a);
	free(b);
}

int
sync(void)
{
	int i, r;
	Blk *b;

	dprint("syncing\n");
	r = 0;
	for(i = 0; i < fs->narena; i++)
		if(synclog(fs->arenas[i].logtl, -1, -1) == -1)
			r = -1;
	for(b = fs->chead; b != nil; b = b->cnext){
//		dprint("sync %p\n", b);
		if(!(b->flag & Bdirty))
			continue;
		if(pwrite(fs->fd, b->buf, Blksz, b->off) == -1)
			r = -1;
	}
	return r;
}
