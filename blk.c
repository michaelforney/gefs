#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>
#include <pool.h>

#include "dat.h"
#include "fns.h"

typedef struct Range Range;
struct Range {
	vlong off;
	vlong len;
};

static vlong	blkalloc_lk(Arena*);
static int	blkdealloc(vlong);
static void	cachedel(vlong);
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

Arena*
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
	Arange *r, *s;

	assert(len % Blksz == 0);
	if((r = calloc(1, sizeof(Arange))) == nil)
		return -1;
	r->off = off;
	r->len = len;
	avlinsert(t, r);

Again:

	s = (Arange*)avlprev(r);
	if(s != nil && s->off+s->len == r->off){
		avldelete(t, r);
		s->len = s->len + r->len;
		free(r);
		r = s;
		goto Again;
	}
	s = (Arange*)avlnext(r);
	if(s != nil && r->off+r->len == s->off){
		avldelete(t, r);
		s->off = r->off;
		s->len = s->len + r->len;
		free(r);
		r = s;
		goto Again;
	}
	return 0;
}

int
grabrange(Avltree *t, vlong off, vlong len)
{
	Arange *r, *s, q;
	vlong l;

	assert(len % Blksz == 0);
	q.off = off;
	q.len = len;
	r = (Arange*)avllookup(t, &q.Avl, -1);
	if(r == nil || off + len > r->off + r->len)
		abort();

	if(off == r->off){
		r->off += len;
		r->len -= len;
	}else if(off + len == r->off + r->len){
		r->len -= len;
	}else if(off > r->off && off+len < r->off + r->len){
		if((s = malloc(sizeof(Arange))) == nil)
			return -1;
		l = r->len;
		s->off = off + len;
		r->len = off - r->off;
		s->len = l - r->len - len;
		avlinsert(t, s);
	}else
		abort();

	if(r->len == 0){
		avldelete(t, r);
		free(r);
	}
	return 0;
}

Blk*
logappend(Arena *a, Blk *lb, vlong off, vlong len, int op)
{
	Blk *pb;
	vlong o;
	char *p;

	assert(off % Blksz == 0);
	assert(op == LogAlloc || op == LogFree);
	if(lb == nil || lb->logsz > Logspc - 8){
		pb = lb;
		if((o = blkalloc_lk(a)) == -1)
			return nil;
		if((lb = mallocz(sizeof(Blk), 1)) == nil)
			return nil;
		lb->data = lb->buf + Hdrsz;
		lb->flag |= Bdirty;
		lb->type = Tlog;
		lb->off = o;
		lb->logsz = Loghdsz;
		p = lb->data + lb->logsz;
		PBIT64(p + 0, (uvlong)LogEnd);
		finalize(lb);
		if(syncblk(lb) == -1){
			free(lb);
			return nil;
		}

		a->logtl = lb;
		if(pb != nil){
			p = pb->data + pb->logsz;
			PBIT64(p + 0, lb->off|LogChain);
			finalize(pb);
			if(syncblk(pb) == -1)
				return nil;
		}
	}

	p = lb->data + lb->logsz;
	if(len == Blksz){
		off |= (op & ~Log2w);
		PBIT64(p, off);
		lb->logsz += 8;
	}else{
		off |= op;
		PBIT64(p+0, off);
		PBIT64(p+8, len);
		lb->logsz += 16;
	}
	/* this gets overwritten by the next append */
	p = lb->data + lb->logsz;
	PBIT64(p, (uvlong)LogEnd);
	return lb;
}

/*
 * Logs an allocation. Must be called
 * with arena lock held. Duplicates some/c
 * of the work in allocblk to prevent
 * recursion.
 */
int
logop(Arena *a, vlong off, int op)
{
	Blk *b;

	if((b = logappend(a, a->logtl, off, Blksz, op)) == nil)
		return -1;
	if(a->log == -1)
		a->log = b->off;
	if(b != a->logtl)
		a->logtl = b;
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

Nextblk:
	if((b = readblk(bp, 0)) == nil)
		return -1;
	p = b->data;
	bh = GBIT64(p + 0);
	/* the hash covers the log and offset */
	if(bh != siphash(p+8, Blkspc-8)){
		werrstr("corrupt log");
		return -1;
	}
	for(i = Loghdsz; i < Logspc; i += n){
		d = b->data + i;
		ent = GBIT64(d);
		op = ent & 0xff;
		off = ent & ~0xff;
		n = (op & Log2w) ? 16 : 8;
		switch(op){
		case LogEnd:
			dprint("log@%d: end\n", i);
			/*
			 * since we want the next insertion to overwrite
			 * this, don't include the size in this entry.
			 */
			b->logsz = i;
			return 0;
		case LogChain:
			bp = off & ~0xff;
			dprint("log@%d: chain %llx\n", i, bp);
			b->logsz = i+n;
			goto Nextblk;
			break;

		case LogFlush:
			dprint("log@%d: flush: %llx\n", i, off>>8);
			lock(&fs->genlk);
			fs->gen = off >> 8;
			unlock(&fs->genlk);
			break;
		case LogAlloc:
		case LogAlloc1:
			len = (op & Log2w) ? GBIT64(d+8) : Blksz;
			dprint("log@%d alloc: %llx+%llx\n", i, off, len);
			if(grabrange(a->free, off & ~0xff, len) == -1)
				return -1;
			break;
		case LogFree:
		case LogFree1:
			len = (op & Log2w) ? GBIT64(d+8) : Blksz;
			dprint("log@%d free: %llx+%llx\n", i, off, len);
			if(freerange(a->free, off & ~0xff, len) == -1)
				return -1;
			break;
		default:
			n = 0;
			dprint("log@%d: log op %d\n", i, op);
			abort();
			break;
		}
showfree("after");
	}
	return -1;
}

int
compresslog(Arena *a)
{
	Arange *r;
	Range *log, *nlog;
	vlong v, bp, nb, graft, oldhd;
	int i, n, sz;
	Blk *hd, *ab, *b;
	char *p;

	/*
	 * Sync the current log to disk, and
	 * set up a new block log tail.  While
	 * compressing the log, nothing else is
	 * using this arena, so any allocs come
	 * from the log compression, and go into
	 * this new log segment.
	 *
	 * A bit of copy paste from newblk,
	 * because otherwise we have a deadlock
	 * allocating the block.
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
	b->logsz = Loghdsz;

	PBIT64(b->data+b->logsz, (uvlong)LogEnd);
	finalize(b);
	if(syncblk(b) == -1){
		free(b);
		return -1;
	}

	graft = b->off;
	if(a->logtl != nil){
		finalize(a->logtl);
		if(syncblk(a->logtl) == -1){
			free(b);
			return -1;
		}
	}
	a->logtl = b;

	/*
	 * Prepare what we're writing back.
	 * Arenas must be sized so that we can
	 * keep the merged log in memory for
	 * a rewrite.
	 */
	n = 0;
	sz = 512;
	if((log = malloc(sz*sizeof(Range))) == nil)
		return -1;
	for(r = (Arange*)avlmin(a->free); r != nil; r = (Arange*)avlnext(r)){
		if(n == sz){
			sz *= 2;
			if((nlog = realloc(log, sz*sizeof(Range))) == nil){
				free(log);
				return -1;
			}
			log = nlog;
		}
		log[n].off = r->off;
		log[n].len = r->len;
		n++;
	}
	if((b = newblk(Tlog)) == nil){
		free(log);
		return -1;
	}
	hd = b;
	b->logsz = Loghdsz;
	for(i = 0; i < n; i++)
		if((b = logappend(a, b, log[i].off, log[i].len, LogFree)) == nil)
			return -1;
	p = b->data + b->logsz;
	PBIT64(p, LogChain|graft);
	free(log);
	finalize(b);
	if(syncblk(b) == -1)
		return -1;

	oldhd = a->log;
	a->log = hd->off;
	a->logh = blkhash(hd);
	ab = a->b;
	PBIT64(ab->data + 0, a->log);
	PBIT64(ab->data + 8, a->logh);
	finalize(ab);
	if(syncblk(ab) == -1)
		return -1;
	if(oldhd != -1){
		for(bp = oldhd; bp != -1; bp = nb){
			nb = -1;
			if((b = readblk(bp, 0)) == nil)
				return -1;
			for(i = Loghdsz; i < Logspc; i += n){
				p = b->data + i;
				v = GBIT64(p);
				n = (v & Log2w) ? 16 : 8;
				if((v&0xff) == LogChain){
					nb = v & ~0xff;
					break;
				}else if((v&0xff) == LogEnd){
					nb = -1;
					break;
				}
			}
			if(blkdealloc(bp) == -1)
				return -1;
		}
	}
	finalize(a->logtl);
	if(syncblk(a->logtl) == -1)
		return -1;
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

static int
blkdealloc(vlong b)
{
	Arena *a;
	int r;

	r = -1;
	a = getarena(b);
	lock(a);
	cachedel(b);
	if(freerange(a->free, b, Blksz) == -1)
		goto out;
	if(logop(a, b, LogFree) == -1)
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
Again:
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
		goto Again;
	}
	if(logop(a, b, LogAlloc) == -1){
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
	if((b = lookupblk(bp)) == nil)
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
cachedel(vlong del)
{
	Bucket *bkt;
	Blk *b, **p;
	u32int h;

	/* FIXME: better hash. */
	h = ihash(del);

	bkt = &fs->cache[h % fs->cmax];
	lock(bkt);
	p = &bkt->b;
	for(b = bkt->b; b != nil; b = b->hnext){
		if(b->off == del){
			*p = b->hnext;
			break;
		}
		p = &b->hnext;
	}
	unlock(bkt);
	if(b == nil)
		return;

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

int
syncblk(Blk *b)
{
	assert(b->flag & Bfinal);
	wlock(b);
	b->flag &= ~(Bqueued|Bdirty);
	wunlock(b);
	return pwrite(fs->fd, b->buf, Blksz, b->off);
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

void
fillsuper(Blk *b)
{
	char *p;

	assert(b->type == Tsuper);
	p = b->data;
	wlock(b);
	b->flag |= Bdirty;
	wunlock(b);
	memcpy(p +  0, "gefs0001", 8);
	PBIT32(p +  8, 0); /* dirty */
	PBIT32(p + 12, Blksz);
	PBIT32(p + 16, Bufspc);
	PBIT32(p + 20, Hdrsz);
	PBIT32(p + 24, fs->root.ht);
	PBIT64(p + 32, fs->root.bp);
	PBIT64(p + 40, fs->root.bh);
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
	if(b->type != Traw)
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
getblk(vlong bp, uvlong bh, int flg)
{
	Blk *b;

	if((b = lookupblk(bp)) == nil){
		if((b = readblk(bp, flg)) == nil)
			return nil;
		if(blkhash(b) != bh){
			werrstr("corrupt block %llx: %llx != %llx", bp, blkhash(b), bh);
			return nil;
		}
	}
	assert(b->off == bp);
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
	if(adec(&b->ref) == 0){
		assert((b->flag & Bqueued) || !(b->flag & Bdirty));
		cachedel(b->off);
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
	logop(a, b->off, LogFree);
	blkdealloc(b->off);
	unlock(a);
	free(b);
}

int
sync(void)
{
	int i, r;
	Blk *b;

	r = 0;
	for(i = 0; i < fs->narena; i++){
		b = fs->arenas[i].logtl;
		finalize(b);
		if(syncblk(b) == -1)
			r = -1;
	}
	/* FIXME: hit it with a big hammer -- flush the whole cache */
	for(b = fs->chead; b != nil; b = b->cnext){
		if(!(b->flag & Bdirty))
			continue;
		if(syncblk(b) == -1)
			r = -1;
	}
	return r;
}
