#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"
#include "atomic.h"

typedef struct Range	Range;

struct Range {
	vlong off;
	vlong len;
};

static vlong	blkalloc_lk(Arena*, int);
static vlong	blkalloc(int);
static int	blkdealloc_lk(vlong);
static Blk*	initblk(Blk*, vlong, int);
static int	logop(Arena *, vlong, vlong, int);

int
checkflag(Blk *b, int f)
{
	return (b->flag & f) == f;
}

void
setflag(Blk *b, int f)
{
	long ov, nv;

	while(1){
		ov = b->flag;
		nv = ov | f;
		if(acasl(&b->flag, ov, nv))
			break;
	}
}

void
clrflag(Blk *b, int f)
{
	long ov, nv;

	while(1){
		ov = b->flag;
		nv = ov & ~f;
		if(acasl(&b->flag, ov, nv))
			break;
	}
}

int
syncblk(Blk *b)
{
	assert(checkflag(b, Bfinal));
	clrflag(b, Bdirty);
	return pwrite(fs->fd, b->buf, Blksz, b->bp.addr);
}

static Blk*
readblk(vlong bp, int flg)
{
	Blk *b;
	vlong off, rem, n;

	assert(bp != -1);
	if((b = cachepluck()) == nil)
		return nil;
	b->alloced = getcallerpc(&bp);
	off = bp;
	rem = Blksz;
	while(rem != 0){
		n = pread(fs->fd, b->buf, rem, off);
		if(n <= 0){
			dropblk(b);
			return nil;
		}
		off += n;
		rem -= n;
	}
	b->cnext = nil;
	b->cprev = nil;
	b->hnext = nil;
	b->flag = 0;

	b->type = (flg&GBraw) ? Traw : UNPACK16(b->buf+0);
	b->bp.addr = bp;
	b->bp.hash = -1;
	b->bp.gen = -1;
	b->fnext = nil;

	b->nval = 0;
	b->valsz = 0;
	b->nbuf = 0;
	b->bufsz = 0;
	b->logsz = 0;
	b->lognxt = 0;

	switch(b->type){
	default:
		fprint(2, "invalid block @%llx\n", bp);
		abort();
		break;
	case Traw:
	case Tarena:
		b->data = b->buf;
		break;
	case Tlog:
	case Tdead:
		b->data = b->buf + Loghdsz;
		break;
		break;
	case Tpivot:
		b->data = b->buf + Pivhdsz;
		b->nval = UNPACK16(b->buf+2);
		b->valsz = UNPACK16(b->buf+4);
		b->nbuf = UNPACK16(b->buf+6);
		b->bufsz = UNPACK16(b->buf+8);
		break;
	case Tleaf:
		b->data = b->buf + Leafhdsz;
		b->nval = UNPACK16(b->buf+2);
		b->valsz = UNPACK16(b->buf+4);
		break;
	}
	assert(b->magic == Magic);
	return b;
}

static Arena*
pickarena(int hint, int tries)
{
	int n;

	n = tries + hint + ainc(&fs->roundrobin)/1024;
	return &fs->arenas[n%fs->narena];
}

Arena*
getarena(vlong b)
{
	int i;

	i = b / fs->arenasz;
	if(i < 0 || i >= fs->narena){
		werrstr("out of range block %lld", b);
		abort();
		return nil;
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
	assert(avllookup(t, r, 0) == nil);
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

static int
syncarena(Arena *a)
{
	packarena(a->b->data, Blksz, a, fs);
	finalize(a->b);
	return syncblk(a->b);
}

static int
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

/*
 * Logs an allocation. Must be called
 * with arena lock held. Duplicates some
 * of the work in allocblk to prevent
 * recursion.
 */
static int
logappend(Arena *a, vlong off, vlong len, int op, Blk **tl)
{
	Blk *pb, *lb;
	vlong o, ao;
	char *p;

	assert(off % Blksz == 0);
	assert(op == LogAlloc || op == LogFree);
	o = -1;
	lb = *tl;
	dprint("logop %llx+%llx@%llx: %s\n", off, len, lb->logsz, (op == LogAlloc) ? "Alloc" : "Free");
	/*
	 * move to the next block when we have
	 * 40 bytes in the log:
	 * We're appending up to 16 bytes as
	 * part of the operation, followed by
	 * 16 bytes of new log entry allocation
	 * and chaining.
	 */
	if(lb == nil || lb->logsz >= Logspc - 40){
		pb = lb;
		if((o = blkalloc_lk(a, 1)) == -1)
			return -1;
		if((lb = cachepluck()) == nil)
			return -1;
		initblk(lb, o, Tlog);

		lb->logsz = Loghashsz;
		p = lb->data + lb->logsz;
		PACK64(p+0, o|LogAlloc1);
		PACK64(p+8, (uvlong)LogEnd);
		finalize(lb);

		if(syncblk(lb) == -1){
			dropblk(lb);
			return -1;
		}

		if(pb != nil){
			p = pb->data + pb->logsz;
			PACK64(p, lb->bp.addr|LogChain);
			finalize(pb);
			if(syncblk(pb) == -1){
				dropblk(pb);
				return -1;
			}
			dropblk(pb);
		}
		*tl = lb;
	}

	if(len == Blksz){
		if(op == LogAlloc)
			op = LogAlloc1;
		else if(op == LogFree)
			op = LogFree1;
	}
	off |= op;
	p = lb->data + lb->logsz;
	PACK64(p, off);
	lb->logsz += 8;
	if(op >= Log2wide){
		PACK64(p+8, len);
		lb->logsz += 8;
	}
	/*
	 * The newly allocated log block needs
	 * to be recorded. If we're compressing
	 * a log, it needs to go to the tail of
	 * the new log, rather than after the
	 * current allocation. so that we don't
	 * reorder allocs and frees.
	 */
	if(o != -1){
		p = lb->data + lb->logsz;
		ao = o|LogAlloc1;
		PACK64(p, ao);
		lb->logsz += 8;
	}
	/* this gets overwritten by the next append */
	p = lb->data + lb->logsz;
	PACK64(p, (uvlong)LogEnd);
	return 0;

}

static int
logop(Arena *a, vlong off, vlong len, int op)
{
	if(logappend(a, off, len, op, &a->tail) == -1)
		return -1;
	if(a->head.addr == -1)
		a->head = a->tail->bp;
	return 0;
}

int
loadlog(Arena *a)
{
	vlong ent, off, len;
	int op, i, n;
	uvlong bh;
	Bptr bp;
	char *d;
	Blk *b;


	bp = a->head;
Nextblk:
	if((b = getblk(bp, GBnochk)) == nil)
		return -1;
	bh = UNPACK64(b->data);
	/* the hash covers the log and offset */
	if(bh != siphash(b->data+Loghashsz, Logspc-Loghashsz)){
		werrstr("corrupt log");
		return -1;
	}
	for(i = Loghashsz; i < Logspc; i += n){
		d = b->data + i;
		ent = UNPACK64(d);
		op = ent & 0xff;
		off = ent & ~0xff;
		n = (op >= Log2wide) ? 16 : 8;
		switch(op){
		case LogEnd:
			dprint("log@%d: end\n", i);
			dropblk(b);
			return 0;
		case LogChain:
			bp.addr = off & ~0xff;
			bp.hash = -1;
			bp.gen = -1;
			dropblk(b);
			dprint("log@%d: chain %B\n", i, bp);
			goto Nextblk;
		case LogAlloc:
		case LogAlloc1:
			len = (op >= Log2wide) ? UNPACK64(d+8) : Blksz;
			dprint("log@%d alloc: %llx+%llx\n", i, off, len);
			if(grabrange(a->free, off & ~0xff, len) == -1)
				return -1;
			break;
		case LogFree:
		case LogFree1:
			len = (op >= Log2wide) ? UNPACK64(d+8) : Blksz;
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
	}
	return -1;
}

int
compresslog(Arena *a)
{
	Arange *r;
	Range *log, *nlog;
	vlong v, ba, na, graft, oldhd;
	int i, n, sz;
	Blk *b, *hd, *tl;
	Bptr bp;
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
	if((ba = blkalloc_lk(a, 1)) == -1)
		return -1;
	if((b = cachepluck()) == nil)
		return -1;
	initblk(b, ba, Tlog);
	b->logsz = Loghashsz;

	p = b->data + b->logsz;
	PACK64(p, (uvlong)LogEnd);
	finalize(b);
	if(syncblk(b) == -1){
		dropblk(b);
		return -1;
	}

	graft = b->bp.addr;
	if(a->tail != nil){
		finalize(a->tail);
		if(syncblk(a->tail) == -1){
			dropblk(b);
			return -1;
		}
	}
	a->tail = b;

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
	/*
	 * we need to allocate this block before
	 * we prepare the ranges we write back,
	 * so we don't record this block as
	 * available when we compress the log.
	 */
	if((ba = blkalloc_lk(a, 1)) == -1){
		free(log);
		return -1;
	}
	initblk(b, ba, Tlog);
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
	hd = b;
	tl = b;
	b->logsz = Loghashsz;
	for(i = 0; i < n; i++)
		if(logappend(a, log[i].off, log[i].len, LogFree, &tl) == -1)
			return -1;

	p = tl->data + tl->logsz;
	PACK64(p, LogChain|graft);
	free(log);
	finalize(tl);
	if(syncblk(tl) == -1)
		return -1;

	oldhd = a->head.addr;
	a->head.addr = hd->bp.addr;
	a->head.hash = hd->bp.hash;
	a->head.gen = -1;
	if(syncarena(a) == -1)
		return -1;
	if(oldhd != -1){
		for(ba = oldhd; ba != -1; ba = na){
			na = -1;
			bp.addr = ba;
			bp.hash = -1;
			bp.gen = -1;
			if((b = getblk(bp, GBnochk)) == nil)
				return -1;
			for(i = Loghashsz; i < Logspc; i += n){
				p = b->data + i;
				v = UNPACK64(p);
				n = ((v&0xff) >= Log2wide) ? 16 : 8;
				if((v&0xff) == LogChain){
					na = v & ~0xff;
					break;
				}else if((v&0xff) == LogEnd){
					na = -1;
					break;
				}
			}
			lock(a);
			cachedel(b->bp.addr);
			if(blkdealloc_lk(ba) == -1){
				unlock(a);
				return -1;
			}
			dropblk(b);
			unlock(a);
		}
	}
	finalize(a->tail);
	if(syncblk(a->tail) == -1)
		return -1;
	return 0;
}

/*
 * Allocate from an arena, with lock
 * held. May be called multiple times
 * per operation, to alloc space for
 * the alloc log.
 */
static vlong
blkalloc_lk(Arena *a, int force)
{
	Avltree *t;
	Arange *r;
	vlong b;

	t = a->free;
	r = (Arange*)t->root;
	if(!force && a->size - a->used <= a->reserve)
		return -1;
	if(r == nil){
		fprint(2, "out of space");
		abort();
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
	a->used += Blksz;
	return b;
}

static int
blkdealloc_lk(vlong b)
{
	Arena *a;
	int r;

	r = -1;
	a = getarena(b);
	if(freerange(a->free, b, Blksz) == -1)
		goto out;
	if(logop(a, b, Blksz, LogFree) == -1)
		goto out;
	a->used -= Blksz;
	r = 0;
out:
	return r;
}

static vlong
blkalloc(int hint)
{
	Arena *a;
	vlong b;
	int tries;

	tries = 0;
Again:
	a = pickarena(hint, tries);
	if(a == nil || tries == fs->narena){
		werrstr("no empty arenas");
		return -1;
	}
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
	lock(a);
	if((b = blkalloc_lk(a, 0)) == -1){
		unlock(a);
		goto Again;
	}
	if(logop(a, b, Blksz, LogAlloc) == -1){
		unlock(a);
		return -1;
	}
	unlock(a);
	return b;
}

static Blk*
initblk(Blk *b, vlong bp, int t)
{
	Blk *ob;

	ob = cacheget(bp);
	if(ob != nil){
		fprint(2, "dup block: %#p %B (alloced %#llx freed %#llx lasthold: %#llx, lastdrop: %#llx)\n",
			ob, ob->bp, ob->alloced, ob->freed, ob->lasthold, ob->lastdrop);
		abort();
	}
	b->type = t;
	b->bp.addr = bp;
	b->bp.hash = -1;
	b->bp.gen = fs->nextgen;
	switch(t){
	case Traw:
	case Tarena:
		b->data = b->buf;
		break;
	case Tdead:
	case Tlog:
		b->data = b->buf + Loghdsz;
		break;
	case Tpivot:
		b->data = b->buf + Pivhdsz;
		break;
	case Tleaf:
		b->data = b->buf + Leafhdsz;
		break;
	}
	b->fnext = nil;

	setflag(b, Bdirty);
	b->nval = 0;
	b->valsz = 0;
	b->nbuf = 0;
	b->bufsz = 0;
	b->logsz = 0;
	b->lognxt = 0;
	b->alloced = getcallerpc(&b);

	return b;
}

Blk*
newblk(int t)
{
	vlong bp;
	Blk *b;

	if((bp = blkalloc(t)) == -1)
		return nil;
	if((b = cachepluck()) == nil)
		return nil;
	initblk(b, bp, t);
	b->alloced = getcallerpc(&t);
	return b;
}

Blk*
dupblk(Blk *b)
{
	Blk *r;

	if((r = newblk(b->type)) == nil)
		return nil;

	setflag(r, Bdirty);
	r->bp.hash = b->bp.hash;
	r->nval = b->nval;
	r->valsz = b->valsz;
	r->nbuf = b->nbuf;
	r->bufsz = b->bufsz;
	r->logsz = b->logsz;
	r->lognxt = b->lognxt;
	r->alloced = getcallerpc(&b);
	memcpy(r->buf, b->buf, sizeof(r->buf));
	return r;
}

void
finalize(Blk *b)
{
	uvlong h;

	if(b->type != Traw)
		PACK16(b->buf, b->type);

	switch(b->type){
	default:
	case Tpivot:
		PACK16(b->buf+2, b->nval);
		PACK16(b->buf+4, b->valsz);
		PACK16(b->buf+6, b->nbuf);
		PACK16(b->buf+8, b->bufsz);
		b->bp.hash = blkhash(b);
		break;
	case Tleaf:
		PACK16(b->buf+2, b->nval);
		PACK16(b->buf+4, b->valsz);
		b->bp.hash = blkhash(b);
		break;
	case Tlog:
	case Tdead:
		h = siphash(b->data + Loghashsz, Logspc-Loghashsz);
		PACK64(b->data, h);
		b->bp.hash = blkhash(b);
		break;
	case Traw:
		b->bp.hash = blkhash(b);
		break;
	case Tmagic:
	case Tarena:
		break;
	}

	setflag(b, Bfinal);
	cacheins(b);
}

Blk*
getblk(Bptr bp, int flg)
{
	uvlong h;
	Blk *b;
	int i;

	i = ihash(bp.addr) % nelem(fs->blklk);
	qlock(&fs->blklk[i]);
	if((b = cacheget(bp.addr)) != nil){
		qunlock(&fs->blklk[i]);
		return b;
	}
	if((b = readblk(bp.addr, flg)) == nil){
		qunlock(&fs->blklk[i]);
		return nil;
	}
	b->alloced = getcallerpc(&bp);
	h = blkhash(b);
	if((flg&GBnochk) == 0 && h != bp.hash){
		fprint(2, "corrupt block %p %B: %.16llux != %.16llux\n", b, bp, h, bp.hash);
		qunlock(&fs->blklk[i]);
		abort();
		return nil;
	}
	b->bp.hash = h;
	b->bp.gen = bp.gen;
	cacheins(b);
	qunlock(&fs->blklk[i]);

	return b;
}


Blk*
holdblk(Blk *b)
{
	ainc(&b->ref);
	b->lasthold1 = b->lasthold0;
	b->lasthold0 = b->lasthold;
	b->lasthold = getcallerpc(&b);
	return b;
}

void
dropblk(Blk *b)
{
	assert(b == nil || b->ref > 0);
	if(b == nil || adec(&b->ref) != 0)
		return;
	b->lastdrop1 = b->lastdrop0;
	b->lastdrop0 = b->lastdrop;
	b->lastdrop = getcallerpc(&b);
	/*
	 * While a freed block can get resurrected
	 * before quiescence, it's unlikely -- so
	 * it goes into the bottom of the LRU to
	 * get selected early for reuse.
	 */
	if(checkflag(b, Bfreed))
		lrubot(b);
	else
		lrutop(b);
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
		fprint(2, "invalid block @%lld\n", b->bp.addr);
		abort();
	}
	return 0; // shut up kencc
}

void
deferfree(Tree *t, Bptr bp, Blk *b)
{
	Bfree *f;
	ulong ge;

	if(t != nil && t != &fs->snap && bp.gen <= t->gen){
		killblk(t, bp);
		return;
	}

	if((f = malloc(sizeof(Bfree))) == nil)
		return;
	f->bp = bp;
	f->b = b;

	ge = agetl(&fs->epoch);
	f->next = fs->limbo[ge];
	fs->limbo[ge] = f;
}

void
freebp(Tree *t, Bptr bp)
{
	deferfree(t, bp, nil);
}

void
freeblk(Tree *t, Blk *b)
{
	holdblk(b);
	b->freed = getcallerpc(&t);
	setflag(b, Bfreed);
	deferfree(t, b->bp, b);
}

void
epochstart(int tid)
{
	ulong ge;

	ge = agetl(&fs->epoch);
	asetl(&fs->lepoch[tid], ge | Eactive);
}

void
epochend(int tid)
{
	ulong le;

	le = agetl(&fs->lepoch[tid]);
	asetl(&fs->lepoch[tid], le &~ Eactive);
}

void
epochclean(void)
{
	ulong e, ge;
	Bfree *p, *n;
	Arena *a;
	int i;

	ge = agetl(&fs->epoch);
	for(i = 0; i < fs->nworker; i++){
		e = agetl(&fs->lepoch[i]);
		if((e & Eactive) && e != (ge | Eactive))
			return;
	}
	lock(&fs->freelk);
	p = fs->limbo[(ge+1)%3];
	fs->limbo[(ge+1)%3] = nil;
	unlock(&fs->freelk);
	asetl(&fs->epoch, (ge+1) % 3);


	while(p != nil){
		n = p->next;
		a = getarena(p->bp.addr);

		lock(a);
		cachedel(p->bp.addr);
		blkdealloc_lk(p->bp.addr);
		if(p->b != nil)
			dropblk(p->b);
		unlock(a);

		free(p);
		p = n;
	}
}

int
blkcmp(Blk *a, Blk *b)
{
	if(a->qgen != b->qgen)
		return (a->qgen < b->qgen) ? -1 : 1;
	if(a->bp.addr != b->bp.addr)
		return (a->bp.addr < b->bp.addr) ? -1 : 1;
	return 0;
}

void
enqueue(Blk *b)
{
	Arena *a;

	b->qgen = aincv(&fs->qgen, 1);
	a = getarena(b->bp.addr);
	assert(checkflag(b, Bdirty));
	holdblk(b);
	finalize(b);
	qput(a->sync, b);
}

void
qinit(Syncq *q)
{
	q->fullrz.l = &q->lk;
	q->emptyrz.l = &q->lk;
	q->nheap = 0;
	q->heapsz = fs->cmax;
	if((q->heap = malloc(q->heapsz*sizeof(Blk*))) == nil)
		sysfatal("alloc queue: %r");

}

void
qput(Syncq *q, Blk *b)
{
	int i;

	qlock(&q->lk);
	while(q->nheap == q->heapsz)
		rsleep(&q->fullrz);
	for(i = q->nheap; i > 0; i = (i-1)/2){
		if(blkcmp(b, q->heap[(i-1)/2]) == 1)
			break;
		q->heap[i] = q->heap[(i-1)/2];
	}
	q->heap[i] = b;
	q->nheap++;
	rwakeup(&q->emptyrz);
	qunlock(&q->lk);
}

static Blk*
qpop(Syncq *q)
{
	int i, l, r, m;
	Blk *b, *t;

	qlock(&q->lk);
	while(q->nheap == 0)
		rsleep(&q->emptyrz);
	b = q->heap[0];
	if(--q->nheap == 0)
		goto Out;

	i = 0;
	q->heap[0] = q->heap[q->nheap];
	while(1){
		m = i;
		l = 2*i+1;
		r = 2*i+2;
		if(l < q->nheap && blkcmp(q->heap[m], q->heap[l]) == 1)
			m = l;
		if(r < q->nheap && blkcmp(q->heap[m], q->heap[r]) == 1)
			m = r;
		if(m == i)
			break;
		t = q->heap[m];
		q->heap[m] = q->heap[i];
		q->heap[i] = t;
		i = m;
	}
Out:
	rwakeup(&q->fullrz);
	qunlock(&q->lk);
	return b;

}

void
runsync(int, void *p)
{
	Syncq *q;
	Blk *b;

	q = p;
	while(1){
		b = qpop(q);
		if(b->type == Tmagic){
			qlock(&fs->synclk);
			if(--fs->syncing == 0)
				rwakeupall(&fs->syncrz);
			qunlock(&fs->synclk);
		}else if(!checkflag(b, Bfreed)){
			if(syncblk(b) == -1){
				ainc(&fs->broken);
				fprint(2, "write: %r\n");
				abort();
			}
		}
		dropblk(b);
	}
}

void
sync(void)
{
	Arena *a;
	Blk *b;
	int i;

	qlock(&fs->synclk);
	fs->syncing = fs->nsyncers;
	for(i = 0; i < fs->nsyncers; i++){
		b = cachepluck();
		b->type = Tmagic;
		lock(&fs->freelk);
		unlock(&fs->freelk);
		qput(&fs->syncq[i], b);
	}
	while(fs->syncing != 0)
		rsleep(&fs->syncrz);
	for(i = 0; i < fs->narena; i++){
		a = &fs->arenas[i];
		finalize(a->tail);
		if(syncblk(a->tail) == -1)
			sysfatal("sync arena: %r");
		if(syncarena(a) == -1)
			sysfatal("sync arena: %r");
	}
	qunlock(&fs->synclk);
}
