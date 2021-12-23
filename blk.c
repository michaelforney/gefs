#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"

typedef struct Range Range;
struct Range {
	vlong off;
	vlong len;
};

static vlong	blkalloc_lk(Arena*);
static vlong	blkalloc(void);
static int	blkdealloc_lk(vlong);
static void	cachedel(vlong);

QLock blklock;

Blk*
readblk(vlong bp, int flg)
{
	Blk *b;
	vlong off, rem, n;

	assert(bp != -1);
	if((b = mallocz(sizeof(Blk), 1)) == nil)
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
	memset(&b->Lock, 0, sizeof(Lock));
	b->type = (flg&GBraw) ? Traw : GBIT16(b->buf+0);
	b->bp.addr = bp;
	b->bp.hash = -1;
	b->bp.gen = -1;
	b->ref = 1;
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
pickarena(void)
{
	long n;

	n = (ainc(&fs->roundrobin)/1024) % fs->narena;
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
logappend(Oplog *ol, Arena *a, vlong off, vlong len, int op)
{
	Blk *pb, *lb;
	vlong o;
	char *p;

	assert(off % Blksz == 0);
	assert(op == LogAlloc || op == LogFree || op == LogDead);
	lb = ol->tail;
	if(lb == nil || lb->logsz > Logspc - 8){
		pb = lb;
		if(a == nil)
			o = blkalloc();
		else
			o = blkalloc_lk(a);
		if(o == -1)
			return nil;
		if((lb = mallocz(sizeof(Blk), 1)) == nil)
			return nil;
		lb->data = lb->buf + Hdrsz;
		lb->flag |= Bdirty;
		lb->type = Tlog;
		lb->bp.addr = o;
		lb->logsz = Loghdsz;
		p = lb->data + lb->logsz;
		PBIT64(p + 0, (uvlong)LogEnd);
		finalize(lb);
		if(syncblk(lb) == -1){
			free(lb);
			return nil;
		}

		ol->tail = lb;
		if(pb != nil){
			p = pb->data + pb->logsz;
			PBIT64(p + 0, lb->bp.addr|LogChain);
			finalize(pb);
			if(syncblk(pb) == -1)
				return nil;
		}
	}

	if(len == Blksz && op == LogAlloc)
		op = LogAlloc1;
	if(len == Blksz && op == LogFree)
		op = LogFree1;
	off |= op;
	p = lb->data + lb->logsz;
	PBIT64(p, off);
	lb->logsz += 8;
	if(op >= Log2wide){
		PBIT64(p+8, len);
		lb->logsz += 8;
	}
	/* this gets overwritten by the next append */
	p = lb->data + lb->logsz;
	PBIT64(p, (uvlong)LogEnd);
	return lb;
}

int
deadlistappend(Tree *t, Bptr bp)
{
	Oplog *l;
	Blk *b;
	int i;

	dprint("deadlisted %B\n", bp);
	l = nil;
	for(i = 0; i < Ndead; i++){
		if(bp.gen >= t->prev[i]){
			l = &t->dead[i];
			break;
		}
	}
	if((b = logappend(l, nil, bp.addr, Blksz, LogDead)) == nil)
		return -1;
	if(l->head == -1)
		l->head = b->bp.addr;
	if(l->tail != b)
		l->tail = b;
	return 0;
}

/*
 * Logs an allocation. Must be called
 * with arena lock held. Duplicates some/c
 * of the work in allocblk to prevent
 * recursion.
 */
int
freelistappend(Arena *a, vlong off, int op)
{
	Blk *b;

	if((b = logappend(&a->log, a, off, Blksz, op)) == nil)
		return -1;
	if(a->log.head == -1)
		a->log.head = b->bp.addr;
	if(a->log.tail != b)
		a->log.tail = b;
	return 0;
}

int
loadlog(Arena *a)
{
	vlong bp, ent, off, len;
	int op, i, n;
	uvlong bh;
	char *d;
	Blk *b;


	bp = a->log.head;

Nextblk:
	if((b = readblk(bp, 0)) == nil)
		return -1;
	cacheblk(b);
	bh = GBIT64(b->data);
	/* the hash covers the log and offset */
	if(bh != siphash(b->data+8, Blkspc-8)){
		werrstr("corrupt log");
		return -1;
	}
	for(i = Loghdsz; i < Logspc; i += n){
		d = b->data + i;
		ent = GBIT64(d);
		op = ent & 0xff;
		off = ent & ~0xff;
		n = (op >= Log2wide) ? 16 : 8;
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
			fs->nextgen = (off >> 8)+1;
			break;
		case LogAlloc:
		case LogAlloc1:
			len = (op >= Log2wide) ? GBIT64(d+8) : Blksz;
			dprint("log@%d alloc: %llx+%llx\n", i, off, len);
			if(grabrange(a->free, off & ~0xff, len) == -1)
				return -1;
			break;
		case LogFree:
		case LogFree1:
			len = (op >= Log2wide) ? GBIT64(d+8) : Blksz;
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
	vlong v, bp, nb, graft, oldhd;
	int i, n, sz;
	Blk *hd, *ab, *b;
	Oplog ol;
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
	b->bp.addr = bp;
	b->ref = 1;
	b->data = b->buf + Hdrsz;
	b->logsz = Loghdsz;

	PBIT64(b->data+b->logsz, (uvlong)LogEnd);
	finalize(b);
	if(syncblk(b) == -1){
		free(b);
		return -1;
	}

	graft = b->bp.addr;
	if(a->log.tail != nil){
		finalize(a->log.tail);
		if(syncblk(a->log.tail) == -1){
			free(b);
			return -1;
		}
	}
	a->log.tail = b;

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
	ol.hash = -1;
	ol.head = b->bp.addr;
	ol.tail = b;
	b->logsz = Loghdsz;
	for(i = 0; i < n; i++)
		if((b = logappend(&ol, a, log[i].off, log[i].len, LogFree)) == nil)
			return -1;
	p = b->data + b->logsz;
	PBIT64(p, LogChain|graft);
	free(log);
	finalize(b);
	if(syncblk(b) == -1)
		return -1;

	oldhd = a->log.head;
	a->log.head = hd->bp.addr;
	a->log.hash = blkhash(hd);
	ab = a->b;
	PBIT64(ab->data + 0, a->log.head);
	PBIT64(ab->data + 8, a->log.hash);
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
				n = ((v&0xff) >= Log2wide) ? 16 : 8;
				if((v&0xff) == LogChain){
					nb = v & ~0xff;
					break;
				}else if((v&0xff) == LogEnd){
					nb = -1;
					break;
				}
			}
			lock(a);
			if(blkdealloc_lk(bp) == -1){
				unlock(a);
				return -1;
			}
			unlock(a);
		}
	}
	finalize(a->log.tail);
	if(syncblk(a->log.tail) == -1)
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
blkdealloc_lk(vlong b)
{
	Arena *a;
	int r;

	r = -1;
	a = getarena(b);
	if(freerange(a->free, b, Blksz) == -1)
		goto out;
	if(freelistappend(a, b, LogFree) == -1)
		goto out;
	r = 0;
out:
	return r;
}

static vlong
blkalloc(void)
{
	Arena *a;
	vlong b;
	int tries;

	tries = 0;
Again:
	a = pickarena();
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
	if((b = blkalloc_lk(a)) == -1){
		unlock(a);
		goto Again;
	}
	if(freelistappend(a, b, LogAlloc) == -1){
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

	if((bp = blkalloc()) == -1)
		return nil;
	if((b = lookupblk(bp)) == nil){
		if((b = mallocz(sizeof(Blk), 1)) == nil)
			return nil;
		/*
		 * If the block is cached,
		 * then the cache holds a ref
		 * to the block, so we only
		 * want to reset the refs
		 * on an allocation.
		 */
		b->ref = 1;
		b->cnext = nil;
		b->cprev = nil;
		b->hnext = nil;
	}
	b->type = t;
	b->bp.addr = bp;
	b->bp.hash = -1;
	b->bp.gen = fs->nextgen;
	b->data = b->buf + Hdrsz;
	b->fnext = nil;

	b->flag = Bdirty;
	b->nval = 0;
	b->valsz = 0;
	b->nbuf = 0;
	b->bufsz = 0;
	b->logsz = 0;
	b->lognxt = 0;

	dprint("new block %B from %p, flag=%x\n", b->bp, getcallerpc(&t), b->flag);
	return cacheblk(b);
}

char*
fillsuper(Blk *b)
{
	char *p;

	assert(b->type == Tsuper);
	p = b->data;
	lock(b);
	b->flag |= Bdirty;
	unlock(b);
	memcpy(p, "gefs0001", 8); p += 8;
	PBIT32(p, 0); p += 4; /* dirty */
	PBIT32(p, Blksz); p += 4;
	PBIT32(p, Bufspc); p += 4;
	PBIT32(p, Hdrsz); p += 4;
	PBIT32(p, fs->snap.ht); p += 4;
	PBIT64(p, fs->snap.bp.addr); p += 8;
	PBIT64(p, fs->snap.bp.hash); p += 8;
	PBIT64(p, fs->snap.bp.gen); p += 8;
	PBIT32(p, fs->narena); p += 4;
	PBIT64(p, fs->arenasz); p += 8;
	PBIT64(p, fs->nextqid); p += 8;
	return p;
}

void
finalize(Blk *b)
{
	vlong h;

	lock(b);
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
		b->bp.hash = blkhash(b);
		break;
	case Tleaf:
		PBIT16(b->buf+2, b->nval);
		PBIT16(b->buf+4, b->valsz);
		b->bp.hash = blkhash(b);
		break;
	case Tlog:
		h = siphash(b->data + 8, Blkspc-8);
		PBIT64(b->data, h);
		b->bp.hash = blkhash(b);
		break;
	case Traw:
		b->bp.hash = blkhash(b);
		break;
	case Tsuper:
	case Tarena:
		break;
	}
	unlock(b);
}

Blk*
getblk(Bptr bp, int flg)
{
	Blk *b;

	if((b = lookupblk(bp.addr)) != nil)
		return cacheblk(b);

	qlock(&blklock);
	if((b = lookupblk(bp.addr)) != nil){
		cacheblk(b);
		qunlock(&blklock);
		return b;
	}
	if((b = readblk(bp.addr, flg)) == nil){
		qunlock(&blklock);
		return nil;
	}
	if(blkhash(b) != bp.hash){
		fprint(2, "corrupt block %B: %llx != %llx\n", bp, blkhash(b), bp.hash);
		qunlock(&blklock);
		abort();
		return nil;
	}
	b->bp.hash = bp.hash;
	b->bp.gen = bp.gen;
	cacheblk(b);
	qunlock(&blklock);

	return b;
}

Blk*
dupblk(vlong bp, uvlong bh)
{
	USED(bp, bh);
	return nil;
}

Blk*
refblk(Blk *b)
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
		fprint(2, "invalid block @%lld\n", b->bp.addr);
		abort();
	}
	return 0; // shut up kencc
}

void
putblk(Blk *b)
{
	if(b == nil || adec(&b->ref) != 0)
		return;
	assert((b->flag & Bqueued) || !(b->flag & Bdirty));
	free(b);
}

void
freebp(Tree *t, Bptr bp)
{
	Bfree *f;

	dprint("[%s] free blk %B\n", (t == &fs->snap) ? "snap" : "data", bp);
	if(bp.gen <= t->prev[0]){
		deadlistappend(t, bp);
		return;
	}
	if((f = malloc(sizeof(Bfree))) == nil)
		return;
	f->bp = bp;
	lock(&fs->freelk);
	f->next = fs->freehd;
	fs->freehd = f;
	unlock(&fs->freelk);
}

void
freeblk(Tree *t, Blk *b)
{
	lock(b);
	assert((b->flag & Bqueued) == 0);
	b->freed = getcallerpc(&b);
	unlock(b);
	freebp(t, b->bp);
}

void
reclaimblk(Bptr bp)
{
	Arena *a;

	a = getarena(bp.addr);
	lock(a);
	blkdealloc_lk(bp.addr);
	unlock(a);
}

void
quiesce(int tid)
{
	int i, allquiesced;
	Bfree *p, *n;

	lock(&fs->activelk);
	allquiesced = 1;
	fs->active[tid]++;
	for(i = 0; i < fs->nproc; i++){
		/*
		 * Odd parity on quiescence implies
		 * that we're between the exit from
		 * and waiting for the next message
		 * that enters us into the critical
		 * section.
		 */
		if((fs->active[i] & 1) == 0)
			continue;
		if(fs->active[i] == fs->lastactive[i])
			allquiesced = 0;
	}
	if(allquiesced)
		for(i = 0; i < fs->nproc; i++)
			fs->lastactive[i] = fs->active[i];
	unlock(&fs->activelk);
	if(!allquiesced)
		return;

	lock(&fs->freelk);
	p = nil;
	if(fs->freep != nil){
		p = fs->freep->next;
		fs->freep->next = nil;
	}
	unlock(&fs->freelk);

	while(p != nil){
		n = p->next;
		reclaimblk(p->bp);
		p = n;
	}
	fs->freep = fs->freehd;
}

int
sync(void)
{
	int i, r;
	Blk *b, *s;
	Arena *a;

	qlock(&fs->snaplk);
	r = 0;
	s = fs->super;
	fillsuper(s);
	enqueue(s);

	for(i = 0; i < fs->narena; i++){
		a = &fs->arenas[i];
		finalize(a->log.tail);
		if(syncblk(a->log.tail) == -1)
			r = -1;
	}
	for(b = fs->chead; b != nil; b = b->cnext){
		if(!(b->flag & Bdirty))
			continue;
		if(syncblk(b) == -1)
			r = -1;
	}
	if(r != -1)
		r = syncblk(s);

	qunlock(&fs->snaplk);
	return r;
}
