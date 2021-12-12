#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"

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
		if(b->bp.addr == del){
			*p = b->hnext;
			break;
		}
		p = &b->hnext;
	}
	unlock(bkt);
	assert(b != nil);

	if(b->cnext != nil)
		b->cnext->cprev = b->cprev;
	if(b->cprev != nil)
		b->cprev->cnext = b->cnext;
	if(fs->ctail == b)
		fs->ctail = b->cprev;
	if(fs->chead == b)
		fs->chead = b->cnext;
	b->cnext = nil;
	b->cprev = nil;
}

Blk*
cacheblk(Blk *b)
{
	Bucket *bkt;
	Blk *e, *c;
	u32int h;

	assert(b->bp.addr != 0);
	if(b->flag & Bzombie){
		print("caching zombie: %B, flg=%x, freed=0x%p\n", b->bp, b->flag, b->freed);
		abort();
	}
	h = ihash(b->bp.addr);
	bkt = &fs->cache[h % fs->cmax];
	lock(bkt);
	for(e = bkt->b; e != nil; e = e->hnext){
		if(b == e)
			goto Found;
		assert(b->bp.addr != e->bp.addr);
	}
	b->hnext = bkt->b;
	bkt->b = b;
Found:
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
	if((b->flag&Bcached) == 0){
		lock(b);
		b->flag |= Bcached;
		unlock(b);
		fs->ccount++;
		refblk(b);
	}
	c=0;
	USED(c);
#ifdef NOTYET
	for(c = fs->ctail; c != nil && fs->ccount >= fs->cmax; c = fs->ctail){
		cachedel(c->bp.addr);
		putblk(c);
	}
#endif
Cached:
	unlock(&fs->lrulk);
	return b;
}

Blk*
lookupblk(vlong off)
{
	Bucket *bkt;
	u32int h;
	Blk *b;

	h = ihash(off);

	bkt = &fs->cache[h % fs->cmax];
	lock(bkt);
	for(b = bkt->b; b != nil; b = b->hnext)
		if(b->bp.addr == off){
 			refblk(b);
			break;
		}
	unlock(bkt);
	return b;
}

