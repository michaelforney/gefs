#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"

static void
lrudel(Blk *b)
{
	if(b == fs->chead)
		fs->chead = b->cnext;
	if(b == fs->ctail)
		fs->ctail = b->cprev;
	if(b->cnext != nil)
		b->cnext->cprev = b->cprev;
	if(b->cprev != nil)
		b->cprev->cnext = b->cnext;
	b->cnext = nil;
	b->cprev = nil;		
}

void
lrutop(Blk *b)
{
	qlock(&fs->lrulk);
	/*
	 * Someone got in first and did a
	 * cache lookup; we no longer want
	 * to put this into the LRU, because
	 * its now in use.
	 */
	assert(b->magic == Magic);
	if(b->ref != 0){
		qunlock(&fs->lrulk);
		return;
	}
	lrudel(b);
	if(fs->chead != nil)
		fs->chead->cprev = b;
	if(fs->ctail == nil)
		fs->ctail = b;
	b->cnext = fs->chead;
	fs->chead = b;
	rwakeup(&fs->lrurz);
	qunlock(&fs->lrulk);
}

void
lrubot(Blk *b)
{
	qlock(&fs->lrulk);
	/*
	 * Someone got in first and did a
	 * cache lookup; we no longer want
	 * to put this into the LRU, because
	 * its now in use.
	 */
	assert(b->magic == Magic);
	if(b->ref != 0){
		qunlock(&fs->lrulk);
		return;
	}
	lrudel(b);
	if(fs->ctail != nil)
		fs->ctail->cnext = b;
	if(fs->chead == nil)
		fs->chead = b;
	b->bp.addr = -1;
	b->bp.hash = -1;
	b->cprev = fs->ctail;
	fs->ctail = b;
	rwakeup(&fs->lrurz);
	qunlock(&fs->lrulk);
}

void
cacheins(Blk *b)
{
	Bucket *bkt;
	u32int h;

	assert(b->magic == Magic);
	h = ihash(b->bp.addr);
	bkt = &fs->cache[h % fs->cmax];
	lock(bkt);
	if(checkflag(b, Bcached)){
		unlock(bkt);
		return;
	}
	setflag(b, Bcached);
	b->hnext = bkt->b;
	bkt->b = b;
	unlock(bkt);
}

void
cachedel(vlong addr)
{
	Bucket *bkt;
	Blk *b, **p;
	u32int h;

	if(addr == -1)
		return;

	h = ihash(addr);
	bkt = &fs->cache[h % fs->cmax];
	lock(bkt);
	p = &bkt->b;
	for(b = bkt->b; b != nil; b = b->hnext){
		if(b->bp.addr == addr){
			*p = b->hnext;
			b->bp.addr = -1;
			b->bp.hash = -1;
			clrflag(b, Bcached);
			b->hnext = nil;
			break;
		}
		p = &b->hnext;
	}
	unlock(bkt);
}

Blk*
cacheget(vlong off)
{
	Bucket *bkt;
	u32int h;
	Blk *b;

	h = ihash(off);

	inc64(&fs->stats.cachelook, 1);
	bkt = &fs->cache[h % fs->cmax];

	qlock(&fs->lrulk);
	lock(bkt);
	for(b = bkt->b; b != nil; b = b->hnext){
		if(b->bp.addr == off){
			inc64(&fs->stats.cachehit, 1);
 			holdblk(b);
			lrudel(b);
			b->lasthold = getcallerpc(&off);
			break;
		}
	}
	unlock(bkt);
	qunlock(&fs->lrulk);

	return b;
}

/*
 * Pulls the block from the bottom of the LRU for reuse.
 */
Blk*
cachepluck(void)
{
	Blk *b;

	qlock(&fs->lrulk);
	while(fs->ctail == nil)
		rsleep(&fs->lrurz);

	b = fs->ctail;
	assert(b->magic == Magic);
	assert(b->ref == 0);
	cachedel(b->bp.addr);
	lrudel(b);
	b->flag = 0;
	b->bp.addr = -1;
	b->bp.hash = -1;
	b->lasthold = 0;
	b->lastdrop = 0;
	b->freed = 0;
	b->hnext = nil;
	qunlock(&fs->lrulk);

	return  holdblk(b);
}
