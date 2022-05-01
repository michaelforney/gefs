#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"

static void
cachedel_lk(vlong del)
{
	Bucket *bkt;
	Blk *b, **p;
	u32int h;

	h = ihash(del);
	bkt = &fs->cache[h % fs->cmax];
	lock(bkt);
	p = &bkt->b;
	for(b = bkt->b; b != nil; b = b->hnext){
		if(b->bp.addr == del)
			break;
		p = &b->hnext;
	}
	unlock(bkt);
	if(b == nil)
		return;
	assert(checkflag(b, Bcached));

	*p = b->hnext;
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
	fs->ccount--;

	clrflag(b, Bcached);
	putblk(b);
}

void
cachedel(vlong del)
{
	lock(&fs->lrulk);
	cachedel_lk(del);
	unlock(&fs->lrulk);
}

Blk*
cacheblk(Blk *b)
{
	Bucket *bkt;
	u32int h;
	Blk *e;

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
	if(!checkflag(b, Bcached)){
		setflag(b, Bcached);
		refblk(b);
		fs->ccount++;
	}
Found:
	unlock(bkt);
	return b;
}

Blk*
lrubump(Blk *b)
{
	Blk *c;

	lock(&fs->lrulk);
	if(checkflag(b, Bcached) == 0){
		assert(b->cnext == nil);
		assert(b->cprev == nil);
		goto Done;
	}
	if(b == fs->chead)
		fs->chead = b->cnext;

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
	b->cnext = fs->chead;
	b->cprev = nil;
	fs->chead = b;
	for(c = fs->ctail; c != b && fs->ccount >= fs->cmax; c = fs->ctail){
		assert(c != fs->chead);
		cachedel_lk(c->bp.addr);
	}

Done:
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

	inc64(&fs->stats.cachelook, 1);
	bkt = &fs->cache[h % fs->cmax];
	lock(bkt);
	for(b = bkt->b; b != nil; b = b->hnext)
		if(b->bp.addr == off){
			inc64(&fs->stats.cachehit, 1);
 			refblk(b);
			break;
		}
	unlock(bkt);
	return b;
}

