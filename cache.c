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
	if(b == nil){
		unlock(bkt);
		return;
	}
	*p = b->hnext;
	unlock(bkt);

	assert(b != fs->chead || b != fs->ctail);
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
	clrflag(b, Bcached);
	fs->ccount--;
	putblk(b);
}

Blk*
cacheblk(Blk *b)
{
	Bucket *bkt;
	Blk *e, *c;
	u32int h;

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
	b->cnext = fs->chead;
	b->cprev = nil;
	fs->chead = b;
	if((b->flag & Bcached) == 0){
		setflag(b, Bcached);
		fs->ccount++;
		refblk(b);
	}
	for(c = fs->ctail; c != nil && fs->ccount >= fs->cmax; c = fs->ctail)
		cachedel_lk(c->bp.addr);

Cached:
	unlock(&fs->lrulk);
	return b;
}

void
cachedel(vlong del)
{
	lock(&fs->lrulk);
	cachedel_lk(del);
	unlock(&fs->lrulk);
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

