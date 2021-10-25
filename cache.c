#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"

Blk*
cacheblk(Blk *b)
{
	Bucket *bkt;
	Blk *e, *c;
	u32int h;

	/* FIXME: better hash. */
	refblk(b);
	assert(b->bp.addr != 0);
	assert(!(b->flag & Bzombie));
	h = ihash(b->bp.addr);
	bkt = &fs->cache[h % fs->cmax];
	lock(bkt);
	for(e = bkt->b; e != nil; e = e->hnext){
		if(b == e)
			goto Found;
		assert(b->bp.addr != e->bp.addr);
	}
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
		wlock(b);
		b->flag |= Bcached;
		wunlock(b);
		fs->ccount++;
		refblk(b);
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
		if(b->bp.addr == del){
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
