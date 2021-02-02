#include <u.h>
#include <libc.h>

#include "dat.h"
#include "fns.h"

Blk*
newblk(int t)
{
	Blk *b;

	b = emalloc(sizeof(Blk));
	b->flag |= Bdirty;
	b->type = t;
	return b;	
}

Blk*
getblk(uvlong bp, uvlong bh)
{
	Blk *b;

	b = (Blk*)bp;
	if(blkhash(b) != bh){
		werrstr("corrupt block %llx\n", bp);
		fprint(2, "corrupt block %llx\n", bp);
//		abort();
	}
	return b;
}

void
freeblk(Blk *b)
{
	b->refs--;
	if(b->refs == 0)
		free(b);
}

uvlong
blkhash(Blk *b)
{
	return siphash(b->data, Blksz);
}

ushort
blkfill(Blk *b)
{
	switch(b->type){
	case Pivot:
		return 2*b->nmsg + b->bufsz +  2*b->nent + b->valsz;
	case Leaf:
		return 2*b->nent + b->valsz;
	default:
		abort();
		return 0; // shut up kencc
	}
}

int
putblk(Blk *b)
{
	USED(b);
	/* TODO: unref. */
	return 0;
}
