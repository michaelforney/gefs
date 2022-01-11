#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"

static int
rangecmp(Avl *a, Avl *b)
{
	return ((Arange*)a)->off - ((Arange*)b)->off;
}

int
loadarena(Arena *a, vlong o)
{
	Blk *b;
	char *p;
	Bptr bp;

	if((a->free = avlcreate(rangecmp)) == nil)
		return -1;
	bp.addr = o;
	bp.hash = -1;
	bp.gen = -1;
	if((b = getblk(bp, GBnochk)) == nil)
		return -1;
	p = b->data;
	a->b = b;
	a->log.head.addr = GBIT64(p);	p += 8;
	a->log.head.hash = GBIT64(p);	p += 8;
	a->log.head.gen = -1;
	a->size = GBIT64(p);	p += 8;
	a->used = GBIT64(p);
	if(loadlog(a) == -1)
		return -1;
	if(compresslog(a) == -1)
		return -1;
	return 0;
}

void
loadfs(char *dev)
{
	int i, blksz, bufspc, hdrsz;
	vlong sb;
	char *p, *e;
	Bptr bp;
	Tree *t;
	Blk *b;
	Dir *d;

	fs->osnap = nil;
	if((fs->fd = open(dev, ORDWR)) == -1)
		sysfatal("open %s: %r", dev);
	if((d = dirfstat(fs->fd)) == nil)
		sysfatal("ream: %r");
	sb = d->length - (d->length % Blksz) - Blksz;
	free(d);

	bp.addr = sb;
	bp.hash = -1;
	bp.gen = -1;
	if((b = getblk(bp, GBnochk)) == nil)
		sysfatal("read superblock: %r");
	if(b->type != Tsuper)
		sysfatal("corrupt superblock: bad type");
	if(memcmp(b->data, "gefs0001", 8) != 0)
		sysfatal("corrupt superblock: bad magic");
	p = b->data + 8;

	blksz = GBIT32(p);		p += 4;
	bufspc = GBIT32(p);		p += 4;
	hdrsz = GBIT32(p);		p += 4;
	fs->snap.ht = GBIT32(p);	p += 4;
	fs->snap.bp.addr = GBIT64(p);	p += 8;
	fs->snap.bp.hash = GBIT64(p);	p += 8;
	fs->snap.bp.gen = GBIT64(p);	p += 8;
	fs->narena = GBIT32(p);		p += 4;
	fs->arenasz = GBIT64(p);	p += 8;
	fs->nextqid = GBIT64(p);	p += 8;
	fs->super = b;
	fs->nextgen = fs->snap.bp.gen + 1;
	for(i = 0; i < Ndead; i++){
		fs->snap.prev[i] = -1;
		fs->snap.dead[i].head.addr = -1;
		fs->snap.dead[i].head.hash = -1;
		fs->snap.dead[i].head.gen = -1;
	}
	fprint(2, "load: %8s\n", p);
	fprint(2, "\tsnaptree:\t%B\n", fs->snap.bp);
	fprint(2, "\tnarenas:\t%d\n", fs->narena);
	fprint(2, "\tarenasz:\t%lld\n", fs->arenasz);
	fprint(2, "\tnextqid:\t%lld\n", fs->nextqid);
	fprint(2, "\tnextgen:\t%lld\n", fs->nextgen);
	fprint(2, "\tcachesz:\t%lld MiB\n", fs->cmax*Blksz/MiB);
	if((fs->arenas = calloc(fs->narena, sizeof(Arena))) == nil)
		sysfatal("malloc: %r");
	for(i = 0; i < fs->narena; i++)
		if((loadarena(&fs->arenas[i], i*fs->arenasz)) == -1)
			sysfatal("loadfs: %r");
	if(bufspc != Bufspc)
		sysfatal("fs uses different buffer size");
	if(hdrsz != Hdrsz)
		sysfatal("fs uses different buffer size");
	if(blksz != Blksz)
		sysfatal("fs uses different block size");
	if((t = openlabel("main")) == nil)
		sysfatal("load users: no main label");
	if((e = loadusers(2, t)) != nil)
		sysfatal("load users: %s\n", e);
}
