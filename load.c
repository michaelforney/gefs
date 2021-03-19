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

	if((a->free = avlcreate(rangecmp)) == nil)
		return -1;
	if((b = readblk(o, 0)) == nil)
		return -1;
	a->b = b;
	a->log = GBIT64(b->data+0);
	a->logh = GBIT64(b->data+8);
	if(loadlog(a) == -1)
		return -1;
	if(compresslog(a) == -1)
		return -1;
	return 0;
}

void
loadfs(char *dev)
{
	vlong sb;
	char *p;
	Blk *b;
	Dir *d;
	int i, dirty;

	if((fs->fd = open(dev, ORDWR)) == -1)
		sysfatal("open %s: %r", dev);
	if((d = dirfstat(fs->fd)) == nil)
		sysfatal("ream: %r");
	sb = d->length - (d->length % Blksz) - Blksz;
print("superblock @%llx\n", sb);
	free(d);

	if((b = readblk(sb, 0)) == nil)
		sysfatal("read superblock: %r");
	if(b->type != Tsuper)
		sysfatal("corrupt superblock: bad type");
	p = b->data;
	if(memcmp(p, "gefs0001", 8) != 0)
		sysfatal("corrupt superblock: bad magic");
	dirty = GBIT32(p +  8);
	if(GBIT32(p + 12) != Blksz)
		sysfatal("fs uses different block size");
	if(GBIT32(p + 16) != Bufspc)
		sysfatal("fs uses different buffer size");
	if(GBIT32(p + 20) != Hdrsz)
		sysfatal("fs uses different buffer size");
	fs->height = GBIT32(p + 24);
	fs->rootb = GBIT64(p + 32);
	fs->rooth = GBIT64(p + 40);
	fs->narena = GBIT32(p + 48);
	fs->arenasz = GBIT64(p + 56);
	fs->arenasz = GBIT64(p + 56);
	fs->gen = GBIT64(p + 64);
	fs->nextqid = GBIT64(p + 72);
	fs->super = b;
	fprint(2, "load: %8s\n", p);
	fprint(2, "\theight:\t%d\n", fs->height);
	fprint(2, "\trootb:\t%llx\n", fs->rootb);
	fprint(2, "\trooth:\t%llx\n", fs->rooth);
	fprint(2, "\tarenas:\t%d\n", fs->narena);
	fprint(2, "\tarenasz:\t%lld\n", fs->arenasz);
	fprint(2, "\trootgen:\t%lld\n", fs->gen);
	fprint(2, "\tnextqid:\t%lld\n", fs->nextqid);
	if((fs->arenas = calloc(fs->narena, sizeof(Arena))) == nil)
		sysfatal("malloc: %r");
	for(i = 0; i < fs->narena; i++)
		if((loadarena(&fs->arenas[i], i*fs->arenasz)) == -1)
			sysfatal("loadfs: %r");
	if(dirty){
		fprint(2, "file system was not unmounted cleanly");
		/* TODO: start gc pass */
	}
}
