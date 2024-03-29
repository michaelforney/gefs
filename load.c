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

static void
mergeinfo(Gefs *fs, Fshdr *fi)
{
	if(fi->blksz != Blksz || fi->bufspc != Bufspc)
		sysfatal("parameter mismatch");
	if(fs->gotinfo && fs->narena != fi->narena)
		sysfatal("arena count mismatch");
	if(fs->gotinfo && fi->nextgen != fs->nextgen)
		fprint(2, "not all arenas synced: rolling back\n");
	fs->Fshdr = *fi;
}

int
loadarena(Arena *a, Fshdr *fi, vlong o)
{
	Blk *b;
	Bptr bp;

	bp.addr = o;
	bp.hash = -1;
	bp.gen = -1;
	if((b = getblk(bp, GBnochk)) == nil)
		return -1;
	if(unpackarena(a, fi, b->data, Blksz) == nil)
		return -1;
	if((a->free = avlcreate(rangecmp)) == nil)
		return -1;
	a->b = b;
	return 0;
}

void
loadfs(char *dev)
{
	Mount *mnt;
	Fshdr fi;
	Arena *a;
	char *e;
	Tree *t;
	int i, k;

	if((mnt = mallocz(sizeof(*mnt), 1)) == nil)
		sysfatal("malloc: %r");
	if((mnt->name = strdup("dump")) == nil)
		sysfatal("malloc: %r");
	mnt->ref = 1;
	mnt->root = nil;
	mnt->gen = -1;
	mnt->root = &fs->snap;

	fs->opensnap = nil;
	fs->snapmnt = mnt;
	fs->gotinfo = 0;
	fs->narena = 1;
	if((fs->fd = open(dev, ORDWR)) == -1)
		sysfatal("open %s: %r", dev);
	if((fs->arenas = calloc(1, sizeof(Arena))) == nil)
		sysfatal("malloc: %r");
	for(i = 0; i < fs->narena; i++){
		a = &fs->arenas[i];
		if((loadarena(a, &fi, i*fs->arenasz)) == -1)
			sysfatal("loadfs: %r");
		a->reserve = a->size / 1024;
		if(a->reserve < 32*MiB)
			a->reserve = 32*MiB;
		mergeinfo(fs, &fi);
		if(!fs->gotinfo){
			if((fs->arenas = realloc(fs->arenas, fs->narena*sizeof(Arena))) == nil)
				sysfatal("malloc: %r");
			for(k = 1; k < fs->narena; k++)
				memset(&fs->arenas[k], 0, sizeof(Arena));
			fs->gotinfo = 1;
		}
	}
	for(i = 0; i < fs->narena; i++){
		a = &fs->arenas[i];
		if(loadlog(a) == -1)
			sysfatal("load log: %r");
		if(compresslog(a) == -1)
			sysfatal("compress log: %r");
	}
	for(i = 0; i < Ndead; i++){
		fs->snap.dead[i].prev = -1;
		fs->snap.dead[i].head.addr = -1;
		fs->snap.dead[i].head.hash = -1;
		fs->snap.dead[i].head.gen = -1;
		fs->snap.dead[i].ins = nil;
	}

	fprint(2, "load %s:\n", dev);
	fprint(2, "\tsnaptree:\t%B\n", fs->snap.bp);
	fprint(2, "\tnarenas:\t%d\n", fs->narena);
	fprint(2, "\tarenasz:\t%lld MiB\n", fs->arenasz/MiB);
	fprint(2, "\tnextqid:\t%lld\n", fs->nextqid);
	fprint(2, "\tnextgen:\t%lld\n", fs->nextgen);
	fprint(2, "\tblocksize:\t%lld\n", Blksz);
	fprint(2, "\tcachesz:\t%lld MiB\n", fs->cmax*Blksz/MiB);
	if((t = openlabel("main")) == nil)
		sysfatal("load users: no main label");
	if((e = loadusers(2, t)) != nil)
		sysfatal("load users: %s\n", e);
}
