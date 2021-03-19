#include <u.h>
#include <libc.h>
#include <bio.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"


static void
initroot(Blk *r)
{
	char buf[512];
	Kvp kv;
	Dir d;

	memset(&d, 0, sizeof(Dir));
	d.qid = (Qid){fs->nextqid++, 0, QTDIR};
	d.mode = 0755;
	d.atime = 0;
	d.mtime = 0;
	d.length = 0;
	d.name = "";
	d.uid = "glenda";
	d.gid = "glenda";
	d.muid = "glenda";
	if(dir2kv(-1, &d, &kv, buf, sizeof(buf)) == -1)
		sysfatal("ream: pack root: %r");
	blkinsert(r, &kv);

	kv.k = buf;
	kv.nk = 9;
	kv.v = buf+9;
	kv.nv = 8;
	buf[0] = Ksuper;
	PBIT64(buf+1, 0);
	PBIT64(buf+9, 0);
	blkinsert(r, &kv);
}

static void
reamarena(Arena *a, vlong start, vlong asz)
{
	vlong off, bo, bh;
	char *p;
	Blk *b;

	off = start;
	if((b = mallocz(sizeof(Blk), 1)) == nil)
		sysfatal("ream: %r");
	off += Blksz;	/* arena header */

	a->log = -1;
	memset(b, 0, sizeof(Blk));
	b->type = Tlog;
	b->off = off;
	b->logsz = 32;
	b->data = b->buf + Hdrsz;
	b->flag |= Bdirty;

	p = b->data;
	PBIT64(p+24, off|LgFree);		/* off */
	PBIT64(p+32, asz);			/* len */
	PBIT64(p+40, b->off|LgAlloc);		/* off */
	PBIT64(p+48, Blksz);			/* len */
	finalize(b);
	synclog(b, -1, 32);

	bh = blkhash(b);
	bo = b->off;

	memset(b, 0, sizeof(Blk));
	b->type = Tarena;
	b->off = start;
	p = b->buf + Hdrsz;
	print("b->off: %llx\n", b->off);
	PBIT64(p+0, bo);
	PBIT64(p+8, bh);
	finalize(b);
	if(pwrite(fs->fd, b->buf, Blksz, b->off) == -1)
		sysfatal("ream: write arena: %r");
}

void
reamfs(char *dev)
{
	vlong sz, asz, off;
	Blk *s, *r;
	Dir *d;
	int i;

	if((fs->fd = open(dev, ORDWR)) == -1)
		sysfatal("open %s: %r", dev);
	if((d = dirfstat(fs->fd)) == nil)
		sysfatal("ream: %r");
	if(d->length < 64*MiB)
		sysfatal("ream: disk too small");
	if((s = mallocz(sizeof(Blk), 1)) == nil)
		sysfatal("ream: %r");


	sz = d->length;
	sz = sz - (sz % Blksz) - Blksz;
	fs->narena = sz / (128*GiB);
	if(fs->narena < 1)
		fs->narena = 1;
	if(fs->narena >= 128)
		fs->narena = 128;
	if((fs->arenas = calloc(fs->narena, sizeof(Arena))) == nil)
		sysfatal("malloc: %r");
	free(d);

	asz = sz/fs->narena;
	asz = asz - (asz % Blksz) - Blksz;
	fs->arenasz = asz;
	off = 0;
	fprint(2, "reaming %d arenas:\n", fs->narena);

	for(i = 0; i < fs->narena; i++){
		print("\tarena %d: %lld blocks at %lld\n", i, asz/Blksz, off);
		reamarena(&fs->arenas[i], off, asz);
		asz += off;
	}
	
	s->type = Tsuper;
	s->off = sz;
	s->data = s->buf + Hdrsz;
	fillsuper(s);
	finalize(s);

print("superblock @%llx\n", s->off);
	for(i = 0; i < fs->narena; i++)
		if((loadarena(&fs->arenas[i], i*asz)) == -1)
			sysfatal("ream: loadarena: %r");

	/*
	 * Now that we have a completely empty fs, give it
	 * a single root block that the tree will insert
	 * into, and take a snapshot as the initial state.
	 */
	if((r = newblk(Tleaf)) == nil)
		sysfatal("ream: allocate root: %r");
	initroot(r);
	finalize(r);

	fs->super = s;
	fs->rootb = r->off;
	fs->rooth = blkhash(r);
	fs->height = 1;
	snapshot();

	putblk(s);
	putblk(r);
	if(sync() == -1)
		sysfatal("ream: sync: %r");
}
