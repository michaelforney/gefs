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

	/* nb: values must be inserted in key order */
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
	setval(r, 0, &kv);

	kv.k = buf;
	kv.nk = 9;
	kv.v = buf+9;
	kv.nv = 8;
	buf[0] = Ksuper;
	PBIT64(buf+1, 0ULL);
	PBIT64(buf+9, 0ULL);
	setval(r, 1, &kv);
}

static void
initsnap(Blk *s, Blk *r)
{
	char kbuf[Keymax], vbuf[Treesz];
	Kvp kv;


	kv.k = kbuf;
	kv.v = vbuf;
	kv.k[0] = Kdset;
	kv.nk = 1 + snprint(kv.k+1, sizeof(kbuf)-1, "main");
	kv.v[0] = Ksnap;
	PBIT64(kv.v+1, 0);
	kv.nv = Snapsz;
	setval(s, 0, &kv);
	
	kv.k[0] = Ksnap;
	PBIT64(kv.k+1, 0);
	kv.nk = Snapsz;

	kv.nv = sizeof(vbuf);
	PBIT32(kv.v +  0, 1);
	PBIT64(kv.v +  4, r->bp.addr);
	PBIT64(kv.v + 12, r->bp.hash);
	PBIT64(kv.v + 20, r->bp.gen);
	PBIT64(kv.v + 28, -1ULL);
	PBIT64(kv.v + 36, -1ULL);
	PBIT64(kv.v + 42, -1ULL);
	setval(s, 1, &kv);
}

static void
reamarena(Arena *a, vlong start, vlong asz)
{
	vlong addr, bo, bh;
	char *p;
	Blk *b;

	addr = start;
	if((b = mallocz(sizeof(Blk), 1)) == nil)
		sysfatal("ream: %r");
	addr += Blksz;	/* arena header */

	a->log.head = -1;
	memset(b, 0, sizeof(Blk));
	b->type = Tlog;
	b->bp.addr = addr;
	b->logsz = 32;
	b->data = b->buf + Hdrsz;
	b->flag |= Bdirty;

	p = b->data+Loghdsz;
	PBIT64(p+ 0, addr|LogFree);		/* addr */
	PBIT64(p+ 8, asz);			/* len */
	PBIT64(p+16, b->bp.addr|LogAlloc);		/* addr */
	PBIT64(p+24, Blksz);			/* len */
	PBIT64(p+32, (uvlong)LogEnd);		/* done */
	finalize(b);
	if(syncblk(b) == -1)
		sysfatal("ream: init log");

	bh = blkhash(b);
	bo = b->bp.addr;

	memset(b, 0, sizeof(Blk));
	b->type = Tarena;
	b->bp.addr = start;
	p = b->buf + Hdrsz;
	print("b->bp.addr: %llx\n", b->bp.addr);
	PBIT64(p+0, bo);
	PBIT64(p+8, bh);
	finalize(b);
	if(syncblk(b) == -1)
		sysfatal("ream: write arena: %r");
}

void
reamfs(char *dev)
{
	vlong sz, asz, off;
	Blk *s, *r, *t;
	Mount *mnt;
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
	if((mnt = mallocz(sizeof(Mount), 1)) == nil)
		sysfatal("ream: alloc mount: %r");
	fs->super = s;
	refblk(s);

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
	s->bp.addr = sz;
	s->data = s->buf + Hdrsz;
	s->ref = 2;

	for(i = 0; i < fs->narena; i++)
		if((loadarena(&fs->arenas[i], i*asz)) == -1)
			sysfatal("ream: loadarena: %r");

	if((t = newblk(Tleaf)) == nil)
		sysfatal("ream: allocate root: %r");
	refblk(t);
	initroot(t);
	finalize(t);
	syncblk(t);

	mnt->root.ht = 1;
	mnt->root.bp = t->bp;

	/*
	 * Now that we have a completely empty fs, give it
	 * a single snap block that the tree will insert
	 * into, and take a snapshot as the initial state.
	 */
	if((r = newblk(Tleaf)) == nil)
		sysfatal("ream: allocate snaps: %r");
	refblk(r);
	initsnap(r, t);
	finalize(r);
	syncblk(r);
	fs->snap.bp = r->bp;
	fs->snap.ht = 1;

	fillsuper(s);
	finalize(s);
	syncblk(s);

	sync();

	putblk(t);
	putblk(s);
	putblk(r);
	free(mnt);
	if(sync() == -1)
		sysfatal("ream: sync: %r");
}
