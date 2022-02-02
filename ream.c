#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"


static void
initroot(Blk *r)
{
	char *p, kbuf[Keymax], vbuf[Inlmax];
	Kvp kv;
	Xdir d;

	/* nb: values must be inserted in key order */
	memset(&d, 0, sizeof(Xdir));
	d.qid = (Qid){fs->nextqid++, 0, QTDIR};
	d.mode = DMDIR|0775;
	d.atime = 0;
	d.mtime = 0;
	d.length = 0;
	d.name = "";
	d.uid = -1;
	d.gid = -1;
	d.muid = 0;
	if(dir2kv(-1, &d, &kv, vbuf, sizeof(vbuf)) == -1)
		sysfatal("ream: pack root: %r");
	setval(r, &kv);

	if((p = packsuper(kbuf, sizeof(kbuf), 0)) == nil)
		sysfatal("ream: pack super");
	kv.k = kbuf;
	kv.nk = p - kbuf;
	if((p = packdkey(vbuf, sizeof(vbuf), -1, "")) == nil)
		sysfatal("ream: pack super");
	kv.v = vbuf;
	kv.nv = p - vbuf;
	setval(r, &kv);
}

static void
initsnap(Blk *s, Blk *r)
{
	char *p, kbuf[Keymax], vbuf[Treesz];
	Tree t;
	Kvp kv;
	int i;

	p = packlabel(kbuf, sizeof(kbuf), "main");
	kv.k = kbuf;
	kv.nk = p - kbuf;
	p = packsnap(vbuf, sizeof(vbuf), 0);
	kv.v = vbuf;
	kv.nv = p - vbuf;
	setval(s, &kv);

	p = packsnap(kbuf, sizeof(kbuf), 0);
	kv.k = kbuf;
	kv.nk = p - kbuf;

	memset(&t, 0, sizeof(Tree));
	t.ref = 2;
	t.ht = 1;
	t.gen = fs->nextgen++;
	t.bp = r->bp;
	for(i = 0; i < Ndead; i++){
		t.dead[i].prev = -1;
		t.dead[i].head.addr = -1;
		t.dead[i].head.hash = -1;
		t.dead[i].head.gen = -1;
		t.dead[i].ins = nil;
	}
	p = packtree(vbuf, sizeof(vbuf), &t);
	kv.v = vbuf;
	kv.nv = p - vbuf;
	setval(s, &kv);
}

static void
initarena(Arena *a, Fshdr *fi, vlong start, vlong asz)
{
	vlong addr, bo, bh;
	char *p;
	Blk *b;

	b = cachepluck();
	addr = start+Blksz;	/* arena header */

	a->head.addr = -1;
	a->head.hash = -1;
	a->head.gen = -1;

	memset(b->buf, 0, sizeof(b->buf));
	b->type = Tlog;
	b->bp.addr = addr;
	b->logsz = 32;
	b->data = b->buf + Loghdsz;
	setflag(b, Bdirty);

	p = b->data + Loghashsz;
	PACK64(p, addr|LogFree);	p += 8;	/* addr */
	PACK64(p, asz-Blksz);		p += 8;	/* len */
	PACK64(p, b->bp.addr|LogAlloc);	p += 8;	/* addr */
	PACK64(p, Blksz);		p += 8;	/* len */
	PACK64(p, (uvlong)LogEnd);	/* done */
	finalize(b);
	if(syncblk(b) == -1)
		sysfatal("ream: init log");
	dropblk(b);

	bh = b->bp.hash;
	bo = b->bp.addr;

	b = cachepluck();
	memset(b->buf, 0, sizeof(b->buf));
	b->type = Tarena;
	b->bp.addr = start;
	b->data = b->buf;
	a->head.addr = bo;
	a->head.hash = bh;
	a->head.gen = -1;
	a->size = asz;
	a->used = Blksz;
	a->tail = nil;
	packarena(b->data, Blksz, a, fi);
	finalize(b);
	if(syncblk(b) == -1)
		sysfatal("ream: write arena: %r");
	dropblk(b);
}

void
reamfs(char *dev)
{
	vlong sz, asz, off;
	Blk *rb, *tb;
	Mount *mnt;
	Arena *a;
	Dir *d;
	int i;

	if((fs->fd = open(dev, ORDWR)) == -1)
		sysfatal("open %s: %r", dev);
	if((d = dirfstat(fs->fd)) == nil)
		sysfatal("ream: %r");
	sz = d->length;
	free(d);
	if(sz < 64*MiB)
		sysfatal("ream: disk too small");
	if((mnt = mallocz(sizeof(Mount), 1)) == nil)
		sysfatal("ream: alloc mount: %r");
	if((mnt->root = mallocz(sizeof(Tree), 1)) == nil)
		sysfatal("ream: alloc tree: %r");

	fs->narena = (sz + 64ULL*GiB - 1) / (64ULL*GiB);
	if(fs->narena < 8)
		fs->narena = 8;
	if(fs->narena >= 128)
		fs->narena = 128;
	if((fs->arenas = calloc(fs->narena, sizeof(Arena))) == nil)
		sysfatal("malloc: %r");

	asz = sz/fs->narena;
	asz = asz - (asz % Blksz) - Blksz;
	if(asz < 128*MiB)
		sysfatal("disk too small");
	fs->arenasz = asz;
	off = 0;
	for(i = 0; i < fs->narena; i++){
		print("\tarena %d: %lld blocks at %llx\n", i, asz/Blksz, off);
		initarena(&fs->arenas[i], fs, off, asz);
		off += asz;
	}
	for(i = 0; i < Ndead; i++){
		fs->snap.dead[i].prev = -1;
		fs->snap.dead[i].head.addr = -1;
		fs->snap.dead[i].head.hash = -1;
		fs->snap.dead[i].head.gen = -1;
	}
	
	for(i = 0; i < fs->narena; i++){
		a = &fs->arenas[i];
		if((loadarena(a, fs, i*asz)) == -1)
			sysfatal("ream: loadarena: %r");
		if(loadlog(a) == -1)
			sysfatal("load log: %r");
		if(compresslog(a) == -1)
			sysfatal("compress log: %r");
	}
	if((tb = newblk(Tleaf)) == nil)
		sysfatal("ream: allocate root: %r");
	holdblk(tb);
	initroot(tb);
	finalize(tb);
	syncblk(tb);

	mnt->root->ht = 1;
	mnt->root->bp = tb->bp;

	/*
	 * Now that we have a completely empty fs, give it
	 * a single snap block that the tree will insert
	 * into, and take a snapshot as the initial state.
	 */
	if((rb = newblk(Tleaf)) == nil)
		sysfatal("ream: allocate snaps: %r");
	holdblk(rb);
	initsnap(rb, tb);
	finalize(rb);
	syncblk(rb);

	fs->snap.bp = rb->bp;
	fs->snap.ht = 1;

	dropblk(tb);
	dropblk(rb);
	for(i = 0; i < fs->narena; i++){
		a = &fs->arenas[i];
		finalize(a->tail);
		if(syncblk(a->tail) == -1)
			sysfatal("sync arena: %r");
		packarena(a->b->data, Blksz, a, fs);
		finalize(a->b);
		if(syncblk(a->b) == -1)
			sysfatal("sync arena: %r");
	}
	free(mnt);
}
