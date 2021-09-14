#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"

char	spc[128];

static int
isfree(vlong bp)
{
	Arange *r, q;
	Arena *a;

	q.off = bp;
	q.len = Blksz;

	a = getarena(bp);
	r = (Arange*)avllookup(a->free, &q, -1);
	if(r == nil)
		return 0;
	return bp < (r->off + r->len);
}

static int
badblk(Blk *b, int h, Kvp *lo, Kvp *hi)
{
	Kvp x, y;
	int i, r;
	Blk *c;
	int fail;

	fail = 0;
	if(b->type == Tleaf){
		if(h != 0){
			fprint(2, "unbalanced leaf\n");
			fail++;
		}
		if(h != 0 && b->nval < 2){
			fprint(2, "underfilled leaf\n");
			fail++;
		}
	}
	if(b->type == Tpivot && b->nval < 2){
		fprint(2, "underfilled  pivot\n");
		fail++;
	}
	getval(b, 0, &x);
	if(lo && keycmp(lo, &x) != 0)
		fprint(2, "out of range keys %P != %P\n", lo, &x);
	for(i = 1; i < b->nval; i++){
		getval(b, i, &y);
		if(hi && keycmp(&y, hi) >= 0){
			fprint(2, "out of range keys %P >= %P\n", &y, hi);
			fail++;
		}
		if(b->type == Tpivot){
			if(isfree(x.bp)){
				fprint(2, "freed block in use: %llx\n", x.bp);
				fail++;
			}
			if((c = getblk(x.bp, x.bh)) == nil){
				fprint(2, "corrupt block: %r\n");
				fail++;
				continue;
			}
			if(blkfill(c) != x.fill){
				fprint(2, "mismatched block fill\n");
				fail++;
			}
			if(badblk(c, h - 1, &x, &y))
				fail++;
		}
		r = keycmp(&x, &y);
		switch(r){
		case -1:
			break;
		case 0:
			fprint(2, "duplicate keys %P, %P\n", &x, &y);
			fail++;
			break;
		case 1:
			fprint(2, "misordered keys %P, %P\n", &x, &y);
			fail++;
			break;
		}
		x = y;
	}
	return fail;
}

static int
badfree(void)
{
	Arange *r, *n;
	int i, fail;

	fail = 0;
	for(i = 0; i < fs->narena; i++){
		r = (Arange*)avlmin(fs->arenas[i].free);
		for(n = (Arange*)avlnext(r); n != nil; n = (Arange*)avlnext(n)){
			if(r->off >= n->off){
				fprint(2, "misordered length %llx >= %llx\n", r->off, n->off);
				fail++;
			}
			if(r->off+r->len >= n->off){
				fprint(2, "overlaping range %llx+%llx >= %llx\n", r->off, r->len, n->off);
				abort();
				fail++;
			}
			r = n;
		}
	}
	return fail;
}

/* TODO: this will grow into fsck. */
int
checkfs(void)
{
	int ok, height;
	Blk *b;

	ok = 1;
	if(badfree())
		ok = 0;
	if((b = getroot(&height)) != nil){
		if(badblk(b, height-1, nil, 0))
			ok = 0;
		putblk(b);
	}
	return ok;
}

void
rshowblk(int fd, Blk *b, int indent, int recurse)
{
	Blk *c;
	int i;
	Kvp kv;
	Msg m;

	if(indent > sizeof(spc)/4)
		indent = sizeof(spc)/4;
	if(b == nil){
		fprint(fd, "NIL\n");
		return;
	}
	if(b->type == Tpivot){
		for(i = 0; i < b->nbuf; i++){
			getmsg(b, i, &m);
			fprint(fd, "%.*s|%M\n", 4*indent, spc, &m);
		}
	}
	for(i = 0; i < b->nval; i++){
		getval(b, i, &kv);
		fprint(fd, "%.*s|%P\n", 4*indent, spc, &kv);
		if(b->type == Tpivot){
			if((c = getblk(kv.bp, kv.bh)) == nil)
				sysfatal("failed load: %r");
			if(recurse)
				rshowblk(fd, c, indent + 1, 1);
			putblk(c);
		}
	}
}


void
initshow(void)
{
	int i;

	memset(spc, ' ', sizeof(spc));
	for(i = 0; i < sizeof(spc); i += 4)
		spc[i] = '|';
}

void
showblk(Blk *b, char *m, int recurse)
{
	fprint(2, "=== %s\n", m);
	rshowblk(2, b, 0, recurse);
}

void
fshowfs(int fd, char *m)
{
	int h;

	fprint(fd, "=== %s\n", m);
	fprint(fd, "fs->height: %d\n", fs->height);
	rshowblk(fd, getroot(&h), 0, 1);
}

void
showfs(char *m)
{
	if(debug)
		fshowfs(2, m);
}

void
showpath(Path *p, int np)
{
	int i;

	for(i = 0; i < np; i++){
		print("==> b=%p, n=%p, idx=%d\n", p[i].b, p[i].n, p[i].idx);
		print("\tclear=(%d, %d):%d\n", p[i].lo, p[i].hi, p[i].sz);
		print("\tl=%p, r=%p, n=%p, split=%d\n", p[i].l, p[i].r, p[i].n, p[i].split);
	}
}

void
showfree(char *m)
{
	Arange *r;
	int i;

	if(!debug)
		return;
	print("=== %s\n", m);
	for(i = 0; i < fs->narena; i++){
		print("arena %d:\n", i);
		for(r = (Arange*)avlmin(fs->arenas[i].free); r != nil; r = (Arange*)avlnext(r))
			print("\t%llx+%llx\n", r->off, r->len);
	}
}
