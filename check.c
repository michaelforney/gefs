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
	Msg mx, my;
	int i, r;
	Blk *c;
	int fail;

	fail = 0;
	if(h < 0){
		fprint(2, "node too deep (loop?\n");
		fail++;
		return fail;
	} 
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
	if(lo && keycmp(lo, &x) > 0){
		fprint(2, "out of range keys %P != %P\n", lo, &x);
		showblk(b, "wut", 1);
		fail++;
	}
	for(i = 1; i < b->nval; i++){
		getval(b, i, &y);
		if(hi && keycmp(&y, hi) >= 0){
			fprint(2, "out of range keys %P >= %P\n", &y, hi);
			fail++;
		}
		if(b->type == Tpivot){
			if(isfree(x.bp.addr)){
				fprint(2, "freed block in use: %llx\n", x.bp.addr);
				fail++;
			}
			if((c = getblk(x.bp, 0)) == nil){
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
			putblk(c);
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
	if(b->type == Tpivot){
		getval(b, b->nval-1, &y);
		if((c = getblk(x.bp, 0)) == nil){
			fprint(2, "corrupt block: %r\n");
			fail++;
		}
		if(c != nil && badblk(c, h - 1, &y, nil))
			fail++;
	}
	if(b->type == Tpivot){
		if(b->nbuf > 0){
			getmsg(b, 0, &mx);
			if(hi && keycmp(&mx, hi) >= 0){
				fprint(2, "out of range messages %P != %M\n", hi, &mx);
				fail++;
			}
		}
		for(i = 1; i < b->nbuf; i++){
			getmsg(b, i, &my);
			switch(my.op){
			case Oinsert:	/* new kvp */
			case Odelete:	/* delete kvp */
			case Oqdelete:	/* delete kvp if exists */
				break;
			case Owstat:		/* kvp dirent */
				if((my.statop & ~(Owsize|Owname|Owmode|Owmtime)) != 0){
					fprint(2, "invalid stat op %d\n", my.statop);
					fail++;
				}
				break;
			default:
				fprint(2, "invalid message op %d\n", my.op);
				fail++;
				break;
			}
			if(hi && keycmp(&y, hi) > 0){
				fprint(2, "out of range keys %P >= %P\n", &y, hi);
				fail++;
			}
			if(keycmp(&mx, &my) == 1){
				fprint(2, "misordered keys %P, %P\n", &x, &y);
				fail++;
				break;
			}
			mx = my;
		}

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
	if((b = getroot(&fs->snap, &height)) != nil){
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
	fprint(fd, "%.*s     +{%B}\n", 4*indent, spc, b->bp);
	if(b->type == Tpivot){
		for(i = 0; i < b->nbuf; i++){
			getmsg(b, i, &m);
			fprint(fd, "%.*s[%03d]|%M\n", 4*indent, spc, i, &m);
		}
	}
	for(i = 0; i < b->nval; i++){
		getval(b, i, &kv);
		fprint(fd, "%.*s[%03d]|%P\n", 4*indent, spc, i, &kv);
		if(b->type == Tpivot){
			if((c = getblk(kv.bp, 0)) == nil)
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
showfs(int fd, char *m)
{
	Blk *b;
	int h;

	fprint(fd, "=== %s %B\n", m, fs->snap.bp);
	fprint(fd, "\tht: %d\n", fs->snap.ht);
	fprint(fd, "\trt: %B\n", fs->snap.bp);
	b = getroot(&fs->snap, &h);
	rshowblk(fd, b, 0, 1);
	putblk(b);
}

void
showcache(int fd)
{
	Bucket *bkt;
	Blk *b;
	int i;

	for(i = 0; i < fs->cmax; i++){
		bkt = &fs->cache[i];
		lock(bkt);
		if(bkt->b != nil)
			fprint(fd, "bkt%d\n", i);
		for(b = bkt->b; b != nil; b = b->hnext)
			if(b->ref != 1)
				fprint(fd, "\t%p[ref=%ld, t=%d] => %B\n", b, b->ref, b->type, b->bp);
		unlock(bkt);
	}
}

void
showpath(Path *p, int np)
{
	int i;
	char *op[] = {
	[POmod] = "POmod",
	[POrot] = "POrot",
	[POsplit] = "POsplit",
	[POmerge] = "POmerge",
	};

	print("path:\n");
#define A(b) (b ? b->bp.addr : -1)
	for(i = 0; i < np; i++){
		print("\t[%d] ==>\n"
			"\t\t%s: b(%p)=%llx [%s]\n"
			"\t\tnl(%p)=%llx, nr(%p)=%llx\n"
			"\t\tidx=%d, midx=%d\n"
			"\t\tpullsz=%d, npull=%d, \n"
			"\t\tclear=(%d. %d)\n",
			i, op[p[i].op],
			p[i].b, A(p[i].b), (p[i].b == nil) ? "nil" : (p[i].b->type == Tleaf ? "leaf" : "pivot"),
			p[i].nl, A(p[i].nl),
			p[i].nr, A(p[i].nr),
			p[i].idx, p[i].midx,
			p[i].pullsz, p[i].npull,
			p[i].lo, p[i].hi);
	}
}

void
showfree(int fd, char *m)
{
	Arange *r;
	int i;

	print("=== %s\n", m);
	for(i = 0; i < fs->narena; i++){
		fprint(fd, "arena %d:\n", i);
		for(r = (Arange*)avlmin(fs->arenas[i].free); r != nil; r = (Arange*)avlnext(r))
			fprint(fd, "\t%llx+%llx\n", r->off, r->len);
	}
}
