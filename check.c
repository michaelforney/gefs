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
badblk(int fd, Blk *b, int h, Kvp *lo, Kvp *hi)
{
	Kvp x, y;
	Msg mx, my;
	int i, r;
	Blk *c;
	int fail;

	fail = 0;
	if(h < 0){
		fprint(fd, "node too deep (loop?\n");
		fail++;
		return fail;
	} 
	if(b->type == Tleaf){
		if(h != 0){
			fprint(fd, "unbalanced leaf\n");
			fail++;
		}
		if(h != 0 && b->nval < 2){
			fprint(fd, "underfilled leaf\n");
			fail++;
		}
	}
	if(b->type == Tpivot && b->nval < 2){
		fprint(fd, "underfilled  pivot\n");
		fail++;
	}
	getval(b, 0, &x);
	if(lo && keycmp(lo, &x) > 0){
		fprint(fd, "out of range keys %P != %P\n", lo, &x);
		showblk(2, b, "wut", 1);
		fail++;
	}
	for(i = 1; i < b->nval; i++){
		getval(b, i, &y);
		if(hi && keycmp(&y, hi) >= 0){
			fprint(fd, "out of range keys %P >= %P\n", &y, hi);
			fail++;
		}
		if(b->type == Tpivot){
			if(isfree(x.bp.addr)){
				fprint(fd, "freed block in use: %llx\n", x.bp.addr);
				fail++;
			}
			if((c = getblk(x.bp, 0)) == nil){
				fprint(fd, "corrupt block: %r\n");
				fail++;
				continue;
			}
			if(blkfill(c) != x.fill){
				fprint(fd, "mismatched block fill\n");
				fail++;
			}
			if(badblk(fd, c, h - 1, &x, &y))
				fail++;
			putblk(c);
		}
		r = keycmp(&x, &y);
		switch(r){
		case -1:
			break;
		case 0:
			fprint(fd, "duplicate keys %P, %P\n", &x, &y);
			fail++;
			break;
		case 1:
			fprint(fd, "misordered keys %P, %P\n", &x, &y);
			fail++;
			break;
		}
		x = y;
	}
	if(b->type == Tpivot){
		getval(b, b->nval-1, &y);
		if((c = getblk(x.bp, 0)) == nil){
			fprint(fd, "corrupt block: %r\n");
			fail++;
		}
		if(c != nil && badblk(fd, c, h - 1, &y, nil))
			fail++;
	}
	if(b->type == Tpivot){
		if(b->nbuf > 0){
			getmsg(b, 0, &mx);
			if(hi && keycmp(&mx, hi) >= 0){
				fprint(fd, "out of range messages %P != %M\n", hi, &mx);
				fail++;
			}
		}
		for(i = 1; i < b->nbuf; i++){
			getmsg(b, i, &my);
			switch(my.op){
			case Oinsert:	/* new kvp */
			case Odelete:	/* delete kvp */
			case Oclearb:	/* delete kvp if exists */
				break;
			case Owstat:		/* kvp dirent */
				if((my.statop & ~(Owsize|Owmode|Owmtime)) != 0){
					fprint(2, "invalid stat op %d\n", my.statop);
					fail++;
				}
				break;
			default:
				fprint(fd, "invalid message op %d\n", my.op);
				fail++;
				break;
			}
			if(hi && keycmp(&y, hi) > 0){
				fprint(fd, "out of range keys %P >= %P\n", &y, hi);
				fail++;
			}
			if(keycmp(&mx, &my) == 1){
				fprint(fd, "misordered keys %P, %P\n", &x, &y);
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

int
checkfs(int fd)
{
	int ok, height;
	Blk *b;

	ok = 1;
	if(badfree())
		ok = 0;
	if((b = getroot(&fs->snap, &height)) != nil){
		if(badblk(fd, b, height-1, nil, 0))
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
showblk(int fd, Blk *b, char *m, int recurse)
{
	fprint(fd, "=== %s\n", m);
	rshowblk(fd, b, 0, recurse);
}

void
showtree(int fd, Tree *t, char *m)
{
	Blk *b;
	int h;

	fprint(fd, "=== [%s] %B\n", m, fs->snap.bp);
	fprint(fd, "\tht: %d\n", fs->snap.ht);
	fprint(fd, "\trt: %B\n", fs->snap.bp);
	b = getroot(t, &h);
	rshowblk(fd, b, 0, 1);
	putblk(b);
}

void
showfs(int fd, char **ap, int na)
{
	char *p, *e, *name, kbuf[Kvmax], kvbuf[Kvmax];;
	Tree t;
	Key k;
	Kvp kv;

	name = (na == 0) ? "main" : ap[0];
	k.k = kbuf;
	k.k[0] = Ksnap;
	k.nk = 1+snprint(k.k+1, sizeof(kbuf)-1, "%s", name);
	if((e = btlookup(&fs->snap, &k, &kv, kvbuf, sizeof(kvbuf))) != nil){
		fprint(fd, "lookup %K: %s\n", &k, e);
		return;
	}
	if(kv.nv != Rootsz+Ptrsz){
		fprint(fd, "bad snap %P\n", &kv);
		return;
	}

	p = kv.v;
	t.ht = GBIT32(p); p += 4;
	t.bp.addr = GBIT64(p); p += 8;
	t.bp.hash = GBIT64(p); p += 8;
	t.bp.gen = GBIT64(p);
	showtree(fd, &t, name);
}

void
showsnap(int fd, char **, int)
{
	showtree(fd, &fs->snap, "snaps");
}

void
showcache(int fd, char**, int)
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
showpath(int fd, Path *p, int np)
{
#define A(b) (b ? b->bp.addr : -1)
	int i;
	char *op[] = {
	[POmod] = "POmod",
	[POrot] = "POrot",
	[POsplit] = "POsplit",
	[POmerge] = "POmerge",
	};

	fprint(fd, "path:\n");
	for(i = 0; i < np; i++){
		fprint(fd, "\t[%d] ==>\n"
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
showfree(int fd, char **, int)
{
	Arange *r;
	int i;

	for(i = 0; i < fs->narena; i++){
		fprint(fd, "arena %d:\n", i);
		for(r = (Arange*)avlmin(fs->arenas[i].free); r != nil; r = (Arange*)avlnext(r))
			fprint(fd, "\t%llx+%llx\n", r->off, r->len);
	}
}
