#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"

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
	int i, r, fill;
	Blk *c;
	int fail;
	Bptr bp;

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
		showblk(2, b, "out of range", 1);
		fail++;
	}
	for(i = 1; i < b->nval; i++){
		getval(b, i, &y);
		if(hi && keycmp(&y, hi) >= 0){
			fprint(fd, "out of range keys %P >= %P\n", &y, hi);
			fail++;
		}
		if(b->type == Tpivot){
			bp = getptr(&x, &fill);
			if(isfree(bp.addr)){
				fprint(fd, "freed block in use: %llx\n", bp.addr);
				fail++;
			}
			if((c = getblk(bp, 0)) == nil){
				fprint(fd, "corrupt block: %r\n");
				fail++;
				continue;
			}
			if(blkfill(c) != fill){
				fprint(fd, "mismatched block fill\n");
				fail++;
			}
			if(badblk(fd, c, h - 1, &x, &y))
				fail++;
			dropblk(c);
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
		bp = getptr(&x, &fill);
		if((c = getblk(bp, 0)) == nil){
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
				if((my.v[0] & ~(Owsize|Owmode|Owmtime)) != 0){
					fprint(2, "invalid stat op %x\n", my.v[0]);
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
		dropblk(b);
	}
	return ok;
}
