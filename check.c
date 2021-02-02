#include <u.h>
#include <libc.h>
#include <bio.h>
#include "dat.h"
#include "fns.h"

char	spc[128];
int	debug;

static int
invalidblk(Blk *b, int h, Kvp *lo, Kvp *hi)
{
	Kvp x, y;
	int i, r;
	Blk *c;
	int fail;

	fail = 0;
	if(b->type == Leaf){
		if(h != fs->height){
			fprint(2, "unbalanced leaf\n");
			fail++;
		}
		if(h != 1 && b->nent < 2){
			fprint(2, "underfilled leaf\n");
			fail++;
		}
	}
	if(b->type == Pivot && b->nent < 2){
		fprint(2, "underfilled  pivot\n");
		fail++;
	}
	getval(b, 0, &x);
	if(lo && keycmp(lo, &x) != 0)
		fprint(2, "out of range keys %P != %P\n", lo, &x);
	for(i = 1; i < b->nent; i++){
		getval(b, i, &y);
		if(hi && keycmp(&y, hi) >= 0){
			fprint(2, "out of range keys %P >= %P\n", &y, hi);
			fail++;
		}
		if(b->type == Pivot){
			c = getblk(x.bp, x.bh);
			if(blkfill(c) != x.fill){
				fprint(2, "mismatched block fill");
				fail++;
			}
			if(invalidblk(c, h + 1, &x, &y))
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
	

/* TODO: this will grow into fsck. */
int
checkfs(void)
{
	return invalidblk(fs->root, 1, nil, nil) == 0;
}

static void
rshowblk(Blk *b, int indent)
{
	Blk *c;
	int i;
	Kvp kv;
	Msg m;

	if(indent > sizeof(spc)/4)
		indent = sizeof(spc)/4;
	if(b == nil){
		print("NIL\n");
		return;
	}
	if(b->type == Pivot){
		for(i = 0; i < b->nmsg; i++){
			getmsg(b, i, &m);
			print("%.*s|%M\n", 4*indent, spc, &m);
		}
	}
	for(i = 0; i < b->nent; i++){
		getval(b, i, &kv);
		print("%.*s|%P\n", 4*indent, spc, &kv);
		if(b->type == Pivot){
			if((c = getblk(kv.bp, kv.bh)) == nil)
				sysfatal("falied load: %r");
			rshowblk(c, indent + 1);
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
showblk(Blk *b, char *m)
{
	if(m != nil)
		print("=== %s\n", m);
	rshowblk(b, 0);
}

void
showfs(char *m)
{
	if(m != nil)
		print("=== %s\n", m);
	rshowblk(fs->root, 0);
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
