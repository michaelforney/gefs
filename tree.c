#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"

void
cpkey(Key *dst, Key *src, char *buf, int nbuf)
{
	assert(src->nk <= nbuf);
	memcpy(buf, src->k, src->nk);
	dst->k = buf;
	dst->nk = src->nk;
}

void
cpkvp(Kvp *dst, Kvp *src, char *buf, int nbuf)
{
	assert(src->nk+src->nv <= nbuf);
	memcpy(buf, src->k, src->nk);
	memcpy(buf+ src->nk, src->v, src->nv);
	dst->k = buf;
	dst->nk = src->nk;
	dst->v = buf+src->nk;
	dst->nv = src->nv;
}

void
cpmsg(Msg *dst, Msg *src, char *buf, int nbuf)
{
	dst->op = src->op;
	cpkvp(dst, src, buf, nbuf);
}

int
keycmp(Key *a, Key *b)
{
	int c, n;

	n = (a->nk < b->nk) ? a->nk : b->nk;
	if((c = memcmp(a->k, b->k, n)) != 0)
		return c < 0 ? -1 : 1;
	if(a->nk < b->nk)
		return -1;
	else if(a->nk > b->nk)
		return 1;
	else
		return 0;
}

int
msgsz(Msg *m)
{
	/* disp + op + klen + key + vlen + v */
	return 2+1+2+m->nk +2+ m->nv;
}

int
valsz(Kvp *kv)
{
	return 2 + 2+kv->nk + 2+kv->nv;
}

void
getval(Blk *b, int i, Kvp *kv)
{
	int o;

	assert(i >= 0 && i < b->nval);
	o = GBIT16(b->data + 2*i);
	kv->nk = GBIT16(b->data + o);
	kv->k = b->data + o + 2;
	kv->nv = GBIT16(kv->k + kv->nk);
	kv->v = kv->k + kv->nk + 2;
}

Bptr
getptr(Kvp *kv, int *fill)
{
	assert(kv->nv == Ptrsz || kv->nv == Ptrsz+2);
	if(fill != nil)
		*fill = GBIT16(kv->v + Ptrsz);
	return unpackbp(kv->v, kv->nv);
}

void
setval(Blk *b, int i, Kvp *kv)
{
	int o, nk, nv, spc;
	char *p;

	assert(i == b->nval);
	spc = (b->type == Tleaf) ? Leafspc : Pivspc;
	p = b->data + 2*i;
	nk = 2 + kv->nk;
	nv = 2 + kv->nv;
	memmove(p + 2, p, 2*(b->nval - i));
	b->nval++;
	b->valsz += nk + nv;
	o = spc - b->valsz;

	if(2*b->nval + b->valsz > spc){
		dprint("2*%d + %d > %d [ksz: %d, vsz: %d]\n",
			2*b->nval, b->valsz, spc, kv->nk, kv->nv);
		showblk(2, b, "setval overflow", 1);
		abort();
	}
	assert(2*b->nval + b->valsz <= spc);
	assert(2*b->nval <= o);
	p = b->data + o;
	PBIT16(b->data + 2*i, o);
	PBIT16(p +  0, kv->nk);
	memcpy(p +  2, kv->k, kv->nk);
	PBIT16(p + kv->nk + 2, kv->nv);
	memcpy(p + kv->nk + 4, kv->v, kv->nv);
}

void
setptr(Blk *b, int i, Key *k, Bptr bp, int fill)
{
	char *p, buf[Ptrsz+2];
	Kvp kv;

	kv.k = k->k;
	kv.nk = k->nk;
	kv.v = buf;
	kv.nv = sizeof(buf);
	p = packbp(buf, sizeof(buf), &bp);
	PBIT16(p, fill);
	setval(b, i, &kv);
}

void
setmsg(Blk *b, int i, Msg *m)
{
	char *p;
	int o;

	assert(b->type == Tpivot);
	assert(i >= 0 && i <= b->nbuf);
	b->nbuf++;
	b->bufsz += msgsz(m)-2;
	assert(2*b->nbuf + b->bufsz <= Bufspc);
	assert(m->op >= 0 && m->op < Nmsgtype);

	p = b->data + Pivspc;
	o = Pivspc - b->bufsz;
	memmove(p + 2*(i+1), p+2*i, 2*(b->nbuf - i));
	PBIT16(p + 2*i, o);

	p = b->data + Bufspc + o;
	*p = m->op;
	PBIT16(p + 1, m->nk);
	memcpy(p + 3, m->k, m->nk);
	PBIT16(p + 3 + m->nk, m->nv);
	memcpy(p + 5 + m->nk, m->v, m->nv);
}

void
getmsg(Blk *b, int i, Msg *m)
{
	char *p;
	int o;

	assert(b->type == Tpivot);
	assert(i >= 0 && i < b->nbuf);
	o = GBIT16(b->data + Pivspc + 2*i);
	p = b->data + Pivspc + o;
	m->op = *p;
	m->nk = GBIT16(p + 1);
	m->k = p + 3;
	m->nv = GBIT16(p + 3 + m->nk);
	m->v = p + 5 + m->nk;
}

static int
bufsearch(Blk *b, Key *k, Msg *m, int *same)
{
	int lo, hi, mid, r;
	Msg cmp;

	r = -1;
	lo = 0;
	hi = b->nbuf;
	while(lo < hi){
		mid = (hi + lo) / 2;
		getmsg(b, mid, &cmp);
		r = keycmp(k, &cmp);
		if(r < 0)
			hi = mid;
		else
			lo = mid + 1;
	}
	/*
	 * we can have duplicate messages, and we
	 * want to point to the first of them:
	 * scan backwards.
	 */
	lo--;
	for(; lo > 0; lo--){
		getmsg(b, lo-1, &cmp);
		if(keycmp(k, &cmp) != 0)
			break;
	}
	if(lo >= 0){
		getmsg(b, lo, m);
		r = keycmp(k, m);
	}
	if(same != nil)
		*same = (r == 0);
	return lo;
}

int
blksearch(Blk *b, Key *k, Kvp *rp, int *same)
{
	int lo, hi, mid, r;
	Kvp cmp;

	r = -1;
	lo = 0;
	hi = b->nval;
	while(lo < hi){
		mid = (hi + lo) / 2;
		getval(b, mid, &cmp);
		r = keycmp(k, &cmp);
		if(r < 0)
			hi = mid;
		else
			lo = mid + 1;
	}
	lo--;
	if(lo >= 0){
		getval(b, lo, rp);
		r = keycmp(k, rp);
	}
	if(same != nil)
		*same = (r == 0);
	return lo;
}

int
buffill(Blk *b)
{
	assert(b->type == Tpivot);
	return 2*b->nbuf + b->bufsz;
}

int
filledbuf(Blk *b, int nmsg, int needed)
{
	assert(b->type == Tpivot);
	return 2*(b->nbuf+nmsg) + b->bufsz + needed > Bufspc;
}

int
filledleaf(Blk *b, int needed)
{
	assert(b->type == Tleaf);
	return 2*(b->nval+1) + b->valsz + needed > Leafspc;
}

int
filledpiv(Blk *b, int reserve)
{
	/* 
	 * We need to guarantee there's room for one message
	 * at all times, so that splits along the whole path
	 * have somewhere to go as they propagate up.
	 */
	assert(b->type == Tpivot);
	return 2*(b->nval+1) + b->valsz + reserve*Kpmax > Pivspc;
}

int
copyup(Blk *n, int i, Path *pp, int *nbytes)
{
	Kvp kv;
	Msg m;

	/*
	 * It's possible for the previous node to have
	 * been fully cleared out by a large number of
	 * delete messages, so we need to check if
	 * there's anything in it to copy up.
	 */
	if(pp->nl->nval > 0){
		getval(pp->nl, 0, &kv);
		if(pp->nl->nbuf > 0){
			getmsg(pp->nl, 0, &m);
			if(keycmp(&kv, &m) > 0)
				kv.Key = m.Key;
		}
		setptr(n, i++, &kv, pp->nl->bp, blkfill(pp->nl));
		if(nbytes != nil)
			*nbytes += valsz(&kv);
	}
	if(pp->nr != nil && pp->nr->nval > 0){
		getval(pp->nr, 0, &kv);
		if(pp->nr->nbuf > 0){
			getmsg(pp->nr, 0, &m);
			if(keycmp(&kv, &m) > 0)
				kv.Key = m.Key;
		}
		setptr(n, i++, &kv, pp->nr->bp, blkfill(pp->nr));
		if(nbytes != nil)
			*nbytes += valsz(&kv);
	}
	return i;
}

void
statupdate(Kvp *kv, Msg *m)
{
	int op;
	char *p;
	Xdir d;

	p = m->v;
	op = *p++;
	kv2dir(kv, &d);
	/* bump version */
	d.qid.vers++;
	if(op & Owsize){
		d.length = GBIT64(p);
		p += 8;
	}
	if(op & Owmode){
		d.mode = GBIT32(p);
		p += 4;
	}
	if(op & Owmtime){
		d.mtime = GBIT64(p);
		p += 8;
	}
	if(op & Owatime){
		d.atime = GBIT64(p);
		p += 8;
	}
	if(op & Owuid){
		d.uid = GBIT32(p);
		p += 4;
	}
	if(op & Owgid){
		d.gid = GBIT32(p);
		p += 4;
	}
	if(op & Owmuid){
		d.muid = GBIT32(p);
		p += 4;
	}
	if(p != m->v + m->nv){
		fprint(2, "kv=%P, m=%M\n", kv, m);
		fprint(2, "malformed stat message (op=%x, len=%lld, sz=%d)\n", op, p - m->v, m->nv);
		abort();
	}
	if(packdval(kv->v, kv->nv, &d) == nil){
		fprint(2, "repacking dir failed");
		abort();
	}
}

static char*
dropsnap(Kvp *t, Msg *m)
{
	char *e, buf[Msgmax];
	Tree snap, tmp, *from;
	vlong id;
	Kvp kv;
	Key k;

	if(unpacktree(&snap, t->v, t->nv) == nil)
		return Efs;
	k.k = m->v;
	k.nk = m->nv;
	id = GBIT64(m->v+1);
	for(from = fs->osnap; from != nil; from = from->snext)
		if(from->gen == id)
			break;
	if(from == nil){
		if((e = btlookup(&fs->snap, &k, &kv, buf, sizeof(buf))) != nil)
			return e;
		if(unpacktree(&tmp, kv.v, kv.nv) == nil)
			return Efs;
		from = &tmp;
	}
	if((e = freesnap(&snap, from)) != nil)
		return e;
	return nil;
}

int
apply(Kvp *kv, Msg *m, char *buf, int nbuf)
{
	int refs;

	switch(m->op){
	case Oclearb:
	case Odelete:
		assert(keycmp(kv, m) == 0);
		return 0;
	case Oinsert:
		cpkvp(kv, m, buf, nbuf);
		return 1;
	case Owstat:
		assert(keycmp(kv, m) == 0);
		statupdate(kv, m);
		return 1;
	case Orefsnap:
		assert(keycmp(kv, m) == 0);
		refs = GBIT32(kv->v) + 1;
		PBIT32(kv->v, refs);
		return 1;
	case Ounrefsnap:
		assert(keycmp(kv, m) == 0);
		refs = GBIT32(kv->v) - 1;
		if(refs == 0){
			dropsnap(kv, m);
			return 0;
		}
		PBIT32(kv->v, refs);
		return 1;
	default:
		abort();
	}
	return 0;
}

int
pullmsg(Path *p, int i, Kvp *v, Msg *m, int *full, int spc)
{
	if(i < 0 || i >= p->hi || *full)
		return -1;

	if(p->ins != nil)
		*m = p->ins[i];
	else
		getmsg(p->b, i, m);
	if(msgsz(m) <= spc)
		return (v == nil) ? 0 : keycmp(v, m);
	*full = 1;
	return -1;
}

/*
 * Creates a new block with the contents of the old
 * block. When copying the contents, it repacks them
 * to minimize the space uses, and applies the changes
 * pending from the downpath blocks.
 *
 * When pidx != -1, 
 */
int
updateleaf(Tree *t, Path *up, Path *p)
{
	char buf[Msgmax];
	int i, j, o, ok, full, spc;
	Blk *b, *n;
	Bptr bp;
	Msg m;
	Kvp v;

	i = 0;
	o = 0;
	j = up->lo;
	b = p->b;
	/*
	 * spc is the amount of room we have
	 * to copy data down from the parent; it's
	 * necessarily a bit conservative, because
	 * deletion messages don't take space -- but
	 * we don't know how what the types of all
	 * messages are.
	 */
	full = 0;
	spc = Blkspc - blkfill(b);
	if((n = newblk(b->type)) == nil)
		return -1;
	while(i < b->nval){
		ok = 1;
		getval(p->b, i, &v);
		switch(pullmsg(up, j, &v, &m, &full, spc)){
		case -1:
			setval(n, o++, &v);
			i++;
			break;
		case 1:
			/*
			 * new values must always start as
			 * an insertion, mutations come after.
			 */
			if(m.op != Oinsert){
				print("%d(/%d), %d: %M not insert\n", i, b->nval, j, &m);
				abort();
			}
			cpkvp(&v, &m, buf, sizeof(buf));
			spc -= valsz(&m);
			goto Copy;
		case 0:
			i++;
			while(j < up->hi){
				if(m.op == Oclearb){
					bp = unpackbp(v.v, v.nv);
					freebp(t, bp);
				}
				ok = apply(&v, &m, buf, sizeof(buf));
		Copy:
				j++;
				p->pullsz += msgsz(&m);
				if(j >= up->hi || pullmsg(up, j, &v, &m, &full, spc) != 0)
					break;
			}
			if(ok)
				setval(n, o++, &v);
			break;
		}
	}
	while(j < up->hi) {
		/* can't fail */
		pullmsg(up, j++, nil, &m, &full, spc);
		ok = 1;
		cpkvp(&v, &m, buf, sizeof(buf));
		p->pullsz += msgsz(&m);
		if(m.op != Oinsert){
			print("%d(/%d), %d: %M not insert\n", i, b->nval, j, &m);
			showblk(2, up->b, "parent", 0);
			showblk(2, p->b, "current", 0);
			abort();
		}
		while(pullmsg(up, j, &v, &m, &full, spc) == 0){
			ok = apply(&v, &m, buf, sizeof(buf));
			p->pullsz += msgsz(&m);
			j++;
		}
		if(ok)
			setval(n, o++, &v);
	}
	p->npull = (j - up->lo);
	p->nl = n;
	return 0;
}

/*
 * Creates a new block with the contents of the old
 * block. When copying the contents, it repacks them
 * to minimize the space uses, and applies the changes
 * pending from the downpath blocks.
 *
 * When pidx != -1, 
 */
int
updatepiv(Tree *, Path *up, Path *p, Path *pp)
{
	char buf[Msgmax];
	int i, j, o, sz, full, spc;
	Blk *b, *n;
	Msg m, u;

	o = 0;
	b = p->b;
	if((n = newblk(b->type)) == nil)
		return -1;
	for(i = 0; i < b->nval; i++){
		if(pp != nil && i == p->midx){
			o = copyup(n, o, pp, nil);
			if(pp->op == POrot || pp->op == POmerge)
				i++;
		}else{
			getval(b, i, &m);
			setval(n, o++, &m);
		}
	}
	o = 0;
	i = 0;
	j = up->lo;
	sz = 0;
	full = 0;
	spc = Bufspc - buffill(b);
	if(pp != nil)
		spc += pp->pullsz;
	while(i < b->nbuf){
		if(i == p->lo)
			i += pp->npull;
		if(i == b->nbuf)
			break;
		getmsg(b, i, &m);
		switch(pullmsg(up, j, &m, &u, &full, spc - sz)){
		case -1:
		case 0:
			setmsg(n, o++, &m);
			i++;
			break;
		case 1:
			cpkvp(&m, &u, buf, sizeof(buf));
			while(pullmsg(up, j, &m, &u, &full, spc) == 0){
				setmsg(n, o++, &u);
				sz = msgsz(&u);
				p->pullsz += sz;
				spc -= sz;
				j++;
			}
		}
	}
	while(j < up->hi){
		pullmsg(up, j, nil, &u, &full, spc);
		if(full)
			break;
		setmsg(n, o++, &u);
		sz = msgsz(&u);
		p->pullsz += sz;
		spc -= sz;
		j++;
	}
	p->npull = (j - up->lo);
	p->nl = n;
	return 0;
}

/*
 * Splits a node, returning the block that msg
 * would be inserted into. Split must never
 * grow the total height of the 
 */
int
splitleaf(Tree *t, Path *up, Path *p, Kvp *mid)
{
	char buf[Msgmax];
	int full, copied, spc, ok, halfsz;
	int i, j, o, c;
	Blk *b, *d, *l, *r;
	Msg m;
	Kvp v;

	/*
	 * If the block one entry up the
	 * p is nil, we're at the root,
	 * so we want to make a new block.
	 */
	b = p->b;
	l = newblk(b->type);
	r = newblk(b->type);
	if(l == nil || r == nil){
		freeblk(t, l);
		freeblk(t, r);
		return -1;
	}

	d = l;
	o = 0;
	i = 0;
	j = up->lo;
	full = 0;
	copied = 0;
	halfsz = (2*b->nval + b->valsz + up->sz) / 2;
	if(halfsz > Blkspc/2)
		halfsz = Blkspc/2;
	spc = Blkspc - (halfsz + Msgmax);
	assert(b->nval >= 4);
	while(i < b->nval){
		/*
		 * We're trying to balance size,
		 * but we need at least 2 nodes
		 * in each half of the split if
		 * we want a valid tree.
		 */
		if(d == l)
		if((i == b->nval-2) || (i >= 2 && copied >= halfsz)){
			o = 0;
			d = r;
			full = 0;
			spc = Blkspc - (halfsz + Msgmax);
			getval(b, i, mid);
		}
		ok = 1;
		getval(b, i, &v);
 		c = pullmsg(up, j, &v, &m, &full, spc);
		switch(c){
		case -1:
			setval(d, o++, &v);
			copied += valsz(&v);
			i++;
			break;
		case 1:
			/*
			 * new values must always start as
			 * an insertion, mutations come after.
			 */
			if(m.op != Oinsert){
				print("%d(/%d), %d: %M not insert\n", i, b->nval, j, &m);
				abort();
			}
			cpkvp(&v, &m, buf, sizeof(buf));
			spc -= valsz(&m);
			goto Copy;
		case 0:
			i++;
			while(j < up->hi){
				ok = apply(&v, &m, buf, sizeof(buf));
		Copy:
				p->pullsz += msgsz(&m);
				j++;
				if(j == up->hi || pullmsg(up, j, &v, &m, &full, spc) != 0)
					break;
			}
			if(ok)
				setval(d, o++, &v);
			break;
		}
	}
	p->npull = (j - up->lo);
	p->op = POsplit;
	p->nl = l;
	p->nr = r;

	return 0;
}

/*
 * Splits a node, returning the block that msg
 * would be inserted into. Split must never
 * grow the total height of the tree by more
 * than one.
 */
int
splitpiv(Tree *t, Path *, Path *p, Path *pp, Kvp *mid)
{
	int i, o, copied, halfsz;
	Blk *b, *d, *l, *r;
	Kvp tk;
	Msg m;

	/*
	 * If the bp->lock one entry up the
	 * p is nil, we're at the root,
	 * so we want to make a new bp->lock.
	 */
	b = p->b;
	l = newblk(b->type);
	r = newblk(b->type);
	if(l == nil || r == nil){
		freeblk(t, l);
		freeblk(t, r);
		return -1;
	}
	o = 0;
	d = l;
	copied = 0;
	halfsz = (2*b->nval + b->valsz)/2;
	assert(b->nval >= 4);
	for(i = 0; i < b->nval; i++){
		/*
		 * We're trying to balance size,
		 * but we need at least 2 nodes
		 * in each half of the split if
		 * we want a valid tree.
		 */
		if(d == l)
		if((i == b->nval-2) || (i >= 2 && copied >= halfsz)){
			o = 0;
			d = r;
			getval(b, i, mid);
		}
		if(i == p->idx){
			o = copyup(d, o, pp, &copied);
			continue;
		}
		getval(b, i, &tk);
		setval(d, o++, &tk);
		copied += valsz(&tk);
	}
	o = 0;
	d = l;
	for(i = 0; i < b->nbuf; i++){
		if(i == p->lo)
			i += pp->npull;
		if(i == b->nbuf)
			break;
		getmsg(b, i, &m);
		if(d == l && keycmp(&m, mid) >= 0){
			o = 0;
			d = r;
		}
		setmsg(d, o++, &m);
	}
	p->op = POsplit;
	p->nl = l;
	p->nr = r;

	return 0;
}

int
merge(Path *p, Path *pp, int idx, Blk *a, Blk *b)
{
	int i, o;
	Msg m;
	Blk *d;

	if((d = newblk(a->type)) == nil)
		return -1;
	o = 0;
	for(i = 0; i < a->nval; i++){
		getval(a, i, &m);
		setval(d, o++, &m);
	}
	for(i = 0; i < b->nval; i++){
		getval(b, i, &m);
		setval(d, o++, &m);
	}
	if(a->type == Tpivot){
		o = 0;
		for(i = 0; i < a->nbuf; i++){
			getmsg(a, i, &m);
			setmsg(d, o++, &m);
		}
		for(i = 0; i < b->nbuf; i++){
			getmsg(b, i, &m);
			setmsg(d, o++, &m);
		}
	}
	enqueue(d);
	p->midx = idx;
	pp->nl = d;
	pp->op = POmerge;
	pp->nr = nil;
	return 0;
}

/*
 * Scan a single block for the split offset;
 * returns 1 if we'd spill out of the buffer,
 * updates *idx and returns 0 otherwise.
 */
int
spillscan(Blk *d, Blk *b, Msg *m, int *idx, int o)
{
	int i, used;
	Msg n;

	used = 2*d->nbuf + d->bufsz;
	for(i = *idx; i < b->nbuf; i++){
		getmsg(b, i, &n);
		if(keycmp(m, &n) <= 0){
			*idx = i + o;
			return 0;
		}
		used += msgsz(&n);
		if(used > Bufspc)
			return 1;
	}
	*idx = b->nbuf;
	return 0;
}

/*
 * Returns whether the keys in b between
 * idx and m would spill out of the buffer
 * of d.
 */
int
spillsbuf(Blk *d, Blk *l, Blk *r, Msg *m, int *idx)
{
	if(l->type == Tleaf)
		return 0;

	if(*idx < l->nbuf && spillscan(d, l, m, idx, 0))
		return 1;
	if(*idx >= l->nbuf && spillscan(d, r, m, idx, l->nbuf))
		return 1;
	return 0;
}

int
rotate(Tree *t, Path *p, Path *pp, int midx, Blk *a, Blk *b, int halfpiv)
{
	int i, o, cp, sp, idx;
	Blk *d, *l, *r;
	Msg m;

	l = newblk(a->type);
	r = newblk(a->type);
	if(l == nil || r == nil){
		freeblk(t, l);
		freeblk(t, r);
		return -1;
	}
	o = 0;
	d = l;
	cp = 0;
	sp = -1;
	idx = 0;
	for(i = 0; i < a->nval; i++){
		getval(a, i, &m);
		if(d == l && (cp >= halfpiv || spillsbuf(d, a, b, &m, &idx))){
			sp = idx;
			d = r;
			o = 0;
		}
		setval(d, o++, &m);
		cp += valsz(&m);
	}
	for(i = 0; i < b->nval; i++){
		getval(b, i, &m);
		if(d == l && (cp >= halfpiv || spillsbuf(d, a, b, &m, &idx))){
			sp = idx;
			d = r;
			o = 0;
		}
		setval(d, o++, &m);
		cp += valsz(&m);
	}
	if(a->type == Tpivot){
		d = l;
		o = 0;
		for(i = 0; i < a->nbuf; i++){
			if(o == sp){
				d = r;
				o = 0;
			}
			getmsg(a, i, &m);
			setmsg(d, o++, &m);
		}
		for(i = 0; i < b->nbuf; i++){
			if(o == sp){
				d = r;
				o = 0;
			}
			getmsg(b, i, &m);
			setmsg(d, o++, &m);
		}
	}
	enqueue(l);
	enqueue(r);
	p->midx = midx;
	pp->op = POrot;
	pp->nl = l;
	pp->nr = r;
	return 0;
}

int
rotmerge(Tree *t, Path *p, Path *pp, int idx, Blk *a, Blk *b)
{
	int na, nb, ma, mb, imbalance;

	assert(a->type == b->type);

	na = 2*a->nval + a->valsz;
	nb = 2*b->nval + b->valsz;
	if(a->type == Tleaf){
		ma = 0;
		mb = 0;
	}else{
		ma = 2*a->nbuf + a->bufsz;
		mb = 2*b->nbuf + b->bufsz;
	}
	imbalance = na - nb;
	if(imbalance < 0)
		imbalance *= -1;
	/* works for leaf, because 0 always < Bufspc */
	if(na + nb < (Pivspc - 4*Msgmax) && ma + mb < Bufspc)
		return merge(p, pp, idx, a, b);
	else if(imbalance > 4*Msgmax)
		return rotate(t, p, pp, idx, a, b, (na + nb)/2);
	else
		return 0;
}

int
trybalance(Tree *t, Path *p, Path *pp, int idx)
{
	Blk *l, *m, *r;
	Kvp kl, kr;
	int ret, fill;
	Bptr bp;

	l = nil;
	r = nil;
	ret = -1;
	if(p->idx == -1 || pp == nil || pp->nl == nil)
		return 0;
	if(pp->op != POmod || pp->op != POmerge)
		return 0;

	m = refblk(pp->nl);
	if(idx-1 >= 0){
		getval(p->b, idx-1, &kl);
		bp = getptr(&kl, &fill);
		if(fill + blkfill(m) < Blkspc){
			if((l = getblk(bp, 0)) == nil)
				goto Out;
			if(rotmerge(t, p, pp, idx-1, l, m) == -1)
				goto Out;
			goto Done;
		}
	}
	if(idx+1 < p->b->nval){
		getval(p->b, idx+1, &kr);
		bp = getptr(&kr, &fill);
		if(fill + blkfill(m) < Blkspc){
			if((r = getblk(bp, 0)) == nil)
				goto Out;
			if(rotmerge(t, p, pp, idx, m, r) == -1)
				goto Out;
			goto Done;
		}
	}
Done:
	ret = 0;
Out:
	putblk(m);
	putblk(l);
	putblk(r);
	return ret;
}

static Blk*
flush(Tree *t, Path *path, int npath, int *redo)
{

	Path *up, *p, *pp, *rp;
	Kvp mid;

	/*
	 * The path must contain at minimum two elements:
	 * we must have 1 node we're inserting into, and
	 * an empty element at the top of the path that
	 * we put the new root into if the root gets split.
	 */
	assert(npath >= 2);
	rp = nil;
	pp = nil;
	p = &path[npath - 1];
	up = &path[npath - 2];
	*redo = 0;
	if(p->b->type == Tleaf){
		if(!filledleaf(p->b, up->sz)){
			if(updateleaf(t, p-1, p) == -1)
				goto Error;
			enqueue(p->nl);
			rp = p;
		}else{

			if(splitleaf(t, up, p, &mid) == -1)
				goto Error;
			enqueue(p->nl);
			enqueue(p->nr);
		}
		p->midx = -1;
		pp = p;
		up--;
		p--;
	}
	while(p != path){
		if(!filledpiv(p->b, 1)){
			if(trybalance(t, p, pp, p->idx) == -1)
				goto Error;
			/* If we merged the root node, break out. */
			if(up == path && pp != nil && pp->op == POmerge && p->b->nval == 2){
				rp = pp;
				goto Out;
			}
			if(updatepiv(t, up, p, pp) == -1)
				goto Error;
			enqueue(p->nl);
			rp = p;
		}else{
			if(splitpiv(t, up, p, pp, &mid) == -1)
				goto Error;
			enqueue(p->nl);
			enqueue(p->nr);
		}
		pp = p;
		up--;
		p--;
	}
	if(pp->nl != nil && pp->nr != nil){
		rp = &path[0];
		rp->nl = newblk(Tpivot);
		if(rp->nl == nil)
			goto Error;
		rp->npull = pp->npull;
		rp->pullsz = pp->pullsz;
		copyup(rp->nl, 0, pp, nil);
		enqueue(rp->nl);
	}
Out:
	*redo = (rp->npull != (path[0].hi - path[0].lo));
	return rp->nl;
Error:
	return nil;
}

void
freepath(Tree *t, Path *path, int npath)
{
	Path *p;

	for(p = path; p != path + npath; p++){
		if(p->b != nil)
			freeblk(t, p->b);
		if(p->m != nil)
			freeblk(t, p->m);
		putblk(p->b);
		putblk(p->nl);
		putblk(p->nr);
	}
}

/*
 * Select child node that with the largest message
 * segment in the current node's buffer.
 */
void
victim(Blk *b, Path *p)
{
	int i, j, lo, maxsz, cursz;
	Kvp kv;
	Msg m;

	j = 0;
	maxsz = 0;
	p->b = b;
	/* 
	 * Start at the second pivot: all values <= this
	 * go to the first node. Stop *after* the last entry,
	 * because entries >= the last entry all go into it.
	 */
	for(i = 1; i <= b->nval; i++){
		if(i < b->nval)
			getval(b, i, &kv);
		cursz = 0;
		lo = j;
		for(; j < b->nbuf; j++){
			getmsg(b, j, &m);
			if(i < b->nval && keycmp(&m, &kv) >= 0)
				break;
			/* 2 bytes for offset, plus message size in buffer */
			cursz += msgsz(&m);
		}
		if(cursz > maxsz){
			maxsz = cursz;
			p->op = POmod;
			p->lo = lo;
			p->hi = j;
			p->sz = maxsz;
			p->idx = i - 1;
			p->midx = i - 1;
			p->npull = 0;
			p->pullsz = 0;
		}
	}
}

int
msgcmp(void *a, void *b)
{
	return keycmp((Msg*)a, (Msg*)b);
}

char*
btupsert(Tree *t, Msg *msg, int nmsg)
{
	int i, npath, redo, dh, sz, height;
	Path *path;
	Blk *b, *rb;
	Kvp sep;
	Bptr bp;

	sz = 0;
	qsort(msg, nmsg, sizeof(Msg), msgcmp);
	for(i = 0; i < nmsg; i++){
		if(msg[i].nk + 2 > Keymax)
			return Efs;
		sz += msgsz(&msg[i]);
	}

Again:
	if((b = getroot(t, &height)) == nil)
		return Efs;

	/*
	 * The tree can grow in height by 1 when we
	 * split, so we allocate room for one extra
	 * node in the path.
	 */
	redo = 0;
	npath = 0;
	if((path = calloc((height + 2), sizeof(Path))) == nil)
		return Emem;
	path[npath].b = nil;
	path[npath].idx = -1;
	path[npath].midx = -1;
	npath++;

	path[0].sz = sz;
	path[0].ins = msg;
	path[0].lo = 0;
	path[0].hi = nmsg;
	while(b->type == Tpivot){
		if(!filledbuf(b, nmsg, path[npath - 1].sz))
			break;
		victim(b, &path[npath]);
		getval(b, path[npath].idx, &sep);
		bp = getptr(&sep, nil);
		b = getblk(bp, 0);
		if(b == nil)
			goto Error;
		npath++;
	}
	path[npath].b = b;
	path[npath].idx = -1;
	path[npath].midx = -1;
	path[npath].lo = -1;
	path[npath].hi = -1;
	path[npath].npull = 0;
	path[npath].pullsz = 0;
	npath++;

	dh = -1;
	rb = flush(t, path, npath, &redo);
	if(rb == nil)
		goto Error;

	if(path[0].nl != nil)
		dh = 1;
	else if(path[1].nl != nil)
		dh = 0;
	else if(npath >2 && path[2].nl != nil)
		dh = -1;
	else
		abort();


	assert(rb->bp.addr != 0);
	lock(&t->lk);
	t->ht += dh;
	t->bp = rb->bp;
	t->dirty = 1;
	unlock(&t->lk);
	freepath(t, path, npath);
	free(path);
	if(redo)
		goto Again;
	return 0;
Error:
	freepath(t, path, npath);
	free(path);
	return Efs;
}

Blk*
getroot(Tree *t, int *h)
{
	Bptr bp;

	lock(&t->lk);
	bp = t->bp;
	if(h != nil)
		*h = t->ht;
	unlock(&t->lk);

	return getblk(bp, 0);
}

char*
btlookupat(Blk *b, int h, Key *k, Kvp *r, char *buf, int nbuf)
{
	int i, j, ok, same;
	char *err;
	Blk **p;
	Bptr bp;
	Msg m;

	assert(k != r);
	if((p = calloc(h, sizeof(Blk*))) == nil)
		return Emem;
	err = Eexist;
	ok = 0;
	p[0] = refblk(b);
	for(i = 1; i < h; i++){
		if(blksearch(p[i-1], k, r, nil) == -1)
			break;
		bp = getptr(r, nil);
		if((p[i] = getblk(bp, 0)) == nil){
			err = Efs;
			goto Out;
		}
	}
	if(p[h-1] != nil)
		blksearch(p[h-1], k, r, &ok);
	if(ok)
		cpkvp(r, r, buf, nbuf);
	for(i = h-2; i >= 0; i--){
		if(p[i] == nil)
			continue;
		j = bufsearch(p[i], k, &m, &same);
		if(j < 0 || !same)
			continue;
		assert(ok || m.op == Oinsert);
		ok = apply(r, &m, buf, nbuf);
		for(j++; j < p[i]->nbuf; j++){
			getmsg(p[i], j, &m);
			if(keycmp(k, &m) != 0)
				break;
			ok = apply(r, &m, buf, nbuf);
		}
	}
	if(ok)
		err = nil;
Out:
	for(i = 0; i < h; i++)
		if(p[i] != nil)
			putblk(p[i]);
	free(p);
	return err;
}

char*
btlookup(Tree *t, Key *k, Kvp *r, char *buf, int nbuf)
{
	char *ret;
	Blk *b;
	int h;

	if((b = getroot(t, &h)) == nil)
		return Efs;
	ret = btlookupat(b, h, k, r, buf, nbuf);
	putblk(b);

	return ret;
}

char*
btscan(Tree *t, Scan *s, char *pfx, int npfx)
{
	int i, same;
	Scanp *p;
	Bptr bp;
	Blk *b;
	Msg m;
	Kvp v;

	s->done = 0;
	s->offset = 0;
	s->pfx.k = s->pfxbuf;
	s->pfx.nk = npfx;
	memcpy(s->pfxbuf, pfx, npfx);

	s->kv.v = s->kvbuf+npfx;
	s->kv.nv = 0;
	cpkey(&s->kv, &s->pfx, s->kvbuf, sizeof(s->kvbuf));

	lock(&t->lk);
	s->root = *t;
	unlock(&t->lk);
	if((s->path = calloc(s->root.ht, sizeof(Scanp))) == nil){
		free(s);
		return nil;
	}

	p = s->path;
	if((b = getblk(s->root.bp, 0)) == nil)
		return Eio;
	p[0].b = b;
	for(i = 0; i < s->root.ht; i++){
		p[i].vi = blksearch(b, &s->kv, &v, &same);
		if(p[i].vi == -1 || (p[i].vi+1 < b->nval && !same && b->type == Tleaf)){
			getval(b, ++p[i].vi, &v);
		}
		if(b->type == Tpivot){
			p[i].bi = bufsearch(b, &s->kv, &m, &same);
			if(p[i].bi == -1 || !same)
				p[i].bi++;
			bp = getptr(&v, nil);
			if((b = getblk(bp, 0)) == nil)
				return Eio;
			p[i+1].b = b;
		}
	}
	assert(i == s->root.ht);
	return nil;
}

char *
btnext(Scan *s, Kvp *r, int *done)
{
	int i, j, h, ok, start, srcbuf;
	Scanp *p;
	Msg m, n;
	Bptr bp;
	Kvp kv;

	/* load up the correct blocks for the scan */
Again:
	p = s->path;
	h = s->root.ht;
	*done = 0;
	start = h;
	srcbuf = -1;
	for(i = h-1; i >= 0; i--){
		if(p[i].b != nil
		&&(p[i].vi < p[i].b->nval || p[i].bi < p[i].b->nbuf))
			break;
		if(i == 0){
			*done = 1;
			return nil;
		}
		if(p[i].b != nil)
			putblk(p[i].b);
		p[i].b = nil;
		p[i].vi = 0;
		p[i].bi = 0;
		p[i-1].vi++;
		start = i;
	}

	if(p[start-1].vi < p[start-1].b->nval){
		for(i = start; i < h; i++){
			getval(p[i-1].b, p[i-1].vi, &kv);
			bp = getptr(&kv, nil);
			if((p[i].b = getblk(bp, 0)) == nil)
				return "error reading block";
		}
	
		/* find the minimum key along the path up */
		m.op = Onop;
		getval(p[h-1].b, p[h-1].vi, &m);
	}else{
		getmsg(p[start-1].b, p[start-1].bi, &m);
		assert(m.op == Oinsert);
		srcbuf = start-1;
	}

	for(i = h-2; i >= 0; i--){
		if(p[i].bi == p[i].b->nbuf)
			continue;
		getmsg(p[i].b, p[i].bi, &n);
		if(keycmp(&n, &m) < 0){
			srcbuf = i;
			m = n;
		}
	}
	if(m.nk < s->pfx.nk || memcmp(m.k, s->pfx.k, s->pfx.nk) != 0){
		*done = 1;
		return nil;
	}

	/* scan all messages applying to the message */
	ok = 1;
	cpkvp(r, &m, s->kvbuf, sizeof(s->kvbuf));
	if(srcbuf == -1)
		p[h-1].vi++;
	else
		p[srcbuf].bi++;
	for(i = h-2; i >= 0; i--){
		for(j = p[i].bi; j < p[i].b->nbuf; j++){
			getmsg(p[i].b, j, &m);
			if(keycmp(r, &m) != 0)
				break;
			ok = apply(r, &m, s->kvbuf, sizeof(s->kvbuf));
			p[i].bi++;
		}
	}
	if(!ok)
		goto Again;
	return nil;
}

void
btdone(Scan *s)
{
	int i;

	for(i = 0; i < s->root.ht; i++)
		putblk(s->path[i].b);
	free(s->path);
}
