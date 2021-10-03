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
	dst->k = buf;
	dst->nk = src->nk;
	memcpy(dst->k, src->k, src->nk);
}

void
cpkvp(Kvp *dst, Kvp *src, char *buf, int nbuf)
{
	assert(src->nk+src->nv <= nbuf);
	cpkey(dst, src, buf, nbuf);
	dst->type = src->type;
	if(src->type == Vinl){
		dst->v = buf+src->nk;
		dst->nv = src->nv;
	}else{
		dst->bp = src->bp;
		dst->fill = src->fill;
	}
	memcpy(dst->v, src->v, src->nv);
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
	return 1 + 2 + m->nk + 2 + m->nv;
}

int
valsz(Kvp *kv)
{
	if(kv->type == Vref)
		return 2+kv->nk + Ptrsz + Fillsz;
	else
		return 2+kv->nk + 2+kv->nv;
}

void
getval(Blk *b, int i, Kvp *kv)
{
	int o;

	assert(i >= 0 && i < b->nval);
	o = GBIT16(b->data + 2*i);
	if(b->type == Tpivot){
		kv->type = Vref;
		kv->nk = GBIT16(b->data + o);
		kv->k = b->data + o + 2;
		kv->bp = unpackbp(kv->k + kv->nk);
		kv->fill = GBIT16(kv->k + kv->nk + Ptrsz);
	}else{
		kv->type = Vinl;
		kv->nk = GBIT16(b->data + o);
		kv->k = b->data + o + 2;
		kv->nv = GBIT16(kv->k + kv->nk);
		kv->v = kv->k + kv->nk + 2;
	}
}

static void
setval(Blk *b, int i, Kvp *kv, int replace)
{
	int o, nk, nv, ksz, vsz, spc;
	char *p;

	assert(i >= 0 && i <= b->nval);
	spc = (b->type == Tleaf) ? Leafspc : Pivspc;
	p = b->data + 2*i;
	nk = 2 + kv->nk;
	nv = (kv->type == Vref) ? Ptrsz+Fillsz : 2 + kv->nv;
	if (i < 0)
		i = 0;
	if(!replace || b->nval == i){
		memmove(p + 2, p, 2*(b->nval - i));
		b->nval++;
		b->valsz += nk + nv;
		o = spc - b->valsz;
	}else{
		/*
		 * If we can't put it where it was before,
		 * we need to allocate new space for the
		 * key-value data. It'll get compacted when
		 * we split or merge.
		 */
		o = GBIT16(b->data + 2*i);
		ksz = 2 + GBIT16(b->data + o);
		if(b->type == Tleaf)
			vsz = 2 + GBIT16(b->data + o + ksz);
		else
			vsz = 16;
		if(ksz + vsz < nk + nv){
			b->valsz += nk + nv;
			o = spc - b->valsz;
		}
	}

	if(2*b->nval + b->valsz > spc){
		dprint("2*%d + %d > %d [ksz: %d, vsz: %d]\n",
			2*b->nval, b->valsz, spc, kv->nk, kv->nv);
		showblk(b, "setval overflow", 1);
		abort();
	}
	assert(2*b->nval + b->valsz <= spc);
	assert(2*b->nval <= o);
	p = b->data + o;
	if(b->type == Tpivot){
		PBIT16(b->data + 2*i, o);
		PBIT16(p +  0, kv->nk);
		memcpy(p +  2, kv->k, kv->nk);
		p = packbp(p + kv->nk + 2, &kv->bp);
		PBIT16(p, kv->fill);
	} else {
		PBIT16(b->data + 2*i, o);
		PBIT16(p +  0, kv->nk);
		memcpy(p +  2, kv->k, kv->nk);
		PBIT16(p + kv->nk + 2, kv->nv);
		memcpy(p + kv->nk + 4, kv->v, kv->nv);
	}
}

static int
delval(Blk *b, int i)
{
	char *p;

	assert(i >= 0 && i <= b->nval);
	b->nval--;
	p = b->data + 2*i;
	memmove(p, p + 2, 2*(b->nval - i));
	return 0;
}

void
setmsg(Blk *b, int i, Msg *m)
{
	char *p;
	int o;

	assert(b->type == Tpivot);
	assert(i >= 0 && i <= b->nbuf);
	b->nbuf++;
	b->bufsz += msgsz(m);
	assert(2*b->nbuf + b->bufsz <= Bufspc);

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
	m->type = Vinl;
	m->op = *p;
	m->nk = GBIT16(p + 1);
	m->k = p + 3;
	m->nv = GBIT16(p + 3 + m->nk);
	m->v = p + 5 + m->nk;
}

void
blkinsert(Blk *b, Kvp *kv)
{
	int lo, hi, mid, r;
	Kvp cmp;

	r = -1;
	lo = 0;
	hi = b->nval;
	while(lo < hi){
		mid = (hi + lo) / 2;
		getval(b, mid, &cmp);
		r = keycmp(kv, &cmp);
		if(r <= 0)
			hi = mid;
		else
			lo = mid + 1;
	}
	if(lo < b->nval){
		getval(b, lo, &cmp);
		r = keycmp(kv, &cmp);
	}
	setval(b, lo, kv, (r == 0));
}

void
bufinsert(Blk *b, Msg *m)
{
	int lo, hi, mid, r;
	Msg cmp;

	lo = 0;
	hi = b->nbuf;
	while(lo < hi){
		mid = (hi + lo) / 2;
		getmsg(b, mid, &cmp);
		r = keycmp(m, &cmp);
		if(r < 0)
			hi = mid;
		else
			lo = mid + 1;
	}
	setmsg(b, lo, m);
}

static int
bufsearch(Blk *b, Key *k, Msg *m, int *same)
{
	int lo, hi, mid, r;

	r = -1;
	lo = 0;
	hi = b->nbuf - 1;
	while(lo <= hi){
		mid = (hi + lo) / 2;
		getmsg(b, mid, m);
		r = keycmp(k, m);
		if(r < 0)
			hi = mid - 1;
		else
			lo = mid + 1;
	}
	lo--;
	if(lo >= 0){
		getmsg(b, lo, m);
		r = keycmp(k, m);
	}
	if(same != nil)
		*same = (r == 0);
	return lo;
}

int
blkdelete(Blk *b, Kvp *kv)
{
	int lo, hi, mid, r;
	Kvp cmp;

	lo = 0;
	hi = b->nval - 1;
	while(lo <= hi){
		mid = (hi + lo) / 2;
		getval(b, mid, &cmp);
		r = keycmp(kv, &cmp);
		if(r == 0)
			delval(b, mid);
		if(r <= 0)
			hi = mid - 1;
		else
			lo = mid + 1;
	}
	return -1;
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

	/*
	 * It's possible for the previous node to have
	 * been fully cleared out by a large number of
	 * delete messages, so we need to check if
	 * there's anything in it to copy up.
	 */
	if(pp->split){
		if(pp->l->nval > 0){
			getval(pp->l, 0, &kv);
			kv.type = Vref;
			kv.bp = pp->l->bp;
			kv.fill = blkfill(pp->l);
			setval(n, i++, &kv, 0);
			if(nbytes != nil)
				*nbytes += 2 + valsz(&kv);
		}
		if(pp->r->nval > 0){
			getval(pp->r, 0, &kv);
			kv.type = Vref;
			kv.bp = pp->r->bp;
			kv.fill = blkfill(pp->r);
			setval(n, i++, &kv, 0);
			if(nbytes != nil)
				*nbytes += 2 + valsz(&kv);
		}
	}else{
		if(pp->n->nval > 0){
			getval(pp->n, 0, &kv);
			kv.type = Vref;
			kv.bp = pp->n->bp;
			kv.fill = blkfill(pp->n);
			setval(n, i++, &kv, 1);
			if(nbytes != nil)
				*nbytes += 2 + valsz(&kv);
		}
	}
	return i;
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
update(Path *p, Path *pp)
{
	int i, j, lo, hi, pidx, midx;
	Blk *b, *n;
	Msg m;

	j = 0;
	b = p->b;
	lo = (p != nil) ? p->lo : -1;
	hi = (p != nil) ? p->hi : -1;
	pidx = (p != nil) ? p->idx : -1;
	midx = (p != nil && p->merge) ? p->midx : -1;
if(p->merge) showblk(b, "preupdate", 0);
	if((n = newblk(b->type)) == nil)
		return -1;
	for(i = 0; i < b->nval; i++){
		if(i == pidx){
			j = copyup(n, j, pp, nil);
			continue;
		}else if(i == midx){
			getval(p->nl, 0, &m);
			m.type = Vref;
			m.bp = p->nl->bp;
			m.fill = blkfill(p->nl);
			setval(n, j++, &m, 0);
			if(p->nr){
				getval(p->nr, 0, &m);
				m.type = Vref;
				m.bp = p->nr->bp;
				m.fill = blkfill(p->nr);
				setval(n, j++, &m, 0);
				i++;
			}
			continue;
		}
		getval(b, i, &m);
		setval(n, j++, &m, 0);
	}
	if(b->type == Tpivot){
		i = 0;
		j = 0;
		while(i < b->nbuf){
			if(i == lo)
				i = hi;
			if(i == b->nbuf)
				break;
			getmsg(b, i++, &m);
			setmsg(n, j++, &m);
		}
		b->nbuf = j;
	}
	p->n = n;
	return 0;
}


/*
 * Splits a node, returning the block that msg
 * would be inserted into. Split must never
 * grow the total height of the 
 */
int
split(Blk *b, Path *p, Path *pp, Kvp *mid)
{
	int i, j, copied, halfsz;
	int lo, hi, pidx;
	Blk *l, *r;
	Kvp t;
	Msg m;

	/*
	 * If the block one entry up the
	 * p is nil, we're at the root,
	 * so we want to make a new block.
	 */
	l = newblk(b->type);
	r = newblk(b->type);
	if(l == nil || r == nil){
		freeblk(l);
		freeblk(r);
		return -1;
	}

	lo = (p != nil) ? p->lo : -1;
	hi = (p != nil) ? p->hi : -1;
	pidx = (p != nil) ? p->idx : -1;

	j = 0;
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
		if(i == b->nval-2)
			break;
		if(i >= 2 && copied >= halfsz)
			break;

		if(i == pidx){
			j = copyup(l, j, pp, &copied);
			continue;
		}
		getval(b, i, &t);
		setval(l, j++, &t, 0);
		copied += 2 + valsz(&t);
	}
	j = 0;
	getval(b, i, mid);
	for(; i < b->nval; i++){
		if(i == pidx){
			j = copyup(r, j, pp, nil);
			continue;
		}
		getval(b, i, &t);
		setval(r, j++, &t, 0);
	}
	if(b->type == Tpivot){
		i = 0;
		for(j = 0; i < b->nbuf; i++, j++){
			if(i == lo)
				i = hi;
			if(i == b->nbuf)
				break;
			getmsg(b, i, &m);
			if(keycmp(&m, mid) >= 0)
				break;
			setmsg(l, j, &m);
		}
		for(j = 0; i < b->nbuf; i++, j++){
			if(i == lo)
				i = hi;
			if(i == b->nbuf)
				break;
			getmsg(b, i, &m);
			setmsg(r, j, &m);
		}
	}
	p->split = 1;
	p->l = l;
	p->r = r;
	return 0;
}

int
apply(Blk *b, Msg *m)
{
	int idx, same;
	vlong v;
	char *p;
	Kvp kv;

	assert(b->type == Tleaf);
	switch(m->op&0xf){
	case Oinsert:
		blkinsert(b, m);
		break;
	case Odelete:
		blkdelete(b, m);
		break;
	case Owstat:
		p = m->v;
		idx = blksearch(b, m, &kv, &same);
		if(idx == -1 || !same)
			abort();
		/* bump version */
		v = GBIT32(kv.v+8);
		PBIT32(kv.v+8, v+1);
		if(m->op & Owmtime){
			v = GBIT64(p);
			p += 8;
			PBIT32(kv.v+25, v);
		}
		if(m->op & Owsize){
			v = GBIT64(p);
			p += 8;
			PBIT64(kv.v+33, v);
		}
		if(m->op & Owmode){
			v = GBIT32(p);
			p += 4;
			PBIT32(kv.v+33, v);
		}
		if(m->op & Owname){
			fprint(2, "renames not yet supported\n");
			abort();
		}
		if(p != m->v + m->nv)
			fprint(2, "malformed wstat message");
		break;
	default:
		abort();
	}
	return 0;
}

int
merge(Path *p, int idx, Blk *a, Blk *b)
{
	int i, o;
	Msg m;
	Blk *d;

//showfs("premerge");
	if((d = newblk(a->type)) == nil)
		return -1;
	o = 0;
	for(i = 0; i < a->nval; i++){
		getval(a, i, &m);
		setval(d, o++, &m, 0);
	}
	for(i = 0; i < b->nval; i++){
		getval(b, i, &m);
		setval(d, o++, &m, 0);
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
//showfs("postmerge");
	enqueue(d);
	p->merge = 1;
	p->midx = idx;
	p->nl = d;
	p->nr = nil;
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
	for(i = 0; i < b->nbuf; i++){
		getmsg(b, i, &n);
		print("check %P before %P: %d\n", &n.Kvp, &m->Kvp, keycmp(&n, m));
		if(keycmp(m, &n) <= 0){
			*idx = i + o;
			return 0;
		}
		print("would copy %P before %P: %d\n", &n.Kvp, &m->Kvp, keycmp(&n, m));
		used += 2 + msgsz(&n);
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
rotate(Path *p, int midx, Blk *a, Blk *b, int halfpiv)
{
	int i, o, cp, sp, idx;
	Blk *d, *l, *r;
	Msg m;

	print("a->type: %d\n", a->type);
	l = newblk(a->type);
	r = newblk(a->type);
	if(l == nil || r == nil){
		freeblk(l);
		freeblk(r);
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
		setval(d, o++, &m, 0);
		cp += valsz(&m);
	}
	for(i = 0; i < b->nval; i++){
		getval(b, i, &m);
		if(d == l && (cp >= halfpiv || spillsbuf(d, a, b, &m, &idx))){
			sp = idx;
			d = r;
			o = 0;
		}
		setval(d, o++, &m, 0);
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
	p->merge = 1;
	p->midx = midx;
	p->nl = l;
	p->nr = r;
	return 0;
}

int
rotmerge(Path *p, int idx, Blk *a, Blk *b)
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
	if(na + nb < Pivspc && ma + mb < Bufspc)
		return merge(p, idx, a, b);
	else if(imbalance > 4*Msgmax)
		return rotate(p, idx, a, b, (na + nb)/2);
	else
		return 0;
}

int
trybalance(Path *p, Path *pp, int idx)
{
	Blk *l,*m, *r;
	Kvp km, kl, kr;
	int ret;

	l = nil;
	r = nil;
	ret = -1;
	if(p->idx == -1)
		return 0;
	if(pp != nil){
		if((m = pp->n) == nil)
			return 0;
	}else{
		if((m = getblk(km.bp, 0)) == nil)
			return -1;
	}
	/* Try merging left */
	getval(p->b, idx, &km);
	if(idx-1 >= 0){
		getval(p->b, idx-1, &kl);
		if(kl.fill + km.fill >= Blkspc)
			goto next;
		if((l = getblk(kl.bp, 0)) == nil)
			goto out;
		if(rotmerge(p, idx-1, l, m) == -1)
			goto out;
		goto done;
	}
next:
	if(idx+1 < p->b->nval){
		getval(p->b, idx+1, &kr);
		if(kr.fill + km.fill >= Blkspc)
			goto done;
		if((r = getblk(kr.bp, 0)) == nil)
			goto out;
		if(rotmerge(p, idx, m, r) == -1)
			goto out;
		goto done;
	}
done:
	ret = 0;
out:
	putblk(l);
	putblk(r);
	if(pp == nil)
		putblk(m);
	return ret;
}

int
insertmsg(Blk *rb, Msg *msg, int nmsg, int sz)
{
	int i;

	if(rb->type == Tleaf && !filledleaf(rb, sz))
		for(i = 0; i < nmsg; i++)
			apply(rb, &msg[i]);
	else if(rb->type == Tpivot && !filledbuf(rb, nmsg, sz))
		for(i = 0; i < nmsg; i++)
			bufinsert(rb, &msg[i]);
	else
		return 1;
	return 0;
}

static Blk*
flush(Path *path, int npath, Msg *msg, int nmsg, int *redo)
{

	Path *p, *pp, *oldroot;
	Blk *b, *r;
	Kvp mid;
	Msg m;
	int i;

	/*
	 * The path must contain at minimum two elements:
	 * we must have 1 node we're inserting into, and
	 * an empty element at the top of the path that
	 * we put the new root into if the root gets split.
	 */
	assert(npath >= 2);
	r = nil;
	pp = nil;
	p = &path[npath - 1];
	oldroot = &path[1];
	*redo = 1;
	if(p->b->type == Tleaf){
		if(!filledleaf(p->b, p[-1].sz)){
			if(update(p, pp) == -1)
				goto error;
			for(i = p[-1].lo; i < p[-1].hi; i++){
				getmsg(p[-1].b, i, &m);
				if(filledleaf(p->n, msgsz(&m)))
					break;
				apply(p->n, &m);
			}
			if(p == oldroot){
				r = p->n;
				*redo = insertmsg(r, msg, nmsg, path[0].sz);
			}
			enqueue(p->n);
		}else{
			if(split(p->b, p, pp, &mid) == -1)
				goto error;
			b = p->l;
			for(i = p[-1].lo; i < p[-1].hi; i++){
				getmsg(p[-1].b, i, &m);
				if(keycmp(&m, &mid) >= 0)
					b = p->r;
				if(filledleaf(b, msgsz(&m)))
					continue;
				if(apply(b, &m) == -1)
					goto error;
			}
			enqueue(p->l);
			enqueue(p->r);
		}
		assert(p->n || (p->l && p->r));
		p->midx = -1;
		pp = p;
		p--;
	}
	for(; p > path; p--){
		if(!filledpiv(p->b, 1)){
			if(trybalance(p, pp, p->idx) == -1)
				goto error;
			/* If we merged the root node, break out. */
			if(p == oldroot && p->merge && p->nr == nil && p->b->nval == 2){
				/* FIXME: shouldn't p[1].n already be right? */
				r = p[1].n;
				*redo = insertmsg(r, msg, nmsg, path[0].sz);
				p[1].n = p[0].nl;
				p[0].n = nil;
				break;
			}
			if(update(p, pp) == -1)
				goto error;
			for(i = p[-1].lo; i < p[-1].hi; i++){
				getmsg(p[-1].b, i, &m);
				if(filledbuf(p->n, 1, msgsz(&m)))
					break;
				bufinsert(p->n, &m);
			}
			if(p == oldroot && !filledbuf(p->n, nmsg, path[0].sz)){
				r = p->n;
				*redo = insertmsg(r, msg, nmsg, path[0].sz);
			}
			enqueue(p->n);
		}else{
			dprint("-- split\n");
			if(split(p->b, p, pp, &mid) == -1)
				goto error;
			b = p->l;
			for(i = p[-1].lo; i < p[-1].hi; i++){
				getmsg(p[-1].b, i, &m);
				if(keycmp(&m, &mid) >= 0)
					b = p->r;
				if(filledbuf(b, 1, msgsz(&m)))
					continue;
				bufinsert(b, &m);
			}
			enqueue(p->l);
			enqueue(p->r);
		}
		pp = p;
	}
	if(path[1].split){
		if((r = newblk(Tpivot)) == nil)
			goto error;
		path[0].n = r;
		*redo = insertmsg(r, msg, nmsg, path[0].sz);
		copyup(path[0].n, 0, &path[1], nil);
		enqueue(r);
	}
	return r;
error:
	return nil;
}

void
freepath(Path *path, int npath)
{
	Path *p;

	for(p = path; p != path + npath; p++){
		if(p->b != nil)
			putblk(p->b);
		if(p->n != nil)
			putblk(p->n);
		if(p->l != nil)
			putblk(p->l);
		if(p->r != nil)
			putblk(p->r);
		if(p->nl != nil)
			putblk(p->nl);
		if(p->nr != nil)
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
			cursz += 2 + msgsz(&m);
		}
		if(cursz > maxsz){
			maxsz = cursz;
			p->lo = lo;
			p->hi = j;
			p->sz = maxsz;
			p->idx = i - 1;
		}
	}
}

int
btupsert(Tree *t, Msg *msg, int nmsg)
{
	int i, npath, redo, dh, sz, height;
	Path *path;
	Blk *b, *rb;
	Kvp sep;

	sz = 0;
	for(i = 0; i < nmsg; i++){
		if(msg[i].nk + 2 > Keymax){
			werrstr("overlong key");
			return -1;
		}
		sz += msgsz(&msg[i]);
	}

again:
	if((b = getroot(t, &height)) == nil){
		werrstr("get root: %r");
		return -1;
	}

	/*
	 * The tree can grow in height by 1 when we
	 * split, so we allocate room for one extra
	 * node in the path.
	 */
	redo = 0;
	npath = 0;
	if((path = calloc((height + 2), sizeof(Path))) == nil)
		return -1;
	path[npath].b = nil;
	path[npath].idx = -1;
	path[npath].midx = -1;
	npath++;

	path[0].sz = sz;
	while(b->type == Tpivot){
		if(!filledbuf(b, nmsg, path[npath - 1].sz))
			break;
		victim(b, &path[npath]);
		getval(b, path[npath].idx, &sep);
		b = getblk(sep.bp, 0);
		if(b == nil)
			goto error;
		npath++;
	}
	path[npath].b = b;
	path[npath].idx = -1;
	path[npath].lo = 0;
	path[npath].hi = 0;
	npath++;

	dh = -1;
	rb = flush(path, npath, msg, nmsg, &redo);
	if(redo)
		goto again;
	if(rb == nil)
		goto error;

	if(path[0].n != nil)
		dh = 1;
	else if(path[1].n != nil)
		dh = 0;
	else if(npath >2 && path[2].n != nil)
		dh = -1;
	else
		abort();


	assert(rb->bp.addr != 0);
	lock(&t->lk);
	t->ht += dh;
	t->bp = rb->bp;
	fs->nextgen++;
	unlock(&t->lk);

	freepath(path, npath);
	free(path);
	if(!checkfs()){
		showfs("broken");
		abort();
	}
	if(redo)
		goto again;
	snapshot();
	return 0;
error:
	freepath(path, npath);
	free(path);
	return -1;
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

	return getblk(t->bp, 0);
}

static char*
collect(Blk *b, Key *k, Kvp *r, int *done)
{
	int i, idx, same;
	char *err;
	Msg m;

	*done = 0;
	idx = bufsearch(b, k, &m, &same);
	if(!same)
		return nil;
	err = Eexist;
	for(i = idx; i < b->nbuf; i++){
		getmsg(b, i, &m);
		if(keycmp(&m, k) != 0)
			break;
		switch(m.op){
		case Oinsert:
			*r = m.Kvp;
			*done = 1;
			err = nil;
			break;
		case Odelete:
			*done = 1;
			err = Eexist;
			break;
		default:
			return Efs;
		}
	}
	return err;
}

char*
btlookupat(Blk *b0, Key *k, Kvp *r, Blk **bp)
{
	int idx, same, done;
	char *ret;
	Blk *b;

	*bp = nil;
	b = pinblk(b0);
	assert(k != r);
	while(b->type == Tpivot){
		ret = collect(b, k, r, &done);
		if(done)
			return ret;
		idx = blksearch(b, k, r, nil);
		if(idx == -1)
			return Eexist;
		putblk(b);
		if((b = getblk(r->bp, 0)) == nil)
			return Efs;
	}
	assert(b->type == Tleaf);
	blksearch(b, k, r, &same);
	if(!same){
		putblk(b);
		return Eexist;
	}
	*bp = b;
	return nil;
}

char*
btlookup(Tree *t, Key *k, Kvp *r, Blk **bp)
{
	char *ret;
	Blk *b;

	*bp = nil;
	if((b = getroot(t, nil)) == nil)
		return Efs;
	ret = btlookupat(b, k, r, bp);
	putblk(b);

	return ret;
}

char*
btscan(Tree *t, Scan *s, char *pfx, int npfx)
{
	int i, same;
	Scanp *p;
	Msg m;
	Kvp v;
	Blk *b;

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
		return "error reading block";
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
			if((b = getblk(v.bp, 0)) == nil)
				return "error reading block";
			p[i+1].b = b;
		}
	}
	assert(i == s->root.ht);
	return nil;
}

int
accum(Scan *s, Msg *m)
{
	vlong v;
	char *p;
	Dir *d;

	d = &s->dir;
	switch(m->op&0xf){
	case Onop:
	case Oinsert:
		s->present = 1;
		kv2dir(m, d);
		fprint(2, "name: %s\n", d->name);
		break;
	case Odelete:
		s->present = 0;
		break;
	case Owstat:
		p = m->v;
		d->qid.vers++;
		if(m->op & Owmtime){
			v = GBIT64(p);
			p += 8;
			d->mtime = v;
		}
		if(m->op & Owsize){
			v = GBIT64(p);
			p += 8;
			d->length = v;
		}
		if(m->op & Owmode){
			v = GBIT32(p);
			p += 4;
			d->mode = v;
		}
		if(m->op & Owname){
			fprint(2, "renames not yet supported\n");
			abort();
		}
		if(p != m->v + m->nv){
			fprint(2, "malformed wstat message");
			abort();
		}
		break;
	default:
		abort();
	}
	return 0;

}

char *
btnext(Scan *s, Kvp *r, int *done)
{
	Scanp *p;
	int i, j, h, start;
	Msg m, n, t;
	Kvp kv;

Again:
	/* load up the correct blocks for the scan */
	p = s->path;
	h = s->root.ht;
	*done = 0;
	start = h;

	for(i = h-1; i > 0; i--){
		if(p[i].vi < p[i].b->nval || p[i].bi < p[i].b->nbuf)
			break;
		if(i == 0){
			*done = 1;
			return nil;
		}
		start = i;
		p[i-1].vi++;
		p[i].vi = 0;
		p[i].bi = 0;
	}
	for(i = start; i < h; i++){
		getval(p[i-1].b, p[i-1].vi, &kv);
		if((p[i].b = getblk(kv.bp, 0)) == nil)
			return "error reading block";
	}

	/* find the minimum key along the path up */
	m.op = Onop;
	getval(p[h-1].b, p[h-1].vi, &m);
	for(i = h-2; i >= 0; i--){
		if(p[i].bi == p[i].b->nbuf)
			continue;
		getmsg(p[i].b, p[i].bi, &n);
		if(keycmp(&n, &m) < 0)
			m = n;
	}
	if(m.nk < s->pfx.nk || memcmp(m.k, s->pfx.k, s->pfx.nk) != 0){
		*done = 1;
		return nil;
	}

	/* scan all messages applying to the message */
	getval(p[h-1].b, p[h-1].vi, &t);
	if(keycmp(&m, &t) == 0){
		t.op = Onop;
		accum(s, &t);
		p[h-1].vi++;
	}
	for(i = h-2; i >= 0; i--){
		for(j = p[i].bi; j < p[i].b->nbuf; j++){
			getmsg(p[i].b, j, &t);
			if(keycmp(&m, &t) != 0)
				break;
			accum(s, &t);
			p[i].bi++;
			m = t;
		}
	}
	if(m.op == Odelete)
		goto Again;
	cpkvp(r, &m, s->kvbuf, sizeof(s->kvbuf));
	return nil;
}

void
btdone(Scan *s)
{
	int i;

	for(i = 0; i < s->root.ht; i++)
		if(s->path[i].b != nil)
			putblk(s->path[i].b);
	free(s->path);
}

int
snapshot(void)
{
	Arena *a;
	Blk *s;
	int i, r;

	r = 0;
	s = fs->super;

	qlock(&fs->snaplk);
	lock(&fs->root.lk);
	fillsuper(s);
	enqueue(s);
	unlock(&fs->root.lk);

	for(i = 0; i < fs->narena; i++){
		a = &fs->arenas[i];
		finalize(a->logtl);
		if(syncblk(a->logtl) == -1)
			r = -1;
	}
	if(r != -1)
		r = syncblk(s);
	qunlock(&fs->snaplk);
	return r;
}
