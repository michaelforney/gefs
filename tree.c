#include <u.h>
#include <libc.h>
#include <fcall.h>

#include "dat.h"
#include "fns.h"

int lookup;
void
dprint(char *fmt, ...)
{
	va_list ap;

	if(!debug)
		return;
	va_start(ap, fmt);
	vfprint(2, fmt, ap);
	va_end(ap);
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
		return 2+kv->nk + Ptrsz;
	else
		return 2+kv->nk + 2+kv->nv;
}

void
getval(Blk *b, int i, Kvp *kv)
{
	int o;

	assert(i >= 0 && i < b->nent);
	o = GBIT16(b->data + 2*i);
	if(b->type == Pivot){
		kv->type = Vref;
		kv->nk = GBIT16(b->data + o);
		kv->k = b->data + o + 2;
		kv->bp = GBIT64(kv->k + kv->nk + 0);
		kv->bh = GBIT64(kv->k + kv->nk + 8);
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

	assert(i >= 0 && i <= b->nent);
	spc = (b->type == Leaf) ? Leafspc : Pivspc;
	p = b->data + 2*i;
	nk = 2 + kv->nk;
	nv = (kv->type == Vref) ? Ptrsz : 2 + kv->nv;
	if (i < 0)
		i = 0;
	if(!replace || b->nent == i){
		memmove(p + 2, p, 2*(b->nent - i));
		b->nent++;
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
		if(b->type == Leaf)
			vsz = 2 + GBIT16(b->data + o + ksz);
		else
			vsz = 16;
		if(ksz + vsz < nk + nv){
			b->valsz += nk + nv;
			o = spc - b->valsz;
		}
	}

	if(2*b->nent + b->valsz > spc)
		showblk(b, "setval overflow");
	assert(2*b->nent + b->valsz <= spc);
	assert(2*b->nent < o);
	p = b->data + o;
	if(b->type == Pivot){
		assert(kv->type == Vref);
		PBIT16(b->data + 2*i, o);
		PBIT16(p +  0, kv->nk);
		memcpy(p +  2, kv->k, kv->nk);
		PBIT64(p + kv->nk +  2, kv->bp);
		PBIT64(p + kv->nk + 10, kv->bh);
	} else {
		assert(kv->type == Vinl);
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

	assert(i >= 0 && i <= b->nent);
	b->nent--;
	p = b->data + 2*i;
	memmove(p, p + 2, 2*(b->nent - i));
	return 0;
}

void
setmsg(Blk *b, int i, Msg *m)
{
	char *p;
	int o;

	assert(b->type == Pivot);
	assert(i >= 0 && i <= b->nmsg);
	b->nmsg++;
	b->bufsz += msgsz(m);
	assert(2*b->nent + b->bufsz <= Bufspc);

	p = b->data + Pivspc;
	o = Pivspc - b->bufsz;
	memmove(p + 2*(i+1), p+2*i, 2*(b->nmsg - i));
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

	assert(b->type == Pivot);
	assert(i >= 0 && i < b->nmsg);
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
	hi = b->nent;
	while(lo < hi){
		mid = (hi + lo) / 2;
		getval(b, mid, &cmp);
		r = keycmp(kv, &cmp);
		if(r <= 0)
			hi = mid;
		else
			lo = mid + 1;
	}
	if(lo < b->nent){
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
	hi = b->nmsg;
	while(lo < hi){
		mid = (hi + lo) / 2;
		getmsg(b, mid, &cmp);
		r = keycmp(m, &cmp);
		if(r <= 0)
			hi = mid;
		else
			lo = mid + 1;
	}
	setmsg(b, lo, m);
}

static int
bufsearch(Blk *b, Key *k, Msg *m, int *idx)
{
	int lo, hi, mid, r;

	lo = 0;
	hi = b->nmsg - 1;
	while(lo <= hi){
		mid = (hi + lo) / 2;
		getmsg(b, mid, m);
		r = keycmp(k, m);
		if(r == 0){
			*idx = mid;
			return 0;
		}
		if(r < 0)
			hi = mid - 1;
		else
			lo = mid + 1;
	}
	return -1;
}

int
blkdelete(Blk *b, Kvp *kv)
{
	int lo, hi, mid, r;
	Kvp cmp;

	lo = 0;
	hi = b->nent - 1;
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
blksearch(Blk *b, Key *k, Kvp *rp, int *idx, int *same)
{
	int lo, hi, mid, r;
	Kvp cmp;

	r = -1;
	lo = 0;
	hi = b->nent;
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
	if(idx != nil)
		*idx = lo;
	return 0;
}
int
filledbuf(Blk *b, int needed)
{
	assert(b->type == Pivot);
	return 2*b->nmsg + b->bufsz > Bufspc - needed;
}


int
filledleaf(Blk *b, int needed)
{
	assert(b->type == Leaf);
	return 2*b->nent + b->valsz > Leafspc - needed;
}

int
filledpiv(Blk *b, int reserve)
{
	/* 
	 * We need to guarantee there's room for one message
	 * at all times, so that splits along the whole path
	 * have somewhere to go.
	 */
	assert(b->type == Pivot);
	return 2*b->nent + b->valsz > Pivspc - reserve*Kpmax;
}

int
copyup(Blk *n, int i, Path *pp, int *nbytes)
{
	Kvp kv;

	
	if(pp->split){
		getval(pp->l, 0, &kv);
		kv.type = Vref;
		kv.bp = (uintptr)pp->l;
		kv.bh = blkhash(pp->l);
		setval(n, i++, &kv, 0);
		if(nbytes != nil)
			*nbytes += 2 + valsz(&kv);

		getval(pp->r, 0, &kv);
		kv.type = Vref;
		kv.bp = (uintptr)pp->r;
		kv.bh = blkhash(pp->r);
		setval(n, i++, &kv, 0);
		if(nbytes != nil)
			*nbytes += 2 + valsz(&kv);
	}else{
		getval(pp->n, 0, &kv);
		kv.type = Vref;
		kv.bp = (uintptr)pp->n;
		kv.bh = blkhash(pp->n);
		setval(n, i++, &kv, 1);
		if(nbytes != nil)
			*nbytes += 2 + valsz(&kv);
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
Blk*
update(Blk *b, Path *p, Path *pp)
{
	int i, j, lo, hi, pidx;
	Blk *n;
	Msg m;

	j = 0;
	lo = (p != nil) ? p->lo : -1;
	hi = (p != nil) ? p->hi : -1;
	pidx = (p != nil) ? p->idx : -1;
	n = newblk(b->type);
	for(i = 0; i < b->nent; i++){
		if(i == pidx){
			j = copyup(n, j, pp, nil);
			continue;
		}
		getval(b, i, &m);
		setval(n, j++, &m, 0);
	}
	if(b->type == Pivot){
		i = 0;
		j = 0;
		while(i < b->nmsg){
			if(i == lo)
				i = hi;
			if(i == b->nmsg)
				break;
			getmsg(b, i++, &m);
			setmsg(n, j++, &m);
		}
		b->nmsg = j;
	}
	return n;
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
	if(l == nil || r == nil)
		goto error;

	lo = (p != nil) ? p->lo : -1;
	hi = (p != nil) ? p->hi : -1;
	pidx = (p != nil) ? p->idx : -1;

	j = 0;
	copied = 0;
	halfsz = (2*b->nent + b->valsz)/2;
	for(i = 0; copied < halfsz; i++){
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
	for(; i < b->nent; i++){
		if(i == pidx){
			j = copyup(r, j, pp, nil);
			continue;
		}
		getval(b, i, &t);
		setval(r, j++, &t, 0);
	}
	if(b->type == Pivot){
		i = 0;
		for(j = 0; i < b->nmsg; i++, j++){
			if(i == lo)
				i = hi;
			if(i == b->nmsg)
				break;
			getmsg(b, i, &m);
			if(keycmp(&m, mid) >= 0)
				break;
			setmsg(l, j, &m);
		}
		for(j = 0; i < b->nmsg; i++, j++){
			if(i == lo)
				i = hi;
			if(i == b->nmsg)
				break;
			getmsg(b, i, &m);
			setmsg(r, j, &m);
		}
	}
	p->split = 1;
	p->l = l;
	p->r = r;
	return 0;
error:
	if(l != nil) freeblk(l);
	if(r != nil) freeblk(r);
	return -1;
}

int
apply(Blk *b, Msg *m)
{
	assert(b->type == Leaf);
	assert(b->flag & Bdirty);
	switch(m->op){
	case Ocreate:
		blkinsert(b, m);
		break;
	case Odelete:
		blkdelete(b, m);
		break;
	case Owrite:
		werrstr("unimplemented");
		goto error;
	}
	return 0;
error:
	werrstr("invalid upsert");
	return -1;
}

static int
flush(Path *path, int npath, Msg *ins, int *redo)
{
	static int nins;

	Path *p, *pp;
	Blk *b;
	Kvp mid;
	Msg m;
	int i, ret;

	ret = -1;
	/*
	 * The path must contain at minimum two elements:
	 * we must have 1 node we're inserting into, and
	 * an empty element at the top of the path that
	 * we put the new root into if the root gets split.
	 */
	assert(npath >= 2);
	*redo = 0;
	pp = nil;
	p = &path[npath - 1];
	if(p->b->type == Leaf){
		if(!filledleaf(p->b, p[-1].sz)){
			p->n = update(p->b, p, pp);
			for(i = p[-1].lo; i < p[-1].hi; i++){
				getmsg(p[-1].b, i, &m);
				if(filledleaf(p->n, msgsz(&m)))
					break;
				apply(p->n, &m);
			}
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
		}
		assert(p->n || (p->l && p->r));
		pp = p;
		p--;
	}
	for(; p > path; p--){
		if(!filledpiv(p->b, 1)){
			p->n = update(p->b, p, pp);
			for(i = p[-1].lo; i < p[-1].hi; i++){
				getmsg(p[-1].b, i, &m);
				if(filledbuf(p->n, msgsz(&m)))
					break;
				bufinsert(p->n, &m);
			}
		}else{
			if(split(p->b, p, pp, &mid) == -1)
				goto error;
			b = p->l;
			for(i = p[-1].lo; i < p[-1].hi; i++){
				getmsg(p[-1].b, i, &m);
				if(keycmp(&m, &mid) >= 0)
					b = p->r;
				if(filledbuf(b, msgsz(&m)))
					continue;
				bufinsert(b, &m);
			}
		}
		pp = p;
	}
	if(path[1].split){
		if((path[0].n = newblk(Pivot)) == nil)
			goto error;
		copyup(path[0].n, 0, &path[1], nil);
		bufinsert(path[0].n, ins);
	}else{
		if(path[1].b->type == Leaf && !filledleaf(path[1].b, msgsz(ins)))
			apply(path[1].n, ins);
		else if(!filledbuf(path[1].n, msgsz(ins)))
			bufinsert(path[1].n, ins);
		else
			*redo = 1;
	}
	ret = 0;
error:
	for(p = path; p != path + npath; p++){
		if(p->b != nil)
			putblk(p->b);
		if(p->n != nil)
			putblk(p->n);
		if(p->l != nil)
			putblk(p->l);
		if(p->r != nil)
			putblk(p->r);
	}
	return ret;
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
	lo = 0;
	maxsz = 0;
	p->b = b;
	/* 
	 * Start at the second pivot: all values <= this
	 * go to the first node. Stop *after* the last entry,
	 * because entries >= the last entry all go into it.
	 */
	for(i = 1; i <= b->nent; i++){
		if(i < b->nent)
			getval(b, i, &kv);
		cursz = 0;
		for(; j < b->nmsg; j++){
			getmsg(b, j, &m);
			if(i < b->nent && keycmp(&m, &kv) >= 0)
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
			lo = j;
		}
	}
}

int
fsupsert(Msg *m)
{
	Blk *b, *n, *s;
	int npath, redo;
	Path *path;
	Kvp sep;

	if(m->nk + 2 > Keymax){
		werrstr("overlong key");
		return -1;
	}
	/*
	 * The tree can grow in height by 1 when we
	 * split, so we allocate room for one extra
	 * node in the path.
	 */
again:
	n = nil;
	s = nil;
	redo = 0;
	npath = 0;
	path = emalloc((fs->height + 2)*sizeof(Path));
	path[npath].b = nil;
	path[npath].idx = -1;
	npath++;

	b = fs->root;
	path[0].sz = msgsz(m);
	while(b->type == Pivot){
		if(!filledbuf(b, path[npath - 1].sz))
			break;
		victim(b, &path[npath]);
		getval(b, path[npath].idx, &sep);
		b = getblk(sep.bp, sep.bh);
		if(b == nil)
			goto error;
		npath++;
	}
	path[npath].b = b;
	path[npath].idx = -1;
	path[npath].lo = 0;
	path[npath].hi = 0;
	npath++;
	if(flush(path, npath, m, &redo) == -1)
		goto error;
	b = path[0].n;
	if(b != nil)
		fs->height++;
	else
		b = path[1].n;
	fs->root = b;
	free(path);
	if(redo)
		goto again;
	return 0;
error:
	if(n != nil) freeblk(n);
	if(s != nil) freeblk(s);
	free(path);
	return -1;
}

static char*
collect(Blk *b, Key *k, Kvp *r, int *done)
{
	int i, idx;
	Msg m;

	*done = 0;
	if(bufsearch(b, k, &m, &idx) != 0)
		return nil;
	for(i = idx; i < b->nmsg; i++){
		getmsg(b, i, &m);
		if(keycmp(&m, k) != 0)
			break;
		switch(m.op){
		case Ocreate:
			*r = m.Kvp;
			*done = 1;
			return nil;
		case Odelete:
			*done = 1;
			return Eexist;
		/* The others don't affect walks */
		}
	}
	return nil;
}

char*
fswalk1(Key *k, Kvp *r)
{
	int idx, same, done;
	char *ret;
	Blk *b;

	b = fs->root;
	while(b->type == Pivot){
		ret = collect(b, k, r, &done);
		if(done)
			return ret;
		if(blksearch(b, k, r, &idx, nil) == -1 || idx == -1)
			return Eexist;
		if((b = getblk(r->bp, r->bh)) == nil)
			return Efs;
	}
	assert(b->type == Leaf);
	if(blksearch(b, k, r, nil, &same) == -1 || !same)
		return Eexist;
	return nil;
}
