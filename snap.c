#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"
#include "atomic.h"

int
scandead(Dlist *l, int lblk, void (*fn)(Bptr, void*), void *dat)
{
	char *p;
	int i, op;
	Dlist t;
	Bptr bp;
	Blk *b;

	if(l->ins != nil)
		b = l->ins;
	else if(l->head.addr != -1)
		b = getblk(l->head, 0);
	else
		return 0;
	if(b == nil)
		return -1;
Nextblk:
	for(i = Loghashsz; i < Logspc; i += 16){
		p = b->data + i;
		op = UNPACK64(p) & 0xff;
		switch(op){
		case DlEnd:
			return 0;
		case DlChain:
			bp.addr = UNPACK64(p);	p += 8;
			bp.addr &= ~0xffULL;
			bp.hash = UNPACK64(p);
			bp.gen = -1;
			if(lblk)
				fn(b->bp, dat);
			if((b = getblk(bp, 0)) == nil)
				return -1;
			goto Nextblk;
		case DlGraft:
			t.head.addr = UNPACK64(p);	p += 8;
			t.head.addr &= ~0xffULL;
			t.head.hash = UNPACK64(p);
			t.head.gen = -1;
			t.ins = nil;
			scandead(&t, lblk, fn, dat);
			break;
		case DlKill:
			bp.addr = UNPACK64(p);	p += 8;
			bp.hash = -1;
			bp.gen = UNPACK64(p);
			bp.addr &= ~0xffULL;
			fn(bp, dat);
			break;
		default:
			fprint(2, "bad op=%d\n", op);
			abort();
		}
	}
	return 0;
}

/*
 * Insert into a deadlist. Because the only
 * operations are chaining deadlist pages
 * and killing blocks, we don't preserve the
 * order of kills.
 */
static int
dlinsert(Dlist *dl, vlong v1, vlong v2)
{
	Blk *lb, *pb;
	vlong end, hash;
	char *p;

	lb = dl->ins;
	if(lb == nil && dl->head.addr != -1)
		lb = getblk(dl->head, 0);
	/*
	 * move to the next block when we have
	 * 32 bytes in the log:
	 * We're appending up to 16 bytes as
	 * part of the operation, followed by
	 * 16 bytes of chaining.
	 */
	if(lb == nil || lb->logsz >= Logspc - 40){
		pb = lb;
		if((lb = newblk(Tdead)) == nil)
			return -1;
		if(pb != nil){
			finalize(pb);
			if(syncblk(pb) == -1)
				return -1;
			dl->head = pb->bp;
		}
		lb->logsz = Loghashsz;
		dl->ins = lb;
		dropblk(pb);
	}
	p = lb->data + lb->logsz;
	PACK64(p, v1);	p += 8;
	PACK64(p, v2);	p += 8;
	if(dl->head.addr == -1){
		end = DlEnd;
		hash = -1;
	}else{
		end = dl->head.addr|DlChain;
		hash = dl->head.hash;
	}
	PACK64(p+0, end);
	PACK64(p+8, hash);
	lb->logsz = (p - lb->data);
	return 0;
}

static int
graft(Dlist *dst, Dlist *src)
{
	if(src->ins != nil){
		finalize(src->ins);
		if(syncblk(src->ins) == -1)
			return -1;
		src->head = src->ins->bp;
		src->ins = nil;
	}
	if(src->head.addr == -1)
		return 0;
	return dlinsert(dst, src->head.addr|DlGraft, src->head.hash);
}

int
killblk(Tree *t, Bptr bp)
{
	Dlist *dl;
	int i;

	dl = &t->dead[0];
	for(i = 0; i < Ndead; i++){
		if(t->dead[i].prev <= bp.gen)
			break;
		dl = &t->dead[i];
	}
	dlinsert(dl, bp.addr|DlKill, bp.gen);
	return 0;
}

Tree*
openlabel(char *name)
{
	char *p, buf[Kvmax];
	Kvp kv;
	Key k;

	if((p = packlabel(buf, sizeof(buf), name)) == nil)
		return nil;
	k.k = buf;
	k.nk = p - buf;
	if(btlookup(&fs->snap, &k, &kv, buf, sizeof(buf)) != nil)
		return nil;
	if(kv.nv != Snapsz)
		return nil;
	return opensnap(UNPACK64(kv.v + 1));
}

Tree*
opensnap(vlong id)
{
	char *p, buf[Kvmax];
	Tree *t;
	Kvp kv;
	Key k;

	qlock(&fs->snaplk);
	for(t = fs->opensnap; t != nil; t = t->snext){
		if(t->gen == id){
			ainc(&t->memref);
			qunlock(&fs->snaplk);
			return t;
		}
	}
	if((t = mallocz(sizeof(Tree), 1)) == nil)
		goto Error;
	memset(&t->lk, 0, sizeof(t->lk));

	if((p = packsnap(buf, sizeof(buf), id)) == nil)
		goto Error;
	k.k = buf;
	k.nk = p - buf;
	if(btlookup(&fs->snap, &k, &kv, buf, sizeof(buf)) != nil)
		goto Error;
	if(unpacktree(t, kv.v, kv.nv) == nil)
		goto Error;
	t->memref = 1;
	t->snext = fs->opensnap;
	fs->opensnap = t;
	qunlock(&fs->snaplk);
	return t;

Error:
	qunlock(&fs->snaplk);
	return nil;
}

void
closesnap(Tree *t)
{
	Tree *te, **p;
	Blk *ins;
	int i;

	if(adec(&t->memref) != 0)
		return;

	for(i = Ndead-1; i >= 0; i--){
		ins = t->dead[i].ins;
		if(ins == nil)
			continue;
		finalize(ins);
		syncblk(ins);
		t->dead[i].head = ins->bp;
//FIXME: 	putblk(ins);
	}

	p = &fs->opensnap;
	for(te = fs->opensnap; te != nil; te = te->snext){
		if(te == t)
			break;
		p = &te->snext;
	}
	assert(te != nil);
	*p = te->snext;
	free(te);
}

static char*
modifysnap(int op, Tree *t)
{
	char kbuf[Snapsz], vbuf[Treesz];
	char *p, *e;
	Msg m;
	int i;

	for(i = 0; i < Ndead; i++){
		if(t->dead[i].ins != nil){
			finalize(t->dead[i].ins);
			syncblk(t->dead[i].ins);
		}
	}
	m.op = op;
	if((p = packsnap(kbuf, sizeof(kbuf), t->gen)) == nil)
		return Elength;
	m.k = kbuf;
	m.nk = p - kbuf;
	if(op == Oinsert){
		if((p = packtree(vbuf, sizeof(vbuf), t)) == nil)
			return Elength;
		m.v = vbuf;
		m.nv = p - vbuf;
	}else{
		m.v = nil;
		m.nv = 0;
	}
	if((e = btupsert(&fs->snap, &m, 1)) != nil)
		return e;
	return nil;
}

static char*
modifylabel(int op, char *name, vlong id)
{
	char *p, *e, kbuf[Keymax], vbuf[Snapsz];
	Msg m;

	if(strcmp(name, "dump") == 0)
		return Ename;
	if(op == Oinsert)
		e = refsnap(id);
	else
		e = unrefsnap(id, -1);
	if(e != nil)
		return e;

	m.op = op;
	if((p = packlabel(kbuf, sizeof(kbuf), name)) == nil)
		return Elength;
	m.k = kbuf;
	m.nk = p - kbuf;
	if((p = packsnap(vbuf, sizeof(vbuf), id)) == nil)
		return Elength;
	m.v = vbuf;
	m.nv = p - vbuf;

	if((e = btupsert(&fs->snap, &m, 1)) != nil)
		return e;
	return nil;
}

char*
labelsnap(char *name, vlong gen)
{
	return modifylabel(Oinsert, name, gen);
}

char*
unlabelsnap(vlong gen, char *name)
{
	return modifylabel(Odelete, name, gen);
}

char*
refsnap(vlong id)
{
	Tree *t;
	char *e;

	t = opensnap(id);
	t->ref++;
	if((e = modifysnap(Oinsert, t)) != nil)
		return e;
	closesnap(t);
	return nil;
}

char*
unrefsnap(vlong id, vlong succ)
{
	Tree *t, *u;
	char *e;

	if((t = opensnap(id)) == nil)
		return Eexist;
	if(--t->ref == 0){
		if((u = opensnap(succ)) == nil)
			return Eexist;
		if((e = freesnap(t, u)) != nil)
			return e;
		if((e = modifysnap(Odelete, t)) != nil)
			return e;
		if((e = modifysnap(Oinsert, u)) != nil)
			return e;
		closesnap(u);
		closesnap(t);
	}else{
		if((e = modifysnap(Oinsert, t)) != nil)
			return e;
		closesnap(t);
	}
	return nil;
}

static void
freedead(Bptr bp, void *)
{
	freebp(nil, bp);
}

static void
redeadlist(Bptr bp, void *pt)
{
	killblk(pt, bp);
}

char*
freesnap(Tree *snap, Tree *next)
{
	int i, j;

	assert(snap->gen != next->gen);
	assert(next->dead[0].prev == snap->gen);

	/* free blocks born after snap's prev and killed in next */
	for(i = 0; i < Ndead-1; i++){
		if(next->dead[i].prev <= snap->dead[0].prev)
			break;
		scandead(&next->dead[i], 1, freedead, nil);
	}

	for(j = 0; j < Ndead; j++){
		next->dead[j] = snap->dead[j];
		if(next->dead[j].prev == -1)
			continue;
		for(; i < Ndead; i++){
			if(next->dead[i].prev < next->dead[j].prev)
				break;
			if(i == Ndead-1 && next->dead[i].prev > next->dead[j].prev)
				scandead(&next->dead[i], 0, redeadlist, next);
			else if(graft(&next->dead[j], &next->dead[i]) == -1)
				return Efs;
		}
	}

	return nil;
}

Tree*
newsnap(Tree *t)
{
	vlong gen;
	Tree *r;
	int i;

	if(t->dirty && modifysnap(Oinsert, t) != nil)
		return nil;

	if((r = calloc(sizeof(Tree), 1)) == nil)
		return nil;
	gen = aincv(&fs->nextgen, 1);
	memset(&r->lk, 0, sizeof(r->lk));
	r->snext = fs->opensnap;
	r->memref = 1;
	r->ref = 0;
	r->ht = t->ht;
	r->bp = t->bp;
	r->gen = gen;
	r->dirty = 0;
	/* shift deadlist down */
	for(i = Ndead-1; i >= 0; i--){
		r->dead[i].prev = (i == 0) ? t->gen : t->dead[i-1].prev;
		r->dead[i].head.addr = -1;
		r->dead[i].head.hash = -1;
		r->dead[i].head.gen = -1;
		r->dead[i].ins = nil;
	}
	fs->opensnap = r;

	return r;
}
