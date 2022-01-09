#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"

uvlong
inc64(uvlong *v, uvlong dv)
{
	vlong ov, nv;

	while(1){
		ov = *v;
		nv = ov + dv;
		if(cas64((u64int*)v, ov, nv))
			return ov;
	}
}

Tree*
openlabel(char *name)
{
	char *p, buf[Keymax];
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
	return opensnap(GBIT64(kv.v + 1));
}

Tree*
opensnap(vlong id)
{
	char *p, buf[Kvmax];
	Tree *t;
	Kvp kv;
	Key k;

	qlock(&fs->snaplk);
	for(t = fs->osnap; t != nil; t = t->snext){
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
	t->snext = fs->osnap;
	fs->osnap = t;
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
	int i;

	if(adec(&t->memref) != 0)
		return;

	for(i = Ndead-1; i >= 0; i--){
		if(t->dead[i].tail == nil)
			continue;
		finalize(t->dead[i].tail);
		syncblk(t->dead[i].tail);
//FIXME: 	putblk(t->dead[i].tail);
	}

	p = &fs->osnap;
	for(te = fs->osnap; te != nil; te = te->snext){
		if(te == t)
			break;
		p = &te->snext;
	}
	assert(te != nil);
	*p = te->snext;
	free(te);
}

char*
modifysnap(int op, Tree *t)
{
	char kbuf[Snapsz], vbuf[Treesz];
	char *p, *e;
	Msg m;
	int i;

	for(i = 0; i < Ndead; i++){
		if(t->dead[i].tail != nil){
			finalize(t->dead[i].tail);
			syncblk(t->dead[i].tail);
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

void
freedead(Bptr bp, void *)
{
	dprint("reclaimed deadlist: %B\n", bp);
	reclaimblk(bp);
}

void
redeadlist(Bptr bp, void *pt)
{
	killblk(pt, bp);
}

char*
freesnap(Tree *snap, Tree *next)
{
	Oplog dl;
	int i;

	assert(snap->gen != next->gen);
	assert(next->prev[0] == snap->gen);

	dl = next->dead[Ndead-1];
	scandead(&next->dead[0], freedead, nil);
	for(i = 0; i < Ndead-2; i++){
		if(graft(&snap->dead[i], &next->dead[i+1]) == -1)
			return Efs;
		next->prev[i] = snap->prev[i];
		next->dead[i] = snap->dead[i];
	}
	for(; i < Ndead; i++){
		next->prev[i] = snap->prev[i];
		next->dead[i] = snap->dead[i];
	}
	scandead(&dl, redeadlist, next);

	return nil;
}

Tree*
newsnap(Tree *t)
{
	vlong gen;
	Tree *r;
	int i;

	if(modifysnap(Oinsert, t) != nil)
		return nil;

	if((r = malloc(sizeof(Tree))) == nil)
		return nil;
	gen = inc64(&fs->nextgen, 1);
	memset(&r->lk, 0, sizeof(r->lk));
	r->snext = fs->osnap;
	r->memref = 1;
	r->ref = 0;
	r->ht = t->ht;
	r->bp = t->bp;
	r->gen = gen;
	r->dirty = 0;
	/* shift deadlist down */
	for(i = Ndead-1; i >= 0; i--){
		r->prev[i] = i == 0 ? t->gen : t->prev[i-1];
		r->dead[i].head.addr = -1;
		r->dead[i].head.hash = -1;
		r->dead[i].head.gen = -1;
		r->dead[i].tail = nil;
	}
	fs->osnap = r;

	return r;
}
