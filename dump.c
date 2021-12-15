#include <u.h>
#include <libc.h>
#include <bio.h>
#include <avl.h>
#include <fcall.h>
#include <ctype.h>

#include "dat.h"
#include "fns.h"

char	spc[128];

static int
showkey(Fmt *fmt, Key *k)
{
	int n;

	/*
	 * dent: pqid[8] qid[8] -- a directory entry key.
	 * ptr:  off[8] hash[8] -- a key for an Dir block.
	 * dir:  fixed statbuf header, user ids
	 */
	switch(k->k[0]){
	case Kdat:	/* qid[8] off[8] => ptr[16]:	pointer to data page */
		n = fmtprint(fmt, "dat qid:%llx off:%llx", GBIT64(k->k+1), GBIT64(k->k+9));
		break;
	case Kent:	/* pqid[8] name[n] => dir[n]:	serialized Dir */
		n = fmtprint(fmt, "ent dir:%llx, name:\"%.*s\")", GBIT64(k->k+1), k->nk-11, k->k+11);
		break;
	case Kdset:	/* name[n] => tree[24]:	snapshot ref */
		n = fmtprint(fmt, "dset name:\"%.*s\"", k->nk-1, k->k+1);
		break;
	case Ksnap:	/* name[n] => tree[24]:	snapshot root */
		n = fmtprint(fmt, "snap id:\"%llx\"", GBIT64(k->k+1));
		break;
	case Ksuper:	/* qid[8] => pqid[8]:		parent dir */
		n = fmtprint(fmt, "up parent:%llx ptr:%llx", GBIT64(k->k+1), GBIT64(k->k+9));
		break;
	default:
		n = fmtprint(fmt, "%.*H", k->nk, k->k);
		break;
	}
	return n;
}

static int
showval(Fmt *fmt, Kvp *v, int op, int statop)
{
	char *p;
	Bptr bp;
	Dir d;
	int n, ht;

	n = 0;
	switch(v->k[0]){
	case Kdat:	/* qid[8] off[8] => ptr[16]:	pointer to data page */
		switch(op){
		case Odelete:
		case Oclearb:
			n = 0;
			break;
		case Onop:
		case Oinsert:
			bp.addr = GBIT64(v->v+0);
			bp.hash = GBIT64(v->v+8);
			bp.gen = GBIT64(v->v+16);
			n = fmtprint(fmt, "ptr:%B", bp);
			break;
		}
	case Kent:	/* pqid[8] name[n] => dir[n]:	serialized Dir */
		switch(op){
		case Onop:
		case Oinsert:
			if(kv2dir(v, &d) == -1)
				n = fmtprint(fmt, "bad dir");
			else
				n = fmtprint(fmt, "[qid=(%lld,%lud,%d), %lo, t=%lud,%lud, l=%lld]",
					d.qid.path, d.qid.vers, d.qid.type,
					d.mode, d.atime, d.mtime, d.length);
			break;
		case Odelete:
			n = fmtprint(fmt, "delete");
			break;
		case Owstat:
			p = v->v;
			if(statop & Owmtime){
				n += fmtprint(fmt, "mtime:%llx ", GBIT64(p));
				p += 8;
			}
			if(statop & Owsize){
				n += fmtprint(fmt, "size:%llx ", GBIT64(p));
				p += 8;
			}
			if(statop & Owmode){
				n += fmtprint(fmt, "mode:%o ", GBIT32(p));
				p += 4;
			}
			if(p != v->v + v->nv)
				abort();
			break;
		}
		break;
	case Ksnap:	/* name[n] => dent[16] ptr[16]:	snapshot root */
		ht = GBIT32(v->v);
		bp.addr = GBIT64(v->v+4);
		bp.hash = GBIT64(v->v+12);
		bp.gen = GBIT64(v->v+20);
		n = fmtprint(fmt, "ht:%d, ptr:%B", ht, bp);
		break;
	case Kdset:
		n = fmtprint(fmt, "snap id:\"%llx\"", GBIT64(v->v+1));
		break;
	case Ksuper:	/* qid[8] => pqid[8]:		parent dir */
		n = fmtprint(fmt, "parent: %llx", GBIT64(v->v));
		break;
	default:
		n = fmtprint(fmt, "%.*H", v->nk, v->k);
		break;
	}
	return n;

}

int
Bconv(Fmt *fmt)
{
	Bptr bp;

	bp = va_arg(fmt->args, Bptr);
	return fmtprint(fmt, "(%llx,%llx,%llx)", bp.addr, bp.hash, bp.gen);
}

int
Mconv(Fmt *fmt)
{
	char *opname[] = {
	[Oinsert]	"Oinsert",
	[Odelete]	"Odelete",
	[Oclearb]	"Oclearb",
	[Owstat]	"Owstat",
	};
	Msg *m;
	int n, o;

	o = 0;
	m = va_arg(fmt->args, Msg*);
	if(m == nil)
		return fmtprint(fmt, "Msg{nil}");
	if(m->op == Owstat)
		o = m->v[0];
	n = fmtprint(fmt, "Msg(%s, ", opname[m->op]);
	n += showkey(fmt, m);
	n += fmtprint(fmt, ") => (");
	n += showval(fmt, m, m->op, o);
	n += fmtprint(fmt, ")");
	return n;
}

int
Pconv(Fmt *fmt)
{
	Kvp *kv;
	int n;

	kv = va_arg(fmt->args, Kvp*);
	if(kv == nil)
		return fmtprint(fmt, "Kvp{nil}");
	n = fmtprint(fmt, "Kvp(");
	n += showkey(fmt, kv);
	n += fmtprint(fmt, ") => (");
	if(kv->type == Vinl)
		n += showval(fmt, kv, Onop, 0);
	else
		n += fmtprint(fmt, "(%B,%ud))", kv->bp, kv->fill);
	n += fmtprint(fmt, ")");
	return n;
}

int
Kconv(Fmt *fmt)
{
	Key *k;
	int n;

	k = va_arg(fmt->args, Key*);
	if(k == nil)
		return fmtprint(fmt, "Key{nil}");
	n = fmtprint(fmt, "Key(");
	n += showkey(fmt, k);
	n += fmtprint(fmt, ")");
	return n;
}

int
Rconv(Fmt *fmt)
{
	Arange *r;

	r = va_arg(fmt->args, Arange*);
	if(r == nil)
		return fmtprint(fmt, "<Arange:nil>");
	else
		return fmtprint(fmt, "Arange(%lld+%lld)", r->off, r->len);
}

int
Qconv(Fmt *fmt)
{
	Qid q;

	q = va_arg(fmt->args, Qid);
	return fmtprint(fmt, "(%llx %ld %d)", q.path, q.vers, q.type);
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
	fprint(fd, "%.*s[BLK]|{%B}\n", 4*indent, spc, b->bp);
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
	char *e, *name;
	Tree t;

	name = "main";
	memset(&t, 0, sizeof(t));
	if(na == 1)
		name = ap[0];
	if((e = opensnap(&t, name)) != nil){
		fprint(fd, "open %s: %s\n", name, e);
		return;
	}
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

void
initshow(void)
{
	int i;

	memset(spc, ' ', sizeof(spc));
	for(i = 0; i < sizeof(spc); i += 4)
		spc[i] = '|';
}
