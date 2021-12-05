#include <u.h>
#include <libc.h>
#include <bio.h>
#include <avl.h>
#include <fcall.h>
#include <ctype.h>

#include "dat.h"
#include "fns.h"

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
	case Ksnap:	/* name[n] => tree[24]:	snapshot root */
		n = fmtprint(fmt, "snap name:\"%.*s\"", k->nk-1, k->k+1);
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
showval(Fmt *fmt, Kvp *v, int op)
{
	char *p;
	Bptr bp;
	Dir d;
	int n, ht;

	n = 0;
	switch(v->k[0]){
	case Kdat:	/* qid[8] off[8] => ptr[16]:	pointer to data page */
		bp.addr = GBIT64(v->v+0);
		bp.hash = GBIT64(v->v+8);
		bp.gen = GBIT64(v->v+16);
		n = fmtprint(fmt, "ptr:%B", bp);
		break;
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
			if(op & Owmtime){
				n += fmtprint(fmt, "mtime:%llx ", GBIT64(p));
				p += 8;
			}
			if(op & Owsize){
				n += fmtprint(fmt, "size:%llx ", GBIT64(p));
				p += 8;
			}
			if(op & Owmode){
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
	[Oqdelete]	"Oqdelete",
	[Owstat]	"Owstat",
	};
	Msg *m;
	int n;

	m = va_arg(fmt->args, Msg*);
	if(m == nil)
		return fmtprint(fmt, "Msg{nil}");
	n = fmtprint(fmt, "Msg(%s, ", opname[m->op]);
	n += showkey(fmt, m);
	n += fmtprint(fmt, ") => (");
	n += showval(fmt, m, m->statop);
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
		n += showval(fmt, kv, Onop);
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
