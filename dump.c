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
	case Ksnap:	/* name[n] => dent[16] ptr[16]:	snapshot root */
		n = fmtprint(fmt, "snap dent:%llx ptr:%llx", GBIT64(k->k+1), GBIT64(k->k+9));
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
	Dir d;
	int n;

	n = 0;
	switch(v->k[0]){
	case Kdat:	/* qid[8] off[8] => ptr[16]:	pointer to data page */
		n = fmtprint(fmt, "blk:%llx, hash:%llx", GBIT64(v->v), GBIT64(v->v+8));
		break;
	case Kent:	/* pqid[8] name[n] => dir[n]:	serialized Dir */
		switch(op&0xf){
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
		}
		break;
	case Ksnap:	/* name[n] => dent[16] ptr[16]:	snapshot root */
		n = fmtprint(fmt, "blk:%llx, hash:%llx", GBIT64(v->v), GBIT64(v->v+8));
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
	[Owstat]	"Owstat",
	};
	Msg *m;
	int n;

	m = va_arg(fmt->args, Msg*);
	n = fmtprint(fmt, "Msg(%s, ", opname[m->op&0xf]);
	n += showkey(fmt, m);
	n += fmtprint(fmt, ") => (");
	n += showval(fmt, m, m->op);
	n += fmtprint(fmt, ")");
	return n;
}

int
Pconv(Fmt *fmt)
{
	Kvp *kv;
	int n;

	kv = va_arg(fmt->args, Kvp*);
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
