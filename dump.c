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
	case Klabel:	/* name[n] => tree[24]:	snapshot ref */
		n = fmtprint(fmt, "label name:\"%.*s\"", k->nk-1, k->k+1);
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
showval(Fmt *fmt, Kvp *v, int op, int flg)
{
	int n, ws;
	char *p;
	Tree t;
	Xdir d;

	n = 0;
	if(flg){
		assert(v->nv == Ptrsz+2);
		n = fmtprint(fmt, "(%B,%d)", unpackbp(v->v, v->nv), GBIT16(v->v+Ptrsz));
		return n;
	}
	switch(v->k[0]){
	case Kdat:	/* qid[8] off[8] => ptr[16]:	pointer to data page */
		switch(op){
		case Odelete:
		case Oclearb:
			n = 0;
			break;
		case Onop:
		case Oinsert:
			n = fmtprint(fmt, "ptr:%B", unpackbp(v->v, v->nv));
			break;
		}
	case Kent:	/* pqid[8] name[n] => dir[n]:	serialized Dir */
		switch(op){
		case Onop:
		case Oinsert:
			if(kv2dir(v, &d) == -1)
				n = fmtprint(fmt, "bad dir");
			else
				n = fmtprint(fmt, "[qid=(%lld,%lud,%d), %o, t=%lld,%lld, l=%lld]",
					d.qid.path, d.qid.vers, d.qid.type,
					d.mode, d.atime, d.mtime, d.length);
			break;
		case Odelete:
			n = fmtprint(fmt, "delete");
			break;
		case Owstat:
			p = v->v;
			ws = *p++;
			if(ws & Owsize){
				n += fmtprint(fmt, "size:%llx ", GBIT64(p));
				p += 8;
			}
			if(ws & Owmode){
				n += fmtprint(fmt, "mode:%o ", GBIT32(p));
				p += 4;
			}
			if(ws & Owmtime){
				n += fmtprint(fmt, "mtime:%llx ", GBIT64(p));
				p += 8;
			}
			if(ws & Owatime){
				n += fmtprint(fmt, "mtime:%llx ", GBIT64(p));
				p += 8;
			}
			if(ws & Owuid){
				n += fmtprint(fmt, "uid:%d ", GBIT32(p));
				p += 4;
			}
			if(ws & Owgid){
				n += fmtprint(fmt, "gid:%d ", GBIT32(p));
				p += 4;
			}
			if(ws & Owmuid){
				n += fmtprint(fmt, "muid:%d ", GBIT32(p));
				p += 4;
			}
			if(p != v->v + v->nv){
				fprint(2, "v->nv: %d, sz=%d\n", v->nv, (int)(p - v->v));
				abort();
			}
			break;
		}
		break;
	case Ksnap:	/* name[n] => dent[16] ptr[16]:	snapshot root */
		switch(op){
		case Orefsnap:
			n = fmtprint(fmt, "ref");
			break;
		case Ounrefsnap:
			n = fmtprint(fmt, "unref");
			break;
		default:
			if(unpacktree(&t, v->v, v->nv) == nil)
				return fmtprint(fmt, "corrupt tree");
			n = fmtprint(fmt, "ref: %d, ht: %d, bp: %B, prev=%lld", t.ref, t.ht, t.bp, t.prev[0]);
			break;
		}
		break;
	case Klabel:
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
	char *opname[Nmsgtype] = {
	[Oinsert]	"Oinsert",
	[Odelete]	"Odelete",
	[Oclearb]	"Oclearb",
	[Owstat]	"Owstat",
	[Orefsnap]	"Orefsnap",
	[Ounrefsnap]	"Ounrefsnap",
	};
	Msg *m;
	int f, n;

	f = (fmt->flags & FmtSharp) != 0;
	m = va_arg(fmt->args, Msg*);
	if(m == nil)
		return fmtprint(fmt, "Msg{nil}");
	n = fmtprint(fmt, "Msg(%s, ", opname[m->op]);
	n += showkey(fmt, m);
	n += fmtprint(fmt, ") => (");
	n += showval(fmt, m, m->op, f);
	n += fmtprint(fmt, ")");
	return n;
}

int
Pconv(Fmt *fmt)
{
	Kvp *kv;
	int f, n;

	f = (fmt->flags & FmtSharp) != 0;
	kv = va_arg(fmt->args, Kvp*);
	if(kv == nil)
		return fmtprint(fmt, "Kvp{nil}");
	n = fmtprint(fmt, "Kvp(");
	n += showkey(fmt, kv);
	n += fmtprint(fmt, ") => (");
	n += showval(fmt, kv, Onop, f);
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
	Bptr bp;
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
		if(b->type == Tpivot){
			fprint(fd, "%.*s[%03d]|%#P\n", 4*indent, spc, i, &kv);
			bp = unpackbp(kv.v, kv.nv);
			if((c = getblk(bp, 0)) == nil)
				sysfatal("failed load: %r");
			if(recurse)
				rshowblk(fd, c, indent + 1, 1);
			putblk(c);
		}else{
			fprint(fd, "%.*s[%03d]|%P\n", 4*indent, spc, i, &kv);
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
showtree(int fd, char **ap, int na)
{
	char *name;
	Tree *t;
	Blk *b;
	int h;

	name = "main";
	memset(&t, 0, sizeof(t));
	if(na == 1)
		name = ap[0];
	if(strcmp(name, "dump") == 0)
		t = &fs->snap;
	else if((t = openlabel(name)) == nil){
		fprint(fd, "open %s: %r\n", name);
		return;
	}
	b = getroot(t, &h);
	fprint(fd, "=== [%s] %B @%d\n", name, t->bp, t->ht);
	rshowblk(fd, b, 0, 1);
	putblk(b);
}

void
showbp(int fd, Bptr bp, int recurse)
{
	Blk *b;

	b = getblk(bp, 0);
	rshowblk(fd, b, 0, recurse);
	putblk(b);
}

static void
showdeadbp(Bptr bp, void *p)
{
	fprint(*(int*)p, "\t\t\t%B\n", bp);
}

void
showtreeroot(int fd, Tree *t)
{
	int i;

	fprint(fd, "\tgen:\t%lld\n", t->gen);
//	fprint(fd, "\tref:\t%d\n", t->ref);
//	fprint(fd, "\tht:\t%d\n", t->ht);
//	fprint(fd, "\tbp:\t%B\n", t->bp);
	for(i = 0; i < Ndead; i++){
		fprint(fd, "\tdeadlist[%d]: prev=%llx\n", i, t->prev[i]);
//		fprint(fd, "\t\tprev:\t%llx\n", t->prev[i]);
//		fprint(fd, "\t\tfhead:\t%B\n", t->dead[i].head);
//		if(t->dead[i].tail != nil){
//			fprint(fd, "\t\tftailp:%llx\n", t->dead[i].tail->bp.addr);
//			fprint(fd, "\t\tftailh:%llx\n", t->dead[i].tail->bp.hash);
//		}else{
//			fprint(fd, "\t\tftailp:\t-1\n");
//			fprint(fd, "\t\tftailh:\t-1\n");
//		}
//		fprint(fd, "\t\tdead[%d]: (%B)\n", i, t->dead[i].head);
//		scandead(&t->dead[i], showdeadbp, &fd);
	}
}

void
showsnap(int fd, char **ap, int na)
{
	char *e, pfx[Snapsz];
	int sz, done;
	vlong id;
	Scan *s;
	Tree t;

	if((s = mallocz(sizeof(Scan), 1)) == nil){
		fprint(fd, "no memory\n");
		return;
	}
	pfx[0] = Ksnap;
	sz = 1;
	if(na != 0){
		sz = Snapsz;
		id = atoll(ap[0]);
		PBIT64(pfx+1, id);
	}
	if((e = btscan(&fs->snap, s, pfx, sz)) != nil){
		fprint(fd, "scan: %s\n", e);
		btdone(s);
		return;
	}
	while(1){
		if((e = btnext(s, &s->kv, &done)) != nil){
			fprint(fd, "scan: %s\n", e);
			break;
		}
		if(done)
			break;
		fprint(fd, "snap: %P\n", &s->kv);
		if(unpacktree(&t, s->kv.v, s->kv.nv) == nil){
			fprint(fd, "unpack: garbled tree\n");
			break;
		}
		showtreeroot(fd, &t);
	}
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
#undef A
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
