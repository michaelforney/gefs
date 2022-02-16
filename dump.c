#include <u.h>
#include <libc.h>
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
	if(k->nk == 0)
		return fmtprint(fmt, "\"\"");
	switch(k->k[0]){
	case Kdat:	/* qid[8] off[8] => ptr[16]:	pointer to data page */
		n = fmtprint(fmt, "dat qid:%llx off:%llx", UNPACK64(k->k+1), UNPACK64(k->k+9));
		break;
	case Kent:	/* pqid[8] name[n] => dir[n]:	serialized Dir */
		n = fmtprint(fmt, "ent dir:%llx, name:\"%.*s\")", UNPACK64(k->k+1), k->nk-11, k->k+11);
		break;
	case Klabel:	/* name[n] => tree[24]:	snapshot ref */
		n = fmtprint(fmt, "label name:\"%.*s\"", k->nk-1, k->k+1);
		break;
	case Ksnap:	/* name[n] => tree[24]:	snapshot root */
		n = fmtprint(fmt, "snap id:\"%llx\"", UNPACK64(k->k+1));
		break;
	case Ksuper:	/* qid[8] => pqid[8]:		parent dir */
		n = fmtprint(fmt, "up dir:%llx", UNPACK64(k->k+1));
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
		n = fmtprint(fmt, "(%B,%d)", unpackbp(v->v, v->nv), UNPACK16(v->v+Ptrsz));
		return n;
	}
	if(op == Odelete || op == Oclearb){
		n = fmtprint(fmt, "delete");
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
		break;
	case Kent:	/* pqid[8] name[n] => dir[n]:	serialized Dir */
		switch(op){
		case Onop:
		case Oinsert:
			if(kv2dir(v, &d) == -1)
				n = fmtprint(fmt, "bad dir");
			else
				n = fmtprint(fmt, "[qid=(%llux,%lud,%d), %luo, t=%lld,%lld, l=%lld]",
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
				n += fmtprint(fmt, "size:%llx ", UNPACK64(p));
				p += 8;
			}
			if(ws & Owmode){
				n += fmtprint(fmt, "mode:%uo ", UNPACK32(p));
				p += 4;
			}
			if(ws & Owmtime){
				n += fmtprint(fmt, "mtime:%llx ", UNPACK64(p));
				p += 8;
			}
			if(ws & Owatime){
				n += fmtprint(fmt, "mtime:%llx ", UNPACK64(p));
				p += 8;
			}
			if(ws & Owuid){
				n += fmtprint(fmt, "uid:%d ", UNPACK32(p));
				p += 4;
			}
			if(ws & Owgid){
				n += fmtprint(fmt, "gid:%d ", UNPACK32(p));
				p += 4;
			}
			if(ws & Owmuid){
				n += fmtprint(fmt, "muid:%d ", UNPACK32(p));
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
		if(unpacktree(&t, v->v, v->nv) == nil)
			return fmtprint(fmt, "corrupt tree");
		n = fmtprint(fmt, "ref: %d, ht: %d, bp: %B, prev=%lld", t.ref, t.ht, t.bp, t.dead[0].prev);
		break;
	case Klabel:
		n = fmtprint(fmt, "snap id:\"%llx\"", UNPACK64(v->v+1));
		break;
	case Ksuper:	/* qid[8] => pqid[8]:		parent dir */
		n = fmtprint(fmt, "super dir:%llx, name:\"%.*s\")", UNPACK64(v->v+1), v->nv-11, v->v+11);
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
	return fmtprint(fmt, "(%llx,%.16llux,%llx)", bp.addr, bp.hash, bp.gen);
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

static void
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
	switch(b->type){
	case Tpivot:
		for(i = 0; i < b->nbuf; i++){
			getmsg(b, i, &m);
			fprint(fd, "%.*s[%03d]|%M\n", 4*indent, spc, i, &m);
		}
		/* wet floor */
	case Tleaf:
		for(i = 0; i < b->nval; i++){
			getval(b, i, &kv);
			if(b->type == Tpivot){
				fprint(fd, "%.*s[%03d]|%#P\n", 4*indent, spc, i, &kv);
				bp = unpackbp(kv.v, kv.nv);
				if((c = getblk(bp, 0)) == nil)
					sysfatal("failed load: %r");
				if(recurse)
					rshowblk(fd, c, indent + 1, 1);
				dropblk(c);
			}else{
				fprint(fd, "%.*s[%03d]|%P\n", 4*indent, spc, i, &kv);
			}
		}
		break;
	case Tmagic:
		fprint(fd, "magic\n");
		break;
	case Tarena:
		fprint(fd, "arena -- ");
		goto Show;
	case Tlog:
		fprint(fd, "log -- ");
		goto Show;
	case Tdead:
		fprint(fd, "dead -- ");
		goto Show;
	case Traw:
		fprint(fd, "raw -- ");
	Show:
		for(i = 0; i < 32; i++){
			fprint(fd, "%x", b->buf[i] & 0xff);
			if(i % 4 == 3)
				fprint(fd, " ");
		}
		fprint(fd, "\n");
		break;
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
	if(strcmp(name, "snap") == 0)
		t = &fs->snap;
	else if((t = openlabel(name)) == nil){
		fprint(fd, "open %s: %r\n", name);
		return;
	}
	b = getroot(t, &h);
	fprint(fd, "=== [%s] %B @%d\n", name, t->bp, t->ht);
	rshowblk(fd, b, 0, 1);
	dropblk(b);
	if(t != &fs->snap)
		closesnap(t);
}

void
showbp(int fd, Bptr bp, int recurse)
{
	Blk *b;

	b = getblk(bp, GBnochk);
	rshowblk(fd, b, 0, recurse);
	dropblk(b);
}

static void
showdeadbp(Bptr bp, void *p)
{
	fprint(*(int*)p, "\t\t\t%B\n", bp);
}

void
showtreeroot(int fd, Tree *t)
{
	Dlist *dl;
	int i;

	fprint(fd, "\tgen:\t%lld\n", t->gen);
	fprint(fd, "\tref:\t%d\n", t->ref);
	fprint(fd, "\tht:\t%d\n", t->ht);
	fprint(fd, "\tbp:\t%B\n", t->bp);
	for(i = 0; i < Ndead; i++){
		dl = &t->dead[i];
		fprint(fd, "	deadlist[%d]:\n", i);
		fprint(fd, "		prev:	%llx\n", dl->prev);
		fprint(fd, "		fhead:	%B\n", dl->head);
		if(dl->ins != nil)
			fprint(fd, "		open:	%B\n", dl->ins->bp);
		fprint(fd, "		dead[%d]: (%B)\n", i, dl->head);
		scandead(&t->dead[i], 0, showdeadbp, &fd);
	}
}

void
showsnap(int fd, char **ap, int na)
{
	char *e, pfx[Snapsz];
	int sz, done;
	Mount *mnt;
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
		PACK64(pfx+1, id);
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
	qlock(&fs->snaplk);
	for(mnt = fs->mounts; mnt != nil; mnt = mnt->next){
		fprint(fd, "open: %s\n", mnt->name);
		showtreeroot(fd, mnt->root);
	}
	qunlock(&fs->snaplk);
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
