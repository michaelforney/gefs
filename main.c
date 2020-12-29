#include <u.h>
#include <libc.h>
#include <bio.h>
#include "dat.h"
#include "fns.h"

Gefs *fs;

static int
Bconv(Fmt *fmt)
{
	Blk *b;

	b = va_arg(fmt->args, Blk*);
	if(b == nil)
		return fmtprint(fmt, "Blk(nil)");
	return fmtprint(fmt, "Blk(%c)", (b->type == Pivot) ? 'P' : 'L');
}

static int
Mconv(Fmt *fmt)
{
	char *opname[] = {
	[Ocreate]	"Ocreate",
	[Odelete]	"Odelete",
	[Owrite]	"Owrite",
	[Owstat]	"Owstat",
	};
	Msg *m;

	m = va_arg(fmt->args, Msg*);
	return fmtprint(fmt, "Msg(%s, %.*s,%.*s)", opname[m->op], m->nk, m->k, m->nv, m->v);
}

static int
Pconv(Fmt *fmt)
{
	Kvp *kv;

	kv = va_arg(fmt->args, Kvp*);
	if(kv->type == Vinl)
		return fmtprint(fmt, "Kvp(%.*s,%.*s)", kv->nk, kv->k, kv->nv, kv->v);
	else
		return fmtprint(fmt, "Kvp(%.*s,(%llx,%llx))", kv->nk, kv->k, kv->bp, kv->bh);
}

static int
Kconv(Fmt *fmt)
{
	Key *k;

	k = va_arg(fmt->args, Key*);
	return fmtprint(fmt, "Key(%.*s)", k->nk, k->k);
}

static void
init(void)
{
	initshow();
	quotefmtinstall();
	fmtinstall('B', Bconv);
	fmtinstall('M', Mconv);
	fmtinstall('P', Pconv);
	fmtinstall('K', Kconv);
	fs = emalloc(sizeof(Gefs));
	fs->root = newblk(Leaf);
	fs->height = 1;
}

int
test(char *path)
{
	Biobuf *bfd;
	char *e, *ln, *f[3];
	int nf;
	Msg m;
	Kvp r;
	Key k;

	if((bfd = Bopen(path, OREAD)) == nil)
		sysfatal("open %s: %r", path);
	while((ln = Brdstr(bfd, '\n', 1)) != nil){
		memset(f, 0, sizeof(f));
		nf = tokenize(ln, f, nelem(f));
		if(nf < 1 || strlen(f[0]) != 1)
			sysfatal("malformed test file");
		switch(*f[0]){
		case '#':
			break;
		case 'I':
			if(nf != 3)
				sysfatal("malformed insert");
			m.type = Vinl;
			m.k = f[1];
			m.v = f[2];
			m.op = Ocreate;
			m.nk = strlen(f[1]);
			m.nv = strlen(f[2]);
			print("insert (%s, %s)\n", m.k, m.v);
			if(fsupsert(&m) == -1){
				print("failed insert (%s, %s): %r\n", f[1], f[2]);
				return -1;
			}
			break;
		case 'D':
			if(nf != 2)
				sysfatal("malformed delete");
			m.type = Vinl;
			m.op = Odelete;
			m.k = f[1];
			m.v = nil;
			m.nk = strlen(f[1]);
			m.nv = 0;
			print("delete %s\n", f[1]);
			if(fsupsert(&m) == -1){
				print("failed delete (%s): %r\n", f[1]);
				return -1;
			}
			break;
		case 'G':
			k.k = f[1];
			k.nk = strlen(f[1]);
			e = fswalk1(&k, &r);
			if(e != nil){
				print("failed lookup on (%s): %s\n", f[1], e);
				return -1;
			}
			break;
		case 'S':
			showfs("fs");
			print("\n\n");
			break;
		case 'C':
			checkfs();
			break;
		case 'V':
			debug++;
			break;
		case 'X':
			exits(f[1]);
			break;
		}
//		if(!checkfs())
//			abort();
		free(ln);
	}
	return 0;
}

void
main(int argc, char **argv)
{
	int i;

	ARGBEGIN{
	}ARGEND;

	init();
	for(i = 0; i < argc; i++)
		if(test(argv[0]) == -1)
			sysfatal("test %s: %r\n", argv[i]);
	exits(nil);
}
