#include <u.h>
#include <libc.h>
#include <bio.h>
#include <avl.h>
#include <fcall.h>
#include <ctype.h>

#include "dat.h"
#include "fns.h"

Gefs *fs;

int	ream;
int	debug;
char	*srvname = "gefs";

static int
Bconv(Fmt *fmt)
{
	Bptr bp;

	bp = va_arg(fmt->args, Bptr);
	return fmtprint(fmt, "(%llx,%llx,%llx)", bp.addr, bp.hash, bp.gen);
}

void
launch(void (*f)(void *), void *arg, char *text)
{
	int pid;


	pid = rfork(RFPROC|RFMEM|RFNOWAIT);
	if (pid < 0)
		sysfatal("can't fork: %r");
	if (pid == 0) {
		procsetname("%s", text);
		(*f)(arg);
		exits("child returned");
	}
}

static int
Mconv(Fmt *fmt)
{
	char *opname[] = {
	[Oinsert]	"Oinsert",
	[Odelete]	"Odelete",
	[Owstat]	"Owstat",
	};
	Msg *m;

	m = va_arg(fmt->args, Msg*);
	return fmtprint(fmt, "Msg(%s, [%d]%.*X,[%d]%.*X)",
		opname[m->op&0xf],
		m->nk, m->nk, m->k,
		m->nv, m->nv, m->v);
}

static int
Pconv(Fmt *fmt)
{
	Kvp *kv;

	kv = va_arg(fmt->args, Kvp*);
	if(kv->type == Vinl)
		return fmtprint(fmt, "Kvp([%d]%.*X,[%d]%.*X)",
			kv->nk, kv->nk, kv->k,
			kv->nv, kv->nv, kv->v);
	else
		return fmtprint(fmt, "Kvp([%d]%.*X,(%B,%ud))",
			kv->nk, kv->nk, kv->k, kv->bp, kv->fill);
}

static int
Rconv(Fmt *fmt)
{
	Arange *r;

	r = va_arg(fmt->args, Arange*);
	if(r == nil)
		return fmtprint(fmt, "<Arange:nil>");
	else
		return fmtprint(fmt, "Arange(%lld+%lld)", r->off, r->len);
}

static int
Kconv(Fmt *fmt)
{
	Key *k;

	k = va_arg(fmt->args, Key*);
	return fmtprint(fmt, "Key([%d]%.*X)", k->nk, k->nk, k->k);
}

static int
Xconv(Fmt *fmt)
{
	char *s, *e;
	int n, i;

	n = 0;
	i = fmt->prec;
	s = va_arg(fmt->args, char*);
	e = s + fmt->prec;
	for(; s != e; s++){
		if(i % 4 == 0 && i != 0)
			n += fmtprint(fmt, ":");
		i--;
		if(isalnum(*s))
			n += fmtrune(fmt, *s);
		else
			n += fmtprint(fmt, "%02x", *s&0xff);
	}
	return n;
}

void
initfs(vlong cachesz)
{
	if((fs = mallocz(sizeof(Gefs), 1)) == nil)
		sysfatal("malloc: %r");

	fs->cmax = cachesz/Blksz;
	if(fs->cmax >= (2*GiB)/sizeof(Bucket))
		sysfatal("cache too big");
	if((fs->cache = mallocz(fs->cmax*sizeof(Bucket), 1)) == nil)
		sysfatal("malloc: %r");
}

int
postfd(char *name, char *suff)
{
	char buf[80];
	int fd[2];
	int cfd;

	if(pipe(fd) < 0)
		sysfatal("can't make a pipe");
	snprint(buf, sizeof buf, "/srv/%s%s", name, suff);
	if((cfd = create(buf, OWRITE|ORCLOSE|OCEXEC, 0600)) == -1)
		sysfatal("create %s: %r", buf);
	if(fprint(cfd, "%d", fd[0]) == -1)
		sysfatal("write %s: %r", buf);
	close(fd[0]);
	return fd[1];
}

void
usage(void)
{
	fprint(2, "usage: %s [-r] dev\n", argv0);
	exits("usage");
}

void
main(int argc, char **argv)
{
	int srvfd, ctlfd;
	vlong cachesz;

	cachesz = 16*MiB;
	ARGBEGIN{
	case 'r':
		ream = 1;
		break;
	case 'c':
		cachesz = strtoll(EARGF(usage()), nil, 0)*MiB;
		break;
	case 'd':
		debug++;
		break;
	case 's':
		srvname = EARGF(usage());
		break;
	default:
		usage();
		break;
	}ARGEND;
	if(argc == 0)
		usage();

	/*
	 * sanity checks -- I've tuned these to stupid
	 * values in the past.
	 */
//	assert(4*Kpmax < Pivspc);
//	assert(2*Msgmax < Bufspc);

	initfs(cachesz);
	initshow();
	quotefmtinstall();
	fmtinstall('B', Bconv);
	fmtinstall('M', Mconv);
	fmtinstall('P', Pconv);
	fmtinstall('K', Kconv);
	fmtinstall('R', Rconv);
	fmtinstall('X', Xconv);
	fmtinstall('F', fcallfmt);
	if(ream){
		reamfs(argv[0]);
		exits(nil);
	}else{
		fs->rdchan = mkchan(128);
		fs->wrchan = mkchan(128);
		srvfd = postfd(srvname, "");
		ctlfd = postfd(srvname, ".cmd");
		loadfs(argv[0]);
		launch(runctl, (void*)ctlfd, "ctl");
		launch(runwrite, nil, "writeio");
		launch(runread, nil, "readio");
//		launch(runfs, (void*)srvfd, "fs");
		runfs((void*)srvfd);
//		launch(syncproc, nil, "sync");
//		launch(flushproc, &fs->flushev, "flush");
//		for(i = 1; i < argc; i++)
//			if(test(argv[i]) == -1)
//				sysfatal("test %s: %r\n", argv[i]);
		exits(nil);
	}
}
