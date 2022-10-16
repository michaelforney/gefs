#include <u.h>
#include <libc.h>
#include <avl.h>
#include <fcall.h>

#include "dat.h"
#include "fns.h"

Gefs *fs;

int	ream;
int	debug;
int	noauth;
int	nproc;
char	*forceuser;
char	*srvname = "gefs";
char	*dev;
vlong	cachesz = 512*MiB;

vlong
inc64(vlong *v, vlong dv)
{
	vlong ov, nv;

	while(1){
		ov = *v;
		nv = ov + dv;
		if(cas64((u64int*)v, ov, nv))
			return ov;
	}
}

static void
initfs(vlong cachesz)
{
	Blk *b, *buf;

	if((fs = mallocz(sizeof(Gefs), 1)) == nil)
		sysfatal("malloc: %r");

	fs->lrurz.l = &fs->lrulk;
	fs->syncrz.l = &fs->synclk;
	fs->noauth = noauth;
	fs->cmax = cachesz/Blksz;
	if(fs->cmax > (1<<30))
		sysfatal("cache too big");
	if((fs->cache = mallocz(fs->cmax*sizeof(Bucket), 1)) == nil)
		sysfatal("malloc: %r");

	/* leave room for corruption check magic */
	buf = sbrk(fs->cmax * sizeof(Blk));
	if(buf == (void*)-1)
		sysfatal("sbrk: %r");
	for(b = buf; b != buf+fs->cmax; b++){
		b->bp.addr = -1;
		b->bp.hash = -1;
		b->magic = Magic;
		lrutop(b);
	}
	fs->blks = buf;
}

static void
launch(void (*f)(int, void *), int wid, void *arg, char *text)
{
	int pid;

	assert(wid == -1 || wid < nelem(fs->active));
	pid = rfork(RFPROC|RFMEM|RFNOWAIT);
	if (pid < 0)
		sysfatal("can't fork: %r");
	if (pid == 0) {
		procsetname("%s", text);
		(*f)(wid, arg);
		exits("child returned");
	}
}

static int
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

static void
runannounce(int, void *arg)
{
	char *ann, adir[40], ldir[40];
	int actl, lctl, fd;

	ann = arg;
	if((actl = announce(ann, adir)) < 0)
		sysfatal("announce %s: %r", ann);
	while(1){
		if((lctl = listen(adir, ldir)) < 0){
			fprint(2, "listen %s: %r", adir);
			break;
		}
		fd = accept(lctl, ldir);
		close(lctl);
		if(fd < 0){
			fprint(2, "accept %s: %r", ldir);
			continue;
		}
		launch(runfs, -1, (void *)fd, "netio");
	}
	close(actl);
}

static void
usage(void)
{
	fprint(2, "usage: %s [-rA] [-m mem] [-n srv] [-u usr] [-a net]... -f dev\n", argv0);
	exits("usage");
}

void
main(int argc, char **argv)
{
	int i, srvfd, ctlfd, nann;
	char *s, *ann[16];

	nann = 0;
	ARGBEGIN{
	case 'a':
		if(nann == nelem(ann))
			sysfatal("too many announces");
		ann[nann++] = EARGF(usage());
		break;
	case 'r':
		ream = 1;
		break;
	case 'm':
		cachesz = strtoll(EARGF(usage()), nil, 0)*MiB;
		break;
	case 'd':
		debug++;
		break;
	case 'n':
		srvname = EARGF(usage());
		break;
	case 'A':
		noauth = 1;
		break;
	case 'u':
		forceuser = EARGF(usage());
		break;
	case 'f':
		dev = EARGF(usage());
		break;
	default:
		usage();
		break;
	}ARGEND;
	if(dev == nil)
		usage();

	/*
	 * sanity checks -- I've tuned these to stupid
	 * values in the past.
	 */
	assert(4*Kpmax < Pivspc);
	assert(2*Msgmax < Bufspc);
	assert(Treesz < Inlmax);

	initfs(cachesz);
	initshow();
	fmtinstall('H', encodefmt);
	fmtinstall('B', Bconv);
	fmtinstall('M', Mconv);
	fmtinstall('P', Pconv);
	fmtinstall('K', Kconv);
	fmtinstall('R', Rconv);
	fmtinstall('F', fcallfmt);
	fmtinstall('Q', Qconv);

	if((s = getenv("NPROC")) != nil)
		nproc = atoi(s);

	/*
	 * too few procs, we can't parallelize io,
	 * too many, we suffer lock contention
	 */
	if(nproc < 2)
		nproc = 2;
	if(nproc > 6)
		nproc = 6;
	if(ream){
		reamfs(dev);
		exits(nil);
	}

	loadfs(dev);

	fs->rdchan = mkchan(32);
	fs->wrchan = mkchan(32);
	fs->nsyncers = 2;
	if(fs->nsyncers > fs->narena)
		fs->nsyncers = fs->narena;
	for(i = 0; i < fs->nsyncers; i++)
		fs->chsync[i] = mkchan(1024);
	for(i = 0; i < fs->narena; i++)
		fs->arenas[i].sync = fs->chsync[i%fs->nsyncers];
	srvfd = postfd(srvname, "");
	ctlfd = postfd(srvname, ".cmd");
	launch(runtasks, -1, nil, "tasks");
	launch(runcons, fs->nquiesce++, (void*)ctlfd, "ctl");
	launch(runwrite, fs->nquiesce++, nil, "mutate");
	for(i = 0; i < 2; i++)
		launch(runread, fs->nquiesce++, nil, "readio");
	for(i = 0; i < fs->nsyncers; i++)
		launch(runsync, -1, fs->chsync[i], "syncio");
	for(i = 0; i < nann; i++)
		launch(runannounce, -1, ann[i], "announce");
	if(srvfd != -1)
		launch(runfs, -1, (void *)srvfd, "srvio");
	exits(nil);
}
