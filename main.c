#define _GNU_SOURCE
#include <u.h>
#include <libc.h>
#include <avl.h>
#include <fcall.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "dat.h"
#include "fns.h"

Gefs *fs;

int	ream;
int	debug;
int	noauth;
int	noperm;
int	nproc;
char	*forceuser;
char	*srvname = "gefs";
char	*dev;
vlong	cachesz = 512*MiB;

static void
initfs(vlong cachesz)
{
	Blk *b, *buf;

	if((fs = mallocz(sizeof(Gefs), 1)) == nil)
		sysfatal("malloc: %r");

	fs->lrurz.l = &fs->lrulk;
	fs->syncrz.l = &fs->synclk;
	fs->noauth = noauth;
	fs->noperm = noperm;
	fs->cmax = cachesz/Blksz;
	if(fs->cmax > (1<<30))
		sysfatal("cache too big");
	if((fs->cache = mallocz(fs->cmax*sizeof(Bucket), 1)) == nil)
		sysfatal("malloc: %r");

	/* leave room for corruption check magic */
	buf = malloc(fs->cmax * sizeof(Blk));
	if(buf == nil)
		sysfatal("sbrk: %r");
	for(b = buf; b != buf+fs->cmax; b++){
		b->bp.addr = -1;
		b->bp.hash = -1;
		b->magic = Magic;
		lrutop(b);
	}
	fs->blks = buf;
}

typedef struct Startarg {
	void (*fn)(int, void *);
	int wid;
	void *arg;
} Startarg;

static void *
start(void *p)
{
	Startarg *arg;

	arg = p;
	arg->fn(arg->wid, arg->arg);
	free(arg);
	return NULL;
}

static void
launch(void (*f)(int, void *), int wid, void *arg, char *text)
{
	pthread_t thread;
	pthread_attr_t attr;
	Startarg *a;

	assert(wid == -1 || wid < nelem(fs->lepoch));
	a = malloc(sizeof *a);
	a->fn = f;
	a->wid = wid;
	a->arg = arg;
	if(pthread_attr_init(&attr) != 0)
		sysfatal("attr init failed");
	if(pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) != 0)
		sysfatal("set detach state failed");
	if(pthread_create(&thread, &attr, start, a) != 0)
		sysfatal("thread create failed");
	pthread_setname_np(thread, text);
}

static void
runannounce(int, void *arg)
{
	char *ann, *p;
	int sock, fd;
	Conn *c;
	union {
		struct sockaddr sa;
		struct sockaddr_in in;
		struct sockaddr_un un;
	} addr;
	socklen_t addrlen;

	ann = arg;
	if(strncmp(ann, "unix!", 5) == 0){
		ann += 5;
		addr.un.sun_family = AF_UNIX;
		strecpy(addr.un.sun_path, addr.un.sun_path+sizeof(addr.un.sun_path), ann);
		addrlen = sizeof(addr.un);
		unlink(addr.un.sun_path);
	}else if(strncmp(ann, "tcp!", 4) == 0){
		ann += 4;
		p = strchr(ann, '!');
		if(p)
			*p++ = '\0';
		addr.in.sin_family = AF_INET;
		addr.in.sin_addr.s_addr = strcmp(ann, "*") == 0 ? INADDR_ANY : inet_addr(ann);
		addr.in.sin_port = htons(p ? atoi(p) : 564);
		addrlen = sizeof(addr.in);
	}else{
		fprint(2, "unknown announce string");
		return;
	}
	sock = socket(addr.sa.sa_family, SOCK_STREAM, 0);
	if(sock < 0){
		fprint(2, "socket: %s", strerror(errno));
		return;
	}
	if(addr.sa.sa_family == AF_INET){
		if(setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int)) != 0){
			fprint(2, "setsockopt: %s", strerror(errno));
			return;
		}
	}
	if(bind(sock, &addr.sa, addrlen) != 0){
		fprint(2, "bind: %s", strerror(errno));
		return;
	}
	if(listen(sock, 1) != 0){
		fprint(2, "listen: %s", strerror(errno));
		return;
	}
	while(1){
		fd = accept(sock, NULL, NULL);
		if(fd < 0){
			fprint(2, "accept: %s", strerror(errno));
			continue;
		}
		if(!(c = newconn(fd, fd))){
			close(fd);
			fprint(2, "%r");
			continue;
		}

		launch(runfs, -1, c, "netio");
	}
	close(sock);
}

static void
usage(void)
{
	fprint(2, "usage: %s [-rA] [-m mem] [-n srv] [-u usr] [-a net]... -f dev\n", argv0);
	exits("usage");
}

int
main(int argc, char **argv)
{
	int i, nann;
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
	case 'P':
		noperm = 1;
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
	if(nann == 0)
		usage();

	loadfs(dev);

	fs->rdchan = mkchan(32);
	fs->wrchan = mkchan(32);
	fs->nsyncers = 2;
	if(fs->nsyncers > fs->narena)
		fs->nsyncers = fs->narena;
	for(i = 0; i < fs->nsyncers; i++)
		qinit(&fs->syncq[i]);
	for(i = 0; i < fs->narena; i++)
		fs->arenas[i].sync = &fs->syncq[i%fs->nsyncers];
	launch(runtasks, -1, nil, "tasks");
	launch(runwrite, fs->nworker++, nil, "mutate");
	for(i = 0; i < 2; i++)
		launch(runread, fs->nworker++, nil, "readio");
	for(i = 0; i < fs->nsyncers; i++)
		launch(runsync, -1, &fs->syncq[i], "syncio");
	for(i = 0; i < nann; i++)
		launch(runannounce, -1, ann[i], "announce");
	runcons(fs->nworker++, nil);
}
