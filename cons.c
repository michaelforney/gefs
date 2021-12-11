#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"

typedef struct Cmd	Cmd;

struct Cmd {
	char	*name;
	char	*sub;
	int	minarg;
	int	maxarg;
	void	(*fn)(int, char**, int);
};

static void
setdbg(int fd, char **ap, int na)
{
	debug = (na == 1) ? atoi(ap[0]) : !debug;
	fprint(fd, "debug → %d\n", debug);
}

static void
syncfs(int fd, char **, int)
{
	fprint(fd, "sync\n");
}

static void
snapfs(int fd, char **, int)
{
	fprint(fd, "snap\n");
}

static void
fsckfs(int fd, char**, int)
{
	if(checkfs(fd))
		fprint(fd, "ok\n");
	else
		fprint(fd, "broken fs\n");
}

Cmd cmdtab[] = {
	{.name="sync",	.sub=nil,	.minarg=0, .maxarg=0, .fn=syncfs},
	{.name="snap",	.sub=nil,	.minarg=1, .maxarg=1, .fn=snapfs},
	{.name="check",	.sub=nil,	.minarg=1, .maxarg=1, .fn=fsckfs},

	/* debugging */
	{.name="show",	.sub="cache",	.minarg=0, .maxarg=0, .fn=showcache},
	{.name="show",	.sub="fs",	.minarg=0, .maxarg=1, .fn=showfs},
	{.name="show",	.sub="snap",	.minarg=0, .maxarg=1, .fn=showsnap},
	{.name="show",	.sub="fid",	.minarg=0, .maxarg=0, .fn=showfid},
	{.name="show",	.sub="free",	.minarg=0, .maxarg=0, .fn=showfree},
	{.name="debug",	.sub=nil,	.minarg=1, .maxarg=1, .fn=setdbg},
	{.name=nil, .sub=nil},
};

void
runcons(int, void *pfd)
{
	char buf[256], *f[4], **ap;
	int i, n, nf, na, fd;
	Cmd *c;

	fd = (uintptr)pfd;
	while(1){
		if((n = read(fd, buf, sizeof(buf)-1)) == -1)
			break;
		buf[n] = 0;
		nf = tokenize(buf, f, nelem(f));
		if(nf == 0 || strlen(f[0]) == 0)
			continue;
		for(c = cmdtab; c->name != nil; c++){
			ap = f;
			na = nf;
			if(strcmp(c->name, *ap) != 0)
				continue;
			ap++; na--;
			if(c->sub != nil){
				if(strcmp(c->sub, *ap) != 0)
					continue;
				ap++; na--;
			}
			if(na < c->minarg || na > c->maxarg)
				continue;
			c->fn(fd, ap, na);
			break;
		}
		if(c->name == nil){
			fprint(fd, "unknown command '");
			for(i = 0; i < nf; i++)
				fprint(fd, " %s", f[i]);
			fprint(fd, "'\n");
		}
	}
}
