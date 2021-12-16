#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"

typedef struct Cmd	Cmd;

Cmd cmdtab[];

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
snapfs(int fd, char **ap, int na)
{
	char *e;
	Tree t;

	if((e = opensnap(&t, ap[0])) != nil){
		fprint(fd, "snap: open %s: %s\n", ap[0], e);
		return;
	}
	if((e = snapshot(&t, ap[na-1], 0)) != nil){
		fprint(fd, "snap: save %s: %s\n", ap[na-1], e);
		return;
	}
	fprint(fd, "snap %s: ok\n", ap[na-1]);
}

static void
fsckfs(int fd, char**, int)
{
	if(checkfs(fd))
		fprint(fd, "ok\n");
	else
		fprint(fd, "broken fs\n");
}

static void
help(int fd, char **ap, int na)
{
	Cmd *c;
	int i;

	for(c = cmdtab; c->name != nil; c++){
		if(na == 0 || strcmp(ap[0], c->name) == 0){
			if(c->sub == nil)
				fprint(fd, "%s", c->name);
			else
				fprint(fd, "%s %s", c->name, c->sub);
			for(i = 0; i < c->minarg; i++)
				fprint(fd, " a%d", i);
			for(i = c->minarg; i < c->maxarg; i++)
				fprint(fd, " [a%d]", i);
			fprint(fd, "\n");
		}
	}
}


Cmd cmdtab[] = {
	{.name="sync",	.sub=nil,	.minarg=0, .maxarg=0, .fn=syncfs},
	{.name="snap",	.sub=nil,	.minarg=1, .maxarg=2, .fn=snapfs},
	{.name="check",	.sub=nil,	.minarg=1, .maxarg=1, .fn=fsckfs},
	{.name="help",	.sub=nil,	.minarg=0, .maxarg=1, .fn=help},

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
runcons(int tid, void *pfd)
{
	char buf[256], *f[4], **ap;
	int i, n, nf, na, fd;
	Cmd *c;

	fd = (uintptr)pfd;
	while(1){
		if((n = read(fd, buf, sizeof(buf)-1)) == -1)
			break;
		quiesce(tid);
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
				ap++;
				na--;
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
		quiesce(tid);
	}
}
