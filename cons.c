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
snapfs(int fd, char **ap, int na)
{
	Tree *t, *n;
	char *e;

	if((t = openlabel(ap[0])) == nil){
		fprint(fd, "snap: open %s: does not exist\n", ap[0]);
		return;
	}
	if((n = newsnap(t)) == nil){
		fprint(fd, "snap: save %s: failed\n", ap[na-1]);
		return;
	}
	if((e = labelsnap(ap[na-1], n->gen)) != nil){
		fprint(fd, "snap: save %s: %s\n", ap[na-1], e);
		return;
	}
	if(na <= 1 || strcmp(ap[0], ap[1]) == 0){
		/* the label moved */
		if((e = unrefsnap(t->gen, n->gen)) != nil){
			fprint(fd, "snap: unref old: %s\n", e);
			return;
		}
	}
	closesnap(n);
	closesnap(t);
	sync();
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
refreshusers(int fd, char **ap, int na)
{
	char *l, *e;
	Tree *t;

	l = (na == 0) ? "main" : ap[0];
	if((t = openlabel(l)) == nil){
		fprint(fd, "load users: no label %s", l);
		return;
	}
	e = loadusers(fd, t);
	if(e != nil)
		fprint(fd, "load users: %s\n", e);
	else
		fprint(fd, "refreshed users\n");
	closesnap(t);
}

static void
showusers(int fd, char**, int)
{
	User *u, *v;
	int i, j;
	char *sep;

	rlock(&fs->userlk);
	for(i = 0; i < fs->nusers; i++){
		u = &fs->users[i];
		fprint(fd, "%d:%s:", u->id, u->name);
		if((v = uid2user(u->lead)) == nil)
			fprint(fd, "???:");
		else
			fprint(fd, "%s:", v->name);
		sep = "";
		for(j = 0; j < u->nmemb; j++){
			if((v = uid2user(u->memb[j])) == nil)
				fprint(fd, "%s???", sep);
			else
				fprint(fd, "%s%s", sep, v->name);
			sep = ",";
		}
		fprint(fd, "\n");
	}
	runlock(&fs->userlk);
}		

static void
help(int fd, char**, int)
{
	char *msg =
		"help\n"
		"	show this help"
		"sync\n"
		"	flush all p[ending writes to disk\n"
		"snap name [new]\n"
		"	create or update a new snapshot\n"
		"check\n"
		"	run a consistency check on the file system\n"
		"users\n"
		"	reload user table from /adm/users in the main snap\n"
		"show\n"
		"	show debug debug information, the following dumps\n"
		"	are supported:\n"
		"	cache\n"
		"		the contents of the in-memory cache\n"
		"	tree [name]\n"
		"		the contents of the tree associated with a\n"
		"		snapshot. The special name 'snap' shows the\n"
		"		snapshot tree\n"
		"	snap\n"
		"		the summary of the existing snapshots\n"
		"	fid\n"
		"		the summary of open fids\n"
		"	users\n"
		"		the known user file\n";
	fprint(fd, "%s", msg);
}

Cmd cmdtab[] = {
	{.name="sync",	.sub=nil,	.minarg=0, .maxarg=0, .fn=syncfs},
	{.name="snap",	.sub=nil,	.minarg=1, .maxarg=2, .fn=snapfs},
	{.name="check",	.sub=nil,	.minarg=1, .maxarg=1, .fn=fsckfs},
	{.name="help",	.sub=nil,	.minarg=0, .maxarg=0, .fn=help},
	{.name="users",	.sub=nil,	.minarg=0, .maxarg=1, .fn=refreshusers},

	/* debugging */
	{.name="show",	.sub="cache",	.minarg=0, .maxarg=0, .fn=showcache},
	{.name="show",	.sub="tree",	.minarg=0, .maxarg=1, .fn=showtree},
	{.name="show",	.sub="snap",	.minarg=0, .maxarg=1, .fn=showsnap},
	{.name="show",	.sub="fid",	.minarg=0, .maxarg=0, .fn=showfid},
	{.name="show",	.sub="free",	.minarg=0, .maxarg=0, .fn=showfree},
	{.name="show",	.sub="users",	.minarg=0, .maxarg=0, .fn=showusers},
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
