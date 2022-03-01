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
sendsync(int fd, int halt)
{
	Fmsg *m;
	Amsg *a;

	m = mallocz(sizeof(Fmsg), 1);
	a = mallocz(sizeof(Amsg), 1);
	if(m == nil || a == nil){
		fprint(fd, "alloc sync msg: %r\n");
		free(m);
		free(a);
		return;
	}
	a->op = AOsync;
	a->halt = halt;
	a->fd = fd;
	m->a = a;
	chsend(fs->wrchan, m);		
}

static void
syncfs(int fd, char **, int)
{
	sendsync(fd, 0);
}

static void
haltfs(int fd, char **, int)
{
	sendsync(fd, 1);
}

static void
snapfs(int fd, char **ap, int)
{
	Fmsg *m;
	Amsg *a;

	m = mallocz(sizeof(Fmsg), 1);
	a = mallocz(sizeof(Amsg), 1);
	if(m == nil || a == nil){
		fprint(fd, "alloc sync msg: %r\n");
		goto Error;
	}
	if(strcmp(ap[0], ap[1]) == 0){
		fprint(fd, "not a new snap: %s\n", ap[1]);
		goto Error;
	}
	strecpy(a->old, a->old+sizeof(a->old), ap[0]);
	strecpy(a->new, a->new+sizeof(a->new), ap[1]);
	a->op = AOsnap;
	a->fd = fd;
	m->a = a;
	sendsync(fd, 0);
	chsend(fs->wrchan, m);
	return;	
Error:
	free(m);
	free(a);
	return;
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
		fprint(fd, "load users: no label %s\n", l);
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
stats(int fd, char**, int)
{
	Stats *s;

	s = &fs->stats;
	fprint(fd, "stats:\n");
	fprint(fd, "	cache hits:	%lld\n", s->cachehit);
	fprint(fd, "	cache lookups:	%lld\n", s->cachelook);
	fprint(fd, "	cache ratio:	%f\n", (double)s->cachehit/(double)s->cachelook);
}

static void
showdf(int fd, char**, int)
{
	vlong size, used;
	double pct;
	Arena *a;
	int i;

	size = 0;
	used = 0;
	for(i = 0; i < fs->narena; i++){
		a = &fs->arenas[i];
		lock(a);
		size += a->size;
		used += a->used;
		unlock(a);
	}
	pct = 100.0*(double)used/(double)size;
	fprint(fd, "used: %lld / %lld (%.2f%%)\n", used, size, pct);
}

static void
showent(int fd, char **ap, int)
{
	char *e, *p, kbuf[Keymax], kvbuf[Kvmax];
	Tree *t;
	Kvp kv;
	Key k;
	vlong pqid;

	if((t = openlabel("main")) == nil){
		fprint(fd, "could not open main snap\n");
		return;
	}
	pqid = strtoll(ap[0], nil, 16);
	if((p = packdkey(kbuf, sizeof(kbuf), pqid, ap[1])) == nil){
		fprint(fd, "could not pack key\n");
		return;
	}
	k.k = kbuf;
	k.nk = p - kbuf;
	if((e = btlookup(t, &k, &kv, kvbuf, sizeof(kvbuf))) != nil)
		fprint(fd, "not found: %s\n", e);
	else
		fprint(fd, "%P\n", &kv);
	closesnap(t);
}

static void
showblkdump(int fd, char **ap, int)
{
	Bptr bp;

	bp.addr = atoll(ap[0]);
	bp.hash = -1;
	bp.gen = -1;
	showbp(fd, bp, 0);
}

static void
help(int fd, char**, int)
{
	char *msg =
		"help\n"
		"	show this help"
		"sync\n"
		"	flush all p[ending writes to disk\n"
		"snap old new\n"
		"	create or update a new snapshot based off old\n"
		"check\n"
		"	run a consistency check on the file system\n"
		"users\n"
		"	reload user table from /adm/users in the main snap\n"
		"show\n"
		"	show debug debug information, the following dumps\n"
		"	are supported:\n"
		"	cache\n"
		"		the contents of the in-memory cache\n"
		"	ent pqid name\n"
		"		the contents of a directory entry\n"
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
	/* admin */
	{.name="sync",	.sub=nil,	.minarg=0, .maxarg=0, .fn=syncfs},
	{.name="halt",	.sub=nil,	.minarg=0, .maxarg=0, .fn=haltfs},
	{.name="snap",	.sub=nil,	.minarg=2, .maxarg=2, .fn=snapfs},
	{.name="check",	.sub=nil,	.minarg=1, .maxarg=1, .fn=fsckfs},
	{.name="help",	.sub=nil,	.minarg=0, .maxarg=0, .fn=help},
	{.name="df",	.sub=nil, 	.minarg=0, .maxarg=0, .fn=showdf},
	{.name="users",	.sub=nil,	.minarg=0, .maxarg=1, .fn=refreshusers},
	{.name="stats", .sub=nil,	.minarg=0, .maxarg=0, .fn=stats},

	/* debugging */
	{.name="show",	.sub="cache",	.minarg=0, .maxarg=0, .fn=showcache},
	{.name="show",	.sub="ent",	.minarg=2, .maxarg=2, .fn=showent},
	{.name="show",	.sub="fid",	.minarg=0, .maxarg=0, .fn=showfid},
	{.name="show",	.sub="free",	.minarg=0, .maxarg=0, .fn=showfree},
	{.name="show",	.sub="snap",	.minarg=0, .maxarg=1, .fn=showsnap},
	{.name="show",	.sub="tree",	.minarg=0, .maxarg=1, .fn=showtree},
	{.name="show",	.sub="users",	.minarg=0, .maxarg=0, .fn=showusers},
	{.name="show",	.sub="blk",	.minarg=1, .maxarg=1, .fn=showblkdump},
	{.name="debug",	.sub=nil,	.minarg=0, .maxarg=1, .fn=setdbg},

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
			fprint(fd, "unknown command '%s", f[0]);
			for(i = 1; i < nf; i++)
				fprint(fd, " %s", f[i]);
			fprint(fd, "'\n");
		}
		quiesce(tid);
	}
}
