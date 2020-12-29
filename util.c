#include <u.h>
#include <libc.h>
#include "dat.h"
#include "fns.h"

void *
emalloc(usize n)
{
	void *v;
	
	v = mallocz(n, 1);
	if(v == nil)
		sysfatal("malloc: %r");
	setmalloctag(v, getcallerpc(&n));
	return v;
}

void *
erealloc(void *p, usize n)
{
	void *v;
	
	v = realloc(p, n);
	if(v == nil)
		sysfatal("realloc: %r");
	setmalloctag(v, getcallerpc(&n));
	return v;
}

char*
estrdup(char *s)
{
	s = strdup(s);
	if(s == nil)
		sysfatal("strdup: %r");
	setmalloctag(s, getcallerpc(&s));
	return s;
}
