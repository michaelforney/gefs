#include <u.h>
#include <libc.h>

void*
mallocz(ulong size, int clr)
{
	void *v;

	v = malloc(size);
	if(clr && v != nil)
		memset(v, 0, size);
	return v;
}

void
setmalloctag(void *v, uintptr pc)
{
}
