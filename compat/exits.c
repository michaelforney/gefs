#include <u.h>
#include <libc.h>

void
exits(char *s)
{
	exit(s && *s);
}
