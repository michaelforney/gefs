#include <u.h>
#include <libc.h>

static _Thread_local char syserr[ERRMAX];

int
errstr(char *err, uint nerr)
{
	char tmp[ERRMAX];

	strecpy(tmp, tmp+ERRMAX, err);
	strecpy(err, err+nerr, syserr);
	strecpy(syserr, syserr+ERRMAX, tmp);
	return 0;
}

void
rerrstr(char *err, uint nerr)
{
	strecpy(err, err+nerr, syserr);
}

void
werrstr(char *fmt, ...)
{
	va_list arg;
	char err[ERRMAX];

	va_start(arg, fmt);
	vseprint(err, err+ERRMAX, fmt, arg);
	va_end(arg);
	strecpy(syserr, syserr+ERRMAX, err);
}
