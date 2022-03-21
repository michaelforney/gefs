#include <u.h>
#include <libc.h>

long
ainc(atomic_long *p)
{
	return atomic_fetch_add_explicit(p, 1, memory_order_relaxed) + 1;
}

long
adec(atomic_long *p)
{
	return atomic_fetch_add_explicit(p, -1, memory_order_acq_rel) - 1;
}
