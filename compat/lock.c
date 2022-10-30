#include <u.h>
#include <libc.h>

void
lock(Lock *l)
{
	while(atomic_flag_test_and_set_explicit(&l->flag, memory_order_acquire));
}

void
unlock(Lock *l)
{
	atomic_flag_clear_explicit(&l->flag, memory_order_release);
}

void
qlock(QLock *q)
{
	pthread_mutex_lock(&q->mutex);
}

void
qunlock(QLock *q)
{
	pthread_mutex_unlock(&q->mutex);
}

void
rlock(RWLock *q)
{
	pthread_rwlock_rdlock(&q->lock);
}

void
runlock(RWLock *q)
{
	pthread_rwlock_unlock(&q->lock);
}

void
wlock(RWLock *q)
{
	pthread_rwlock_wrlock(&q->lock);
}

void
wunlock(RWLock *q)
{
	pthread_rwlock_unlock(&q->lock);
}

void
rsleep(Rendez *r)
{
	pthread_cond_wait(&r->cond, &r->l->mutex);
}

int
rwakeup(Rendez *r)
{
	pthread_cond_signal(&r->cond);
	return 0;
}

int
rwakeupall(Rendez *r)
{
	pthread_cond_broadcast(&r->cond);
	return 0;
}
