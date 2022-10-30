static inline long
agetl(atomic_long *p)
{
	return atomic_load(p);
}

static inline vlong
agetv(atomic_llong* p)
{
	return atomic_load(p);
}

static inline void *
agetp(void *_Atomic*p)
{
	return atomic_load(p);
}

static inline long
asetl(atomic_long *p, long v)
{
	atomic_store(p, v);
	return v;
}

static inline vlong
asetv(atomic_llong *p, vlong v)
{
	atomic_store(p, v);
	return v;
}

static inline void*
asetp(void *_Atomic*p, void *v)
{
	atomic_store(p, v);
	return v;
}

static inline long
aincl(atomic_long *p, vlong v)
{
	return atomic_fetch_add(p, v);
}

static inline vlong
aincv(atomic_llong *p, vlong v)
{
	return atomic_fetch_add(p, v);
}

static inline void*
aincp(void *_Atomic*p, void *v)
{
	return atomic_fetch_add(p, v);
}

static inline void
coherence(void)
{
	atomic_thread_fence(memory_order_relaxed);
}
