static inline void *
putbe8(void *p, uint_least8_t v)
{
	unsigned char *b = p;

	b[0] = v & 0xff;
	return b + 1;
}

static inline void *
putbe16(void *p, uint_least16_t v)
{
	unsigned char *b = p;

	b[0] = v >> 8 & 0xff;
	b[1] = v & 0xff;
	return b + 2;
}

static inline void *
putbe32(void *p, uint_least32_t v)
{
	unsigned char *b = p;

	b[0] = v >> 24 & 0xff;
	b[1] = v >> 16 & 0xff;
	b[2] = v >> 8 & 0xff;
	b[3] = v & 0xff;
	return b + 4;
}

static inline void *
putbe64(void *p, uint_least64_t v)
{
	unsigned char *b = p;

	b[0] = v >> 56 & 0xff;
	b[1] = v >> 48 & 0xff;
	b[2] = v >> 40 & 0xff;
	b[3] = v >> 32 & 0xff;
	b[4] = v >> 24 & 0xff;
	b[5] = v >> 16 & 0xff;
	b[6] = v >> 8 & 0xff;
	b[7] = v & 0xff;
	return b + 8;
}

static inline uint_least8_t
getbe8(void *p)
{
	unsigned char *b = p;

	return b[0] & 0xffu;
}

static inline uint_least16_t
getbe16(void *p)
{
	unsigned char *b = p;
	uint_least16_t v;

	v = (b[0] & 0xffu) << 8;
	v |= b[1] & 0xffu;
	return v;
}

static inline uint_least32_t
getbe32(void *p)
{
	unsigned char *b = p;
	uint_least32_t v;

	v = (b[0] & 0xfful) << 24;
	v |= (b[1] & 0xfful) << 16;
	v |= (b[2] & 0xfful) << 8;
	v |= b[3] & 0xfful;
	return v;
}

static inline uint_least64_t
getbe64(void *p)
{
	unsigned char *b = p;
	uint_least64_t v;

	v = (b[0] & 0xfful) << 56;
	v |= (b[1] & 0xfful) << 48;
	v |= (b[2] & 0xfful) << 40;
	v |= (b[3] & 0xfful) << 32;
	v |= (b[4] & 0xfful) << 24;
	v |= (b[5] & 0xfful) << 16;
	v |= (b[6] & 0xfful) << 8;
	v |= b[7] & 0xfful;
	return v;
}
