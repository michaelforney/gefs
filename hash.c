/*
 Copyright (c) 2013  Marek Majkowski <marek@popcount.org>

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 THE SOFTWARE.

 Original location:
    https://github.com/majek/csiphash/

 Solution inspired by code from:
    Samuel Neves (supercop/crypto_auth/siphash24/little)
    djb (supercop/crypto_auth/siphash24/little2)
    Jean-Philippe Aumasson (https://131002.net/siphash/siphash24.c)
*/
#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"

#define _le64toh(x) \
	GBIT64((char*)&x)


#define ROTATE(x, b) (u64int)( ((x) << (b)) | ( (x) >> (64 - (b))) )

#define HALF_ROUND(a,b,c,d,s,t)			\
	a += b; c += d;				\
	b = ROTATE(b, s) ^ a;			\
	d = ROTATE(d, t) ^ c;			\
	a = ROTATE(a, 32);

#define DOUBLE_ROUND(v0,v1,v2,v3)		\
	HALF_ROUND(v0,v1,v2,v3,13,16);		\
	HALF_ROUND(v2,v1,v0,v3,17,21);		\
	HALF_ROUND(v0,v1,v2,v3,13,16);		\
	HALF_ROUND(v2,v1,v0,v3,17,21);


static u64int
siphash24(const void *src, unsigned long src_sz, const char key[16]) {
	const u64int *_key = (u64int *)key;
	u64int k0 = _le64toh(_key[0]);
	u64int k1 = _le64toh(_key[1]);
	u64int b = (u64int)src_sz << 56;
	const u64int *in = (u64int*)src;

	u64int v0 = k0 ^ 0x736f6d6570736575ULL;
	u64int v1 = k1 ^ 0x646f72616e646f6dULL;
	u64int v2 = k0 ^ 0x6c7967656e657261ULL;
	u64int v3 = k1 ^ 0x7465646279746573ULL;

	while (src_sz >= 8) {
		u64int mi = _le64toh(*in);
		in += 1; src_sz -= 8;
		v3 ^= mi;
		DOUBLE_ROUND(v0,v1,v2,v3);
		v0 ^= mi;
	}

	u64int t = 0; u8int *pt = (u8int *)&t; u8int *m = (u8int *)in;
	switch (src_sz) {
	case 7: pt[6] = m[6];
	case 6: pt[5] = m[5];
	case 5: pt[4] = m[4];
	case 4: *((u32int*)&pt[0]) = *((u32int*)&m[0]); break;
	case 3: pt[2] = m[2];
	case 2: pt[1] = m[1];
	case 1: pt[0] = m[0];
	}
	b |= _le64toh(t);

	v3 ^= b;
	DOUBLE_ROUND(v0,v1,v2,v3);
	v0 ^= b; v2 ^= 0xff;
	DOUBLE_ROUND(v0,v1,v2,v3);
	DOUBLE_ROUND(v0,v1,v2,v3);
	return (v0 ^ v1) ^ (v2 ^ v3);
}

u64int
siphash(void *src, usize len)
{
	static char key[16] = "gefsgefsgefsgefs";
	return siphash24(src, len, key);
}

uvlong
blkhash(Blk *b)
{
	return siphash(b->buf, Blksz);
}

u32int
ihash(uvlong x)
{
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    x = x ^ (x >> 31);
    return x;
}
