#pragma varargck type "M"	Msg*
#pragma varargck type "P"	Kvp*
#pragma varargck type "K"	Key*
#pragma varargck type "V"	Val*
#pragma varargck type "B"	Bptr
#pragma varargck type "R"	Arange*
#pragma varargck type "X"	char*
#pragma varargck type "Q"	Qid

extern Gefs*	fs;
extern int	debug;
extern char*	forceuser;

#define	UNPACK8(p)	(((uchar*)(p))[0])
#define	UNPACK16(p)	((((uchar*)(p))[0]<<8)|(((uchar*)(p))[1]))
#define	UNPACK32(p)	((((uchar*)(p))[0]<<24)|(((uchar*)(p))[1]<<16)|\
				(((uchar*)(p))[2]<<8)|(((uchar*)(p))[3]))
#define	UNPACK64(p)	(((u64int)((((uchar*)(p))[0]<<24)|(((uchar*)(p))[1]<<16)|\
				(((uchar*)(p))[2]<<8)|(((uchar*)(p))[3])))<<32 |\
			((u64int)((((uchar*)(p))[4]<<24)|(((uchar*)(p))[5]<<16)|\
				(((uchar*)(p))[6]<<8)|(((uchar*)(p))[7]))))

#define	PACK8(p,v)	do{(p)[0]=(v);}while(0)
#define	PACK16(p,v)	do{(p)[0]=(v)>>8;(p)[1]=(v);}while(0)
#define	PACK32(p,v)	do{(p)[0]=(v)>>24;(p)[1]=(v)>>16;(p)[2]=(v)>>8;(p)[3]=(v);}while(0)
#define	PACK64(p,v)	do{(p)[0]=(v)>>56;(p)[1]=(v)>>48;(p)[2]=(v)>>40;(p)[3]=(v)>>32;\
			   (p)[4]=(v)>>24;(p)[5]=(v)>>16;(p)[6]=(v)>>8;(p)[7]=(v);}while(0)

Blk*	newblk(int type);
Blk*	dupblk(Blk*);
Blk*	getroot(Tree*, int*);
Blk*	getblk(Bptr, int);
Blk*	holdblk(Blk*);
void	dropblk(Blk*);

void	lrutop(Blk*);
void	lrubot(Blk*);
void	cacheins(Blk*);
void	cachedel(vlong);
Blk*	cacheget(vlong);
Blk*	cachepluck(void);

void	qinit(Syncq*);
void	qput(Syncq*, Blk*);

Arena*	getarena(vlong);
int	syncblk(Blk*);
void	enqueue(Blk*);
void	epochstart(int);
void	epochend(int);
void	epochclean(void);
void	freesync(void);
void	freeblk(Tree*, Blk*);
void	freebp(Tree*, Bptr);
int	killblk(Tree*, Bptr);
void	reclaimblk(Bptr);
ushort	blkfill(Blk*);
uvlong	blkhash(Blk*);
u32int	ihash(uvlong);
void	finalize(Blk*);
Tree*	newsnap(Tree*);
char*	freesnap(Tree*, Tree*);
char*	labelsnap(char*, vlong);
char*	unlabelsnap(vlong, char*);
char*	refsnap(vlong);
char*	unrefsnap(vlong, vlong);
Tree*	openlabel(char*);
Tree*	opensnap(vlong);
void	closesnap(Tree*);
uvlong	siphash(void*, usize);
void	reamfs(char*);
int	loadarena(Arena*, Fshdr *fi, vlong);
void	loadfs(char*);
void	sync(void);
int	loadlog(Arena*);
int	scandead(Dlist*, int, void(*)(Bptr, void*), void*);
int	endfs(void);
int	compresslog(Arena*);
void	setval(Blk*, Kvp*);

char*	loadusers(int, Tree*);
User*	uid2user(int);
User*	name2user(char*);

char*	btupsert(Tree*, Msg*, int);
char*	btlookup(Tree*, Key*, Kvp*, char*, int);
char*	btscan(Tree*, Scan*, char*, int);
char*	btnext(Scan*, Kvp*, int*);
void	btdone(Scan*);

int	checkflag(Blk *b, int);
void	setflag(Blk *b, int);
void	clrflag(Blk *b, int);

char*	estrdup(char*);

int	keycmp(Key *, Key *);
void	cpkey(Key*, Key*, char*, int);
void	cpkvp(Kvp*, Kvp*, char*, int);

/* for dumping */
void	getval(Blk*, int, Kvp*);
void	getmsg(Blk*, int, Msg*);
Bptr	getptr(Kvp*, int*);

void	initshow(void);
void	showblk(int, Blk*, char*, int);
void	showbp(int, Bptr, int);
void	showtreeroot(int, Tree*);
void	showtree(int, char**, int);
void	showsnap(int, char**, int);
void	showfid(int, char**, int);
void	showcache(int, char**, int);
void	showfree(int, char**, int);
int	checkfs(int);

#define dprint(...) \
	do{ \
		if(debug) fprint(2, __VA_ARGS__); \
	}while(0)

char*	pack8(int*, char*, char*, uchar);
char*	pack16(int*, char*, char*, ushort);
char*	pack32(int*, char*, char*, uint);
char*	pack64(int*, char*, char*, uvlong);
char*	packstr(int*, char*, char*, char*);

/* void* is a bit hacky, but we want both signed and unsigned to work */
char*	unpack8(int*, char*, char*, void*);
char*	unpack16(int*, char*, char*, void*);
char*	unpack32(int*, char*, char*, void*);
char*	unpack64(int*, char*, char*, void*);
char*	unpackstr(int*, char*, char*, char**);
int	dir2kv(vlong, Xdir*, Kvp*, char*, int);
int	kv2statbuf(Kvp*, char*, int);
int	dir2statbuf(Xdir*, char*, int);
int	kv2dir(Kvp*, Xdir*);
int	kv2qid(Kvp*, Qid*);

char*	packbp(char*, int, Bptr*);
Bptr	unpackbp(char*, int);
char*	packtree(char*, int, Tree*);
Tree*	unpacktree(Tree*, char*, int);
char*	packdkey(char*, int, vlong, char*);
char*	unpackdkey(char*, int, vlong*);
char*	packdval(char*, int, Xdir*);
char*	packsnap(char*, int, vlong);
char*	packlabel(char*, int, char*);
char*	packsuper(char*, int, vlong);
char*	packarena(char*, int, Arena*, Fshdr*);
char*	unpackarena(Arena*, Fshdr*, char*, int);

/* fmt */
int	Bconv(Fmt*);
int	Mconv(Fmt*);
int	Pconv(Fmt*);
int	Rconv(Fmt*);
int	Kconv(Fmt*);
int	Qconv(Fmt*);

Chan*	mkchan(int);
void*	chrecv(Chan*);
void	chsend(Chan*, void*);
void	runfs(int, void*);
void	runwrite(int, void*);
void	runread(int, void*);
void	runcons(int, void*);
void	runtasks(int, void*);
void	runsync(int, void*);
