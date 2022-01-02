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

Blk*	newblk(int type);
Blk*	getroot(Tree*, int*);
Blk*	getblk(Bptr, int);
Blk*	refblk(Blk*);
Blk*	cacheblk(Blk*);
void	cachedel(vlong);
Blk*	lookupblk(vlong);
Blk*	readblk(vlong, int);
Arena*	getarena(vlong);
void	putblk(Blk*);
int	syncblk(Blk*);
void	enqueue(Blk*);
void	quiesce(int);
void	freeblk(Tree*, Blk*);
void	freebp(Tree*, Bptr);
int	killblk(Tree*, Bptr);
int	graft(Oplog*, Oplog*);
void	reclaimblk(Bptr);
ushort	blkfill(Blk*);
uvlong	blkhash(Blk*);
u32int	ihash(vlong);
void	finalize(Blk*);
char*	fillsuper(Blk*);
Tree*	newsnap(Tree*);
char*	freesnap(Tree*, Tree*);
int	savesnap(Tree*);
char*	labelsnap(char*, vlong);
char*	unlabelsnap(vlong, char*);
char*	refsnap(vlong);
char*	unrefsnap(vlong, vlong);
Tree*	openlabel(char*);
void	closesnap(Tree*);
uvlong	siphash(void*, usize);
void	reamfs(char*);
int	loadarena(Arena*, vlong);
void	loadfs(char*);
int	sync(void);
int	loadlog(Arena*);
int	scandead(Oplog*, void(*)(Bptr, void*), void*);
int	endfs(void);
int	compresslog(Arena*);
void	setval(Blk*, int, Kvp*);

char*	loadusers(int, Tree*);
User*	uid2user(int);
User*	name2user(char*);

char*	btupsert(Tree*, Msg*, int);
char*	btlookup(Tree*, Key*, Kvp*, char*, int);
char*	btscan(Tree*, Scan*, char*, int);
char*	btnext(Scan*, Kvp*, int*);
void	btdone(Scan*);

void	setflag(Blk *b, int);
void	clrflag(Blk *b, int);

char*	estrdup(char*);

int	keycmp(Key *, Key *);

/* for dumping */
void	getval(Blk*, int, Kvp*);
void	getmsg(Blk*, int, Msg*);
Bptr	getptr(Kvp*, int*);

void	initshow(void);
void	showblk(int, Blk*, char*, int);
void	showbp(int, Bptr, int);
void	showpath(int, Path*, int);
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
char*	packdval(char*, int, Xdir*);

/* fmt */
int	Bconv(Fmt*);
int	Mconv(Fmt*);
int	Pconv(Fmt*);
int	Rconv(Fmt*);
int	Kconv(Fmt*);
int	Qconv(Fmt*);

/* scratch */
void	setmsg(Blk *, int, Msg *);
void	bufinsert(Blk *, Msg *);
void	victim(Blk *b, Path *p);

Chan*	mkchan(int);
Fmsg*	chrecv(Chan*);
void	chsend(Chan*, Fmsg*);
void	runfs(int, void*);
void	runwrite(int, void*);
void	runread(int, void*);
void	runcons(int, void*);

/* it's in libc... */
extern int cas(long*, long, long);
extern int fas32(int*, int);
extern int cas64(u64int*, u64int, u64int);
uvlong	inc64(uvlong*, uvlong);
