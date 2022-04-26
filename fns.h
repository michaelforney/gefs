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

Blk*	newblk(int type);
Blk*	dupblk(Blk*);
Blk*	getroot(Tree*, int*);
Blk*	getblk(Bptr, int);
Blk*	refblk(Blk*);
Blk*	cacheblk(Blk*);
void	cachedel_lk(vlong);
Blk*	lookupblk(vlong);
Arena*	getarena(vlong);
void	putblk(Blk*);
int	syncblk(Blk*);
void	enqueue(Blk*);
void	quiesce(int);
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
void*	chrecv(Chan*, int);
void	chsend(Chan*, void*);
void	runfs(int, void*);
void	runwrite(int, void*);
void	runread(int, void*);
void	runcons(int, void*);
void	runtasks(int, void*);
void	runsync(int, void*);

/* it's in libc... */
extern int cas(long*, long, long);
extern int fasp(void***, void*);
extern int cas64(u64int*, u64int, u64int);
vlong	inc64(vlong*, vlong);
