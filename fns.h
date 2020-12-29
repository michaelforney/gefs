#pragma varargck type "M"	Msg*
#pragma varargck type "P"	Kvp*
#pragma varargck type "K"	Key*
#pragma varargck type "V"	Val*
#pragma varargck type "B"	Blk*

extern Gefs	*fs;
extern int	debug;

void	initfs(void);

Blk*	newblk(int type);
Blk*	shadow(Blk*, Path*, Path*);
int	putblk(Blk*);
Blk*	getblk(uvlong bp, uvlong bh);
void	freeblk(Blk *b);
uvlong	blkhash(Blk*);
uvlong	siphash(void*, usize);

int	fsupsert(Msg*);
char	*fswalk1(Key*, Kvp*);

void*	emalloc(usize);
void*	erealloc(void*, usize);
char*	estrdup(char*);

int	keycmp(Key *, Key *);

/* for dumping */
void	getval(Blk *, int, Kvp *);
void	getmsg(Blk *, int, Msg *);

void	initshow(void);
void	showblk(Blk*, char*);
void	showpath(Path*, int);
void	showfs(char*);
int	checkfs(void);

/* scratch */
void	setmsg(Blk *, int, Msg *);
void	bufinsert(Blk *, Msg *);
void victim(Blk *b, Path *p);
void blkinsert(Blk *b, Kvp *kv);

