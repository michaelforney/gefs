typedef struct Blk	Blk;
typedef struct Gefs	Gefs;
typedef struct Fmsg	Fmsg;
typedef struct Fid	Fid;
typedef struct Msg	Msg;
typedef struct Key	Key;
typedef struct Val	Val;
typedef struct Kvp	Kvp;
typedef struct Bptr	Bptr;
typedef struct Path	Path;
typedef struct Scan	Scan;
typedef struct Dent	Dent;
typedef struct Scanp	Scanp;
typedef struct Arena	Arena;
typedef struct Arange	Arange;
typedef struct Bucket	Bucket;
typedef struct Chan	Chan;
typedef struct Tree	Tree;

enum {
	KiB	= 1024ULL,
	MiB	= 1024ULL*KiB,
	GiB	= 1024ULL*MiB,
	TiB	= 1024ULL*GiB,

	Lgblk	= 13,
	Blksz	= (1ULL<<Lgblk),

	Nrefbuf	= 1024,			/* number of ref incs before syncing */
	Nfidtab	= 1024,			/* number of fit hash entries */
	Ndtab	= 1024,			/* number of dir tab entries */
	Max9p	= 16*KiB,		/* biggest message size we're willing to negotiate */
	Nsec	= 1000*1000*1000,	/* nanoseconds to the second */
	Maxname	= 256,			/* maximum size of a name element */
	Maxent	= 9+Maxname+1,		/* maximum size of ent key, with terminator */

	/*
	 * Kpmax must be no more than 1/4 of pivspc, or
	 * there is no way to get a valid split of a
	 * maximally filled tree.
	 */
	Loghdsz	= 8,			/* log hash */
	Keymax	= 32,			/* key data limit */
	Inlmax	= 128,			/* inline data limit */
	Ptrsz	= 18,			/* off, hash, fill */
	Kvmax	= Keymax + Inlmax,	/* Key and value */
	Kpmax	= Keymax + Ptrsz,	/* Key and pointer */
	

	Hdrsz	= 10,
	Blkspc	= Blksz - Hdrsz,
	Bufspc  = Blkspc / 2,
	Pivspc	= Blkspc - Bufspc,
	Logspc	= Blkspc - Loghdsz,
	Leafspc = Blkspc,
	Msgmax  = 1 + (Kvmax > Kpmax ? Kvmax : Kpmax)
};

enum {
	/*
	 * dent: pqid[8] qid[8] -- a directory entry key.
	 * ptr:  off[8] hash[8] -- a key for an Dir block.
	 * dir:  fixed statbuf header, user ids
	 */
	Kref,	/* off[8] => ptr[16]:		pointer to refcount page */
	Kdat,	/* qid[8] off[8] => ptr[16]:	pointer to data page */
	Kent,	/* pqid[8] name[n] => dir[n]:	serialized Dir */
	Ksnap,	/* name[n] => dent[16] ptr[16]:	snapshot root */
	Ksuper,	/* qid[8] => pqid[8]:		parent dir */
};

enum {
	Bdirty	= 1 << 0,
	Bqueued	= 1 << 1,
	Bfinal	= 1 << 2,
	Bcache	= 1 << 3,
};

#define Efs	"i will not buy this fs, it is scratched"
#define Efid	"bad fid"
#define Edscan	"invalid dir scan offset"
#define Eexist	"does not exist"
#define Ebotch	"protocol botch"
#define Emode	"unknown mode"
#define Efull	"file system full"
#define Eauth	"authentication failed"
#define Elength	"name too long"
#define Eperm	"permission denied"
#define Einuse	"resource in use"
#define Ebadf	"invalid file"
#define Emem	"out of memory"
#define Ename	"invalid file name"
#define Enomem	"out of memory"

/*
 * All metadata blocks share a common header:
 * 
 *	type[2]
 *
 * The None type is reserved for file data blocks
 * and refcount blocks.
 *
 * The superblock has this layout:
 *	version[8]	always "gefs0001"
 *	flags[4]	status flags:
 *				dirty=1<<0,
 *	blksz[4]	block size in bytes
 *	bufsz[4]	portion of leaf nodes
 *			allocated to buffers,
 *			in bytes
 *	hdrsz[4]	size of tree node header,
 *			in bytes.
 *	height[4]	tree height of root node
 *	rootb[8]	address of root in last
 *			snapshot.
 *	rooth[8]	hash of root node
 *	narena[4]	number of arenas in tree
 *	arenasz[8]	maximum size of arenas;
 *			they may be smaller.
 *	gen[8]		The flush generation
 *
 * The arena zone blocks have this layout, and
 * are overwritten in place:
 *
 *	log[8]		The head of the alloc log
 *	logh[8]		The hash of the alloc log
 *
 * The log blocks have this layout, and are one of
 * two types of blocks that get overwritten in place:
 *
 *	hash[8]		The hash of the previous log block
 *
 *	The remainder of the block is filled with log
 *	entries. Each log entry has at least 8 bytes
 *	of entry. Some are longer. The opcode is or'ed
 *	into the low order bits of the first vlong.
 *	These ops take the following form:
 *
 *	Alloc, Free:
 *		off[8] len[8]
 *	Alloc1, Free1:
 *		off[8]
 *	Ref:
 *		off[8]
 *	Flush:	
 *		gen[8]
 *
 * Pivots have the following layout:
 *
 *	nval[2]
 *	valsz[2]
 *	nbuf[2]
 *	bufsz[2]
 *
 * Leaves have the following layout:
 *
 *	nval[2]
 *	valsz[2]
 *	_pad[4]sure, 
 *
 * Within these nodes, pointers have the following
 * layout:
 *
 *	off[8] hash[8] fill[2]
 */
enum {
	Tnone,
	Traw,
	Tsuper,
	Tarena,
	Tlog,
	Tpivot,
	Tleaf,
};

enum {
	Vinl,	/* Inline value */
	Vref,	/* Block pointer */
};

enum {
	GBraw	= 1<<0,
	GBwrite	= 1<<1,
};

enum {
	Oinsert,
	Odelete,
	Owstat,
	/* wstat flags */
	Owsize	= 1<<4,
	Owname	= 1<<5,
	Owmode	= 1<<6,
	Owmtime	= 1<<7,
};

/*
 * Operations for the allocation log.
 */
enum {
	/* 1-wide entries */
	LogAlloc1,	/* alloc a block */
	LogFree1,	/* alloc a block */
	LogDead1,	/* free a block */
	LogFlush,	/* flush log, bump gen */
	LogChain,	/* point to next log block */
	LogEnd,		/* last entry in log */	

	/* 2-wide entries */
	Log2w	= 1<<5,
	LogAlloc = LogAlloc1|Log2w,	/* alloc a range */
	LogFree	= LogFree1|Log2w,	/* free a range */
};

struct Arange {
	Avl;
	vlong	off;
	vlong	len;
};

struct Bucket {
	Lock;
	Blk	*b;
};

struct Fmsg {
	Fcall;
	int	fd;	/* the fd to repsond on */
	QLock	*wrlk;	/* write lock on fd */
	int	sz;	/* the size of the message buf */
	uchar	buf[];
};

struct Tree {
	Lock	lk;
	vlong	bp;
	vlong	bh;
	int	ht;
};

/*
 * Overall state of the file sytem.
 * Shadows the superblock contents.
 */
struct Gefs {
	int	blksz;
	int	bufsz;
	int	pivsz;
	int	hdrsz;

	QLock	snaplk;
	Blk*	super;

	Chan	*wrchan;
	Chan	*rdchan;

	int	fd;
	long	broken;

	/* protected by rootlk */
	Tree	root;

	Lock	genlk;
	vlong	gen;
	Lock	qidlk;
	vlong	nextqid;

	Arena	*arenas;
	int	narena;
	long	nextarena;
	vlong	arenasz;

	Lock	fidtablk;
	Fid	*fidtab[Nfidtab];
	Lock	dtablk;
	Dent	*dtab[Ndtab];

	Lock	lrulk;
	/* protected by lrulk */
	Bucket	*cache;
	Blk	*chead;
	Blk	*ctail;
	int	ccount;
	int	cmax;
};

struct Arena {
	Lock;
	Avltree *free;
	Avltree *partial;

	vlong	log;	/* log head */
	vlong	logh;	/* log head hash */
	Blk	*logtl;	/* tail block open for writing */
	Blk	*b;	/* arena block */

	Blk	**q;	/* write queue */
	vlong	nq;
};

struct Key{
	char	*k;
	int	nk;
};

struct Val {
	int type;
	union {
		/* block pointer */
		struct {
			uvlong	bp;
			uvlong	bh;
			ushort	fill;
		};
		/* inline values */
		struct {
			short	nv;
			char	*v;
		};
	};
};

struct Kvp {
	Key;
	Val;
};

struct Msg {
	char	op;
	Kvp;
};

struct Dent {
	RWLock;
	Key;
	Dent	*next;
	long	ref;

	Qid	qid;
	vlong	length;
	vlong	rootb;
	char	buf[Maxent];
};

struct Fid {
	Lock;
	Fid	*next;
	/*
	 * if opened with OEXEC, we want to use a snapshot,
	 * instead of the most recent root, to prevent
	 * paging in the wrong executable.
	 */
	Tree	root;

	u32int	fid;
	vlong	qpath;
	int	mode;
	int	iounit;

	Scan	*scan;	/* in progres scan */
	Dent	*dent;	/* (pqid, name) ref, modified on rename */
};

struct Path {
	/* Flowing down for flush */
	Blk	*b;	/* insertion */
	int	idx;	/* insert at */
	int	lo;	/* key range */
	int	hi;	/* key range */
	int	sz;	/* size of range */

	/* Flowing up from flush */
	Blk	*l;	/* left of split */
	Blk	*r;	/* right of split */
	Blk	*n;	/* shadowed node */
	/*
	 * If we merged or rotated, at least
	 * one of these nodes is not nil,
	 * and midx points at the left of
	 * the two nodes.
	 */
	Blk	*nl;	/* merged/rotated left */
	Blk	*nr;	/* merged/rotated right */
	int	midx;	/* merged index */
	char	split;	/* did we split? */
	char	merge;	/* did we merge? */
};

struct Scanp {
	int	bi;
	int	vi;
	Blk	*b;
};

struct Scan {
	vlong	offset;	/* last read offset */
	Tree	root;

	int	done;
	Kvp	kv;
	Key	pfx;
	char	kvbuf[Kvmax];
	char	pfxbuf[Keymax];
	Scanp	*path;
};

struct Blk {
	RWLock;

	/* cache entry */
	Blk	*cnext;
	Blk	*cprev;
	Blk	*hnext;

	short	flag;

	/* serialized to disk in header */
	short	type;	/* @0, for all */
	short	nval;	/* @2, for Leaf, Pivot: data[0:2] */
	short	valsz;	/* @4, for Leaf, Pivot: data[2:4] */
	short   nbuf;	/* @6, for Pivot */
	short   bufsz;	/* @8, for Pivot */

	vlong	logsz;	/* for allocation log */
	vlong	lognxt;	/* for allocation log */

	vlong	off;	/* -1 for unallocated */
	long	ref;	/* TODO: move out */
	char	*data;
	char	buf[Blksz];
};

struct Chan {
	int	size;	/* size of queue */
	long	count;	/* how many in queue (semaphore) */
	long	avail;	/* how many available to send (semaphore) */
	Lock	rl, wl;	/* circular pointers */
	void	**rp;
	void	**wp;
	void*	args[];	/* list of saved pointers, [->size] */
};
