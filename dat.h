typedef struct Blk	Blk;
typedef struct Amsg	Amsg;
typedef struct Gefs	Gefs;
typedef struct Fmsg	Fmsg;
typedef struct Fid	Fid;
typedef struct Msg	Msg;
typedef struct Key	Key;
typedef struct Val	Val;
typedef struct Kvp	Kvp;
typedef struct Xdir	Xdir;
typedef struct Bptr	Bptr;
typedef struct Bfree	Bfree;
typedef struct Scan	Scan;
typedef struct Dent	Dent;
typedef struct Scanp	Scanp;
typedef struct Fshdr	Fshdr;
typedef struct Arena	Arena;
typedef struct Arange	Arange;
typedef struct Bucket	Bucket;
typedef struct Chan	Chan;
typedef struct Syncq	Syncq;
typedef struct Tree	Tree;
typedef struct Dlist	Dlist;
typedef struct Mount	Mount;
typedef struct User	User;
typedef struct Stats	Stats;
typedef struct Conn	Conn;

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
	Nsec	= 1000LL*1000*1000,	/* nanoseconds to the second */
	Maxname	= 256,			/* maximum size of a name element */
	Maxent	= 9+Maxname+1,		/* maximum size of ent key, with terminator */
	Maxtag	= 1<<16,		/* maximum tag in 9p */

	/*
	 * Kpmax must be no more than 1/4 of pivspc, or
	 * there is no way to get a valid split of a
	 * maximally filled tree.
	 */
	Keymax	= 128,			/* key data limit */
	Inlmax	= 512,			/* inline data limit */
	Ptrsz	= 24,			/* off, hash, gen */
	Pptrsz	= 26,			/* off, hash, gen, fill */
	Fillsz	= 2,			/* block fill count */
	Offksz	= 17,			/* type, qid, off */
	Snapsz	= 9,			/* tag, snapid */
	Dpfxsz	= 9,
	Ndead	= 8,			/* number of deadlist heads */
	Deadsz	= 8+8+8,		/* prev, head, hash */
	Treesz	= 4+4+8+Ptrsz+Ndead*Deadsz,	/* ref, height, gen, root, deadlist */
	Kvmax	= Keymax + Inlmax,	/* Key and value */
	Kpmax	= Keymax + Ptrsz,	/* Key and pointer */
	Wstatmax = 4+8+8+8,		/* mode, size, atime, mtime */
	

	Pivhdsz		= 10,
	Leafhdsz	= 6,
	Loghdsz		= 2,
	Loghashsz	= 8,
	Rootsz		= 4+Ptrsz,	/* root pointer */
	Pivsz		= Blksz - Pivhdsz,
	Bufspc		= (Blksz - Pivhdsz) / 2,
	Pivspc		= Blksz - Pivhdsz - Bufspc,
	Logspc		= Blksz - Loghdsz,
	Leafspc 	= Blksz - Leafhdsz,
	Msgmax  	= 1 + (Kvmax > Kpmax ? Kvmax : Kpmax)
};

enum {
	Eactive	= 1UL<<30,	/* epoch active flag */
};

enum {
	/*
	 * dent: pqid[8] qid[8] -- a directory entry key.
	 * ptr:  off[8] hash[8] -- a key for an Dir block.
	 * dir:  serialized Xdir
	 */
	Kdat,	/* qid[8] off[8] => ptr[16]:	pointer to data page */
	Kent,	/* pqid[8] name[n] => dir[n]:	serialized Dir */
	Klabel,	/* name[] => snapid[]:		snapshot label */
	Ktref,	/* tag[8] = snapid[]		scratch snapshot label */
	Ksnap,	/* sid[8] => ref[8], tree[52]:	snapshot root */
	Ksuper,	/* qid[8] => Kent:		parent dir */
};

enum {
	Bdirty	= 1 << 0,
	Bfinal	= 1 << 1,
	Bfreed	= 1 << 2,
	Bcached	= 1 << 3,
};

enum {
	Qdump = 1ULL << 63,
};

/* internal errors */
#define Efs	(abort(), "fs broke")
extern char Eimpl[];
extern char Ebotch[];
extern char Eio[];
extern char Efid[];
extern char Etype[];
extern char Edscan[];
extern char Eexist[];
extern char Emode[];
extern char Efull[];
extern char Eauth[];
extern char Elength[];
extern char Eperm[];
extern char Einuse[];
extern char Ebadf[];
extern char Ename[];
extern char Enomem[];
extern char Eattach[];
extern char Enosnap[];
extern char Edir[];
extern char Esyntax[];
extern char Enouser[];
extern char Efsize[];
extern char Ebadu[];
extern char Erdonly[];
extern char Elocked[];
extern char Eauthp[];
extern char Eauthd[];
extern char Ephase[];
extern char Enone[];
extern char Enoauth[];
extern char Einval[];

extern char Ewstatb[];
extern char Ewstatd[];
extern char Ewstatg[];
extern char Ewstatl[];
extern char Ewstatm[];
extern char Ewstato[];
extern char Ewstatp[];
extern char Ewstatq[];
extern char Ewstatu[];
extern char Ewstatv[];
extern char Enempty[];

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
 *	blksz[4]	block size in bytes
 *	bufsz[4]	portion of leaf nodes
 *			allocated to buffers,
 *			in bytes
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
 *	pad[4]sure, 
 *
 * Within these nodes, pointers have the following
 * layout:
 *
 *	off[8] hash[8] fill[2]
 */
enum {
	Traw,
	Tpivot,
	Tleaf,
	Tlog,
	Tdead,
	Tmagic,
	Tarena = 0x6765,	/* 'ge' bigendian */
};

enum {
	Vinl,	/* Inline value */
	Vref,	/* Block pointer */
};

enum {
	GBraw	= 1<<0,
	GBwrite	= 1<<1,
	GBnochk	= 1<<2,
};

enum {
	Onop,		/* nothing */
	Oinsert,	/* new kvp */
	Odelete,	/* delete kvp */
	Oclearb,	/* free block ptr if exists */
	Owstat,		/* kvp dirent */
	Orefsnap,	/* increase snap ref count */
	Nmsgtype,	/* maximum message type */
};

enum {
	Magic = 0x979b929e98969c8c,
};

/*
 * Wstat ops come with associated data, in the order
 * of the bit flags.
 */
enum{
	/* wstat flags */
	Owsize	= 1<<0,	/* [8]fsize: update file size */
	Owmode	= 1<<1,	/* [4]mode: update file mode */
	Owmtime	= 1<<2, /* [8]mtime: update mtime, in nsec */
	Owatime	= 1<<3, /* [8]atime: update atime, in nsec */
	Owuid	= 1<<4,	/* [4]uid: set uid */
	Owgid	= 1<<5,	/* [4]uid: set gid */
	Owmuid	= 1<<6,	/* [4]uid: set muid */
};

/*
 * Operations for the allocation log.
 */
enum {
	LogNop,		/* unused */
	/* 1-wide entries */
	LogAlloc1,	/* alloc a block */
	LogFree1,	/* free a block */
	LogChain,	/* point to next log block */
	LogEnd,		/* last entry in log */	

	/* 2-wide entries */
#define	Log2wide	LogAlloc
	LogAlloc,	/* alloc a range */
	LogFree,	/* free a range */
};

/*
 * Operations for the deadlist log
 */
enum {
	DlEnd,		/* no data */
	DlChain,	/* [8]addr, [8]hash */
	DlGraft,	/* [8]addr, [8]hash */
	DlKill,		/* [8]addr, [8]gen */
};

enum {
	AOnone,
	AOsnap,
	AOsync,
};

struct Bptr {
	vlong	addr;
	uvlong	hash;
	vlong	gen;
};

struct Key{
	char	*k;
	int	nk;
};

struct Val {
	short	nv;
	char	*v;
};

struct Kvp {
	Key;
	Val;
};

struct Msg {
	char	op;
	Kvp;
};

struct Dlist {
	vlong	prev;	/* previous generation */
	Bptr	head;	/* first flushed block */
	Blk	*ins;	/* inserted block */
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

struct Amsg {
	int	op;
	int	fd;
	union {
		struct {	/* AOsnap */
			char	old[128];
			char	new[128];
		};
		struct {	/* AOsync */
			int	halt;
		};
	};
};

struct Fmsg {
	Fcall;
	Conn	*conn;
	int	sz;	/* the size of the message buf */
	Fmsg	*flush;	/* the flush message */
	Amsg	*a;	/* admin messages */
	uchar	buf[];
};

struct Tree {
	Lock	lk;
	Tree	*snext;

	atomic_long memref;	/* number of in-memory references to this */
	int	dirty;
	int	ref;	/* number of on-disk references to this */
	int	ht;
	Bptr	bp;
	vlong	gen;
	Msg	flush[16];
	int	nflush;
	int	flushsz;
	char	flushbuf[Bufspc/2];
	Dlist	dead[Ndead];
};

struct Bfree {
	Bfree	*next;
	Blk	*b;
	Bptr	bp;
};

struct User {
	int	id;
	int	lead;
	int	*memb;
	int	nmemb;
	char	name[128];
};

struct Syncq {
	QLock	lk;
	Rendez	fullrz;
	Rendez	emptyrz;
	Blk	**heap;
	int	nheap;
	int	heapsz;
};

struct Stats {
	vlong	cachehit;
	vlong	cachelook;
};

struct Fshdr {
	int	blksz;
	int	bufspc;
	Tree	snap;
	int	narena;
	vlong	arenasz;
	atomic_llong	nextqid;
	atomic_llong	nextgen;
};

/*
 * Overall state of the file sytem.
 * Shadows the superblock contents.
 */
struct Gefs {
	Fshdr;
	/* arena allocation */
	Arena	*arenas;
	atomic_long roundrobin;
	long	syncing;
	long	nsyncers;

	int	gotinfo;
	QLock	synclk;
	Rendez	syncrz;

	QLock	snaplk;	/* snapshot lock */
	Tree	*opensnap;
	Lock	mountlk;
	Mount	*mounts;
	Mount	*snapmnt;
	Lock	connlk;
	Conn	*conns;

	Chan	*wrchan;
	Chan	*rdchan;

	int	nworker;
	Lock	freelk;
	atomic_llong	qgen;
	atomic_long	epoch;
	atomic_long	lepoch[32];
	Bfree	*limbo[3];

	Syncq	syncq[32];


	int	fd;
	atomic_long broken;
	atomic_long rdonly;
	int	noauth;
	int	noperm;

	/* user list */
	RWLock	userlk;
	User	*users;
	int	nusers;

	/* dent hash table */
	Lock	dtablk;
	Dent	*dtab[Ndtab];

	/* slow block io */
	QLock	blklk[32];

	/* protected by lrulk */
	QLock	lrulk;
	Rendez	lrurz;
	Bucket	*cache;
	Blk	*blks;	/* all blocks for debugging */
	Blk	*chead;
	Blk	*ctail;
	usize	ccount;
	usize	cmax;

	Lock	mflushlk;
	Fmsg	*mflush[Maxtag];

	Stats	stats;
};

struct Arena {
	Lock;
	Avltree *free;
	Blk	**queue;
	int	nqueue;
	Blk	*b;	/* arena block */
	Blk	**q;	/* write queue */
	vlong	nq;
	vlong	size;
	vlong	used;
	vlong	reserve;
	/* freelist */
	Bptr	head;
	Blk	*tail;	/* tail held open for writing */
	Syncq	*sync;
};

struct Xdir {
	/* file data */
	Qid	qid;	/* unique id from server */
	ulong	mode;	/* permissions */
	vlong	atime;	/* last read time: nsec */
	vlong	mtime;	/* last write time: nsec */
	uvlong	length;	/* file length */
	int	uid;	/* owner name */
	int	gid;	/* group name */
	int	muid;	/* last modifier name */
	char	*name;	/* last element of path */
};

struct Dent {
	RWLock;
	Key;
	Xdir;
	Dent	*next;
	atomic_long ref;

	char	buf[Maxent];
};

struct Mount {
	Lock;
	Mount	*next;
	atomic_long ref;
	vlong	gen;
	char	*name;
	Tree	*root;
};

struct Conn {
	Conn	*next;
	int	rfd;
	int	wfd;
	int	iounit;
	int	versioned;

	/* fid hash table */
	Lock	fidtablk;
	Fid	*fidtab[Nfidtab];
};

struct Fid {
	Lock;
	Fid	*next;
	/*
	 * if opened with OEXEC, we want to use a snapshot,
	 * instead of the most recent root, to prevent
	 * paging in the wrong executable.
	 */
	Mount	*mnt;
	Scan	*scan;	/* in progres scan */
	Dent	*dent;	/* (pqid, name) ref, modified on rename */	
	void	*auth;

	u32int	fid;
	vlong	qpath;
	vlong	pqpath;
	atomic_long ref;
	int	mode;
	int	iounit;

	int	uid;
	int	duid;
	int	dgid;
	int	dmode;
};

enum {
	POmod,
	POrot,
	POsplit,
	POmerge,
};

struct Scanp {
	int	bi;
	int	vi;
	Blk	*b;
};

struct Scan {
	vlong	offset;	/* last read offset */

	int	done;
	int	overflow;
	int	present;
	Kvp	kv;
	Key	pfx;
	char	kvbuf[Kvmax];
	char	pfxbuf[Keymax];
	Scanp	*path;
	int	pathsz;
};

struct Blk {
	/* cache entry */
	Blk	*cnext;
	Blk	*cprev;
	Blk	*hnext;

	/* Freelist entry */
	Blk	*fnext;

	atomic_long flag;
	long	qgen;

	/* serialized to disk in header */
	short	type;	/* @0, for all */
	short	nval;	/* @2, for Leaf, Pivot: data[0:2] */
	short	valsz;	/* @4, for Leaf, Pivot: data[2:4] */
	short   nbuf;	/* @6, for Pivot */
	short   bufsz;	/* @8, for Pivot */

	vlong	logsz;	/* for allocation log */
	vlong	lognxt;	/* for allocation log */

	/* debug */
	uintptr lasthold;
	uintptr lasthold0;
	uintptr lasthold1;

	uintptr lastdrop;
	uintptr lastdrop0;
	uintptr lastdrop1;

	uintptr cached;
	uintptr uncached;
	uintptr	alloced;
	uintptr	freed;

	Bptr	bp;
	atomic_long	ref;
	char	*data;
	char	buf[Blksz];
	vlong	magic;
};

struct Chan {
	int	size;	/* size of queue */
	sem_t	count;	/* how many in queue (semaphore) */
	sem_t	avail;	/* how many available to send (semaphore) */
	Lock	rl, wl;	/* circular pointers */
	void	**rp;
	void	**wp;
	void*	args[];	/* list of saved pointers, [->size] */
};
