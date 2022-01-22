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
typedef struct Path	Path;
typedef struct Scan	Scan;
typedef struct Dent	Dent;
typedef struct Scanp	Scanp;
typedef struct Arena	Arena;
typedef struct Arange	Arange;
typedef struct Bucket	Bucket;
typedef struct Chan	Chan;
typedef struct Tree	Tree;
typedef struct Dlist	Dlist;
typedef struct Mount	Mount;
typedef struct User	User;
typedef struct Stats	Stats;

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
	

	Hdrsz	= 10,
	Rootsz	= 4+Ptrsz,		/* root pointer */
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
	 * dir:  serialized Xdir
	 */
	Kdat,	/* qid[8] off[8] => ptr[16]:	pointer to data page */
	Kent,	/* pqid[8] name[n] => dir[n]:	serialized Dir */
	Klabel,	/* name[] => snapid[]:		snapshot label */
	Ktref,	/* tag[8] = snapid[]		scratch snapshot label */
	Ksnap,	/* sid[8] => ref[8], tree[52]:	snapshot root */
	Ksuper,	/* qid[8] => Kent:		parent dir */
	Kdirty,	/* [0] => [0]:			mark dirty unmount */
};

enum {
	Bdirty	= 1 << 0,
	Bqueued	= 1 << 1,
	Bfinal	= 1 << 2,
	Bfreed	= 1 << 3,
	Bcached	= 1 << 4,
};

/* internal errors */
#define Eimpl	"not implemented"
#define Efs	(abort(), "fs broke")
#define Ebotch	"protocol botch"
#define Eio	"i/o error"
#define Efid	"unknown fid"
#define Etype	"invalid fid type"
#define Edscan	"invalid dir scan offset"
#define Eexist	"directory entry not found"
#define Emode	"unknown mode"
#define Efull	"file system full"
#define Eauth	"authentication failed"
#define Elength	"name too long"
#define Eperm	"permission denied"
#define Einuse	"resource in use"
#define Ebadf	"invalid file"
#define Emem	"out of memory"
#define Ename	"create/wstat -- bad character in file name"
#define Enomem	"out of memory"
#define Eattach	"attach required"
#define Enosnap	"attach -- bad specifier"
#define Edir	"invalid directory"
#define Esyntax "syntax error"
#define Enouser	"user does not exist"
#define Efsize	"file too big"
#define Ebadu	"attach -- unknown user or failed authentication"
#define Erdonly	"file system read only"
#define Elocked	"open/create -- file is locked"
#define Eauthp	"authread -- auth protocol not finished"
#define Eauthd	"authread -- not enough data"
#define Ephase	"auth phase error"
#define Enone	"auth -- user 'none' requires no authentication"
#define Enoauth	"auth -- authentication disabled"

#define Ewstatb	"wstat -- unknown bits in qid.type/mode"
#define Ewstatd	"wstat -- attempt to change directory"
#define Ewstatg	"wstat -- not in group"
#define Ewstatl	"wstat -- attempt to make length negative"
#define Ewstatm	"wstat -- attempt to change muid"
#define Ewstato	"wstat -- not owner or group leader"
#define Ewstatp	"wstat -- attempt to change qid.path"
#define Ewstatq	"wstat -- qid.type/dir.mode mismatch"
#define Ewstatu	"wstat -- not owner"
#define Ewstatv	"wstat -- attempt to change qid.vers"
#define Enempty	"remove -- directory not empty"


//#define Echar		"bad character in directory name"
//#define Eopen		"read/write -- on non open fid"
//#define Ecount	"read/write -- count too big"
//#define Ealloc	"phase error -- directory entry not allocated"
//#define Eqid		"phase error -- qid does not match"
//#define Eaccess	"access permission denied"
//#define Eentry	"directory entry not found"
//#define Emode		"open/create -- unknown mode"
//#define Edir1		"walk -- in a non-directory"
//#define Edir2		"create -- in a non-directory"
//#define Ephase	"phase error -- cannot happen"
//#define Eexist	"create/wstat -- file exists"
//#define Edot		"create/wstat -- . and .. illegal names"
//#define Ewalk		"walk -- too many (system wide)"
//#define Eoffset	"read/write -- offset negative"
//#define Ebroken	"read/write -- lock is broken"
//#define Eauth		"attach -- authentication failed"
//#define Eauth2	"read/write -- authentication unimplemented"
//#define Etoolong	"name too long"
//#define Efidinuse	"fid in use"
//#define Econvert	"protocol botch"
//#define Eversion	"version conversion"
//#define Eauthnone	"auth -- user 'none' requires no authentication"
//#define Eauthdisabled	"auth -- authentication disabled",	/* development */
//#define Eauthfile	"auth -- out of auth files"

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
 *	pad[4]sure, 
 *
 * Within these nodes, pointers have the following
 * layout:
 *
 *	off[8] hash[8] fill[2]
 */
enum {
	Tnone,
	Traw,
	Tpivot,
	Tleaf,
	Tsuper,
	Tarena,
	Tlog,
	Tdead,
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
	Nmsgtype,	/* maximum message type */
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
	vlong	hash;
	vlong	gen;
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
	int	fd;	/* the fd to repsond on */
	int	sz;	/* the size of the message buf */
	Amsg	*a;	/* admin messages */
	uchar	buf[];
};

struct Tree {
	Lock	lk;
	Tree	*snext;

	long	memref;	/* number of in-memory references to this */
	int	dirty;
	int	ref;	/* number of on-disk references to this */
	int	ht;
	Bptr	bp;
	vlong	gen;
	Dlist	dead[Ndead];
};

struct Bfree {
	Bptr	bp;
	Bfree	*next;
};

struct User {
	int	id;
	int	lead;
	int	*memb;
	int	nmemb;
	char	name[128];
};

struct Stats {
	vlong	cachehit;
	vlong	cachelook;
};

/*
 * Overall state of the file sytem.
 * Shadows the superblock contents.
 */
struct Gefs {
	/* immutable data */
	int	blksz;	/* immutable */
	int	bufsz;	/* immutable */
	int	pivsz;	/* immutable */
	int	hdrsz;	/* immutable */

	QLock	snaplk;	/* snapshot lock */
	Mount	*mounts;
	Tree	*osnap;
	Blk	*super;

	Chan	*wrchan;
	Chan	*rdchan;
	int	nquiesce;

	Lock	activelk;
	int	active[32];
	int	lastactive[32];
	Lock	freelk;
	Bfree	*freep;
	Bfree	*freehd;

	int	fd;
	long	broken;
	long	rdonly;
	int	noauth;

	/* root snapshot tree */
	Tree	snap;

	vlong	nextqid;
	vlong	nextgen;

	/* arena allocation */
	Arena	*arenas;
	int	narena;
	long	roundrobin;
	vlong	arenasz;

	/* user list */
	RWLock	userlk;
	User	*users;
	int	nusers;

	/* fid hash table */
	Lock	fidtablk;
	Fid	*fidtab[Nfidtab];

	/* dent hash table */
	Lock	dtablk;
	Dent	*dtab[Ndtab];

	/* slow block io */
	QLock	blklk[32];

	/* protected by lrulk */
	Lock	lrulk;
	Bucket	*cache;
	Blk	*chead;
	Blk	*ctail;
	int	ccount;
	int	cmax;

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
	/* freelist */
	Bptr	head;
	Blk	*tail;	/* tail held open for writing */
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

struct Xdir {
	/* file data */
	Qid	qid;	/* unique id from server */
	int	mode;	/* permissions */
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
	long	ref;

	char	buf[Maxent];
};

struct Mount {
	Lock;
	Mount	*next;
	long	ref;
	vlong	gen;
	char	*name;
	Tree	*root;
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
	long	ref;
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

struct Path {
	/* Flowing down for flush */
	Msg	*ins;	/* inserted values, bounded by lo..hi */
	Blk	*b;	/* to shadow */
	int	idx;	/* insert at */
	int	lo;	/* key range */
	int	hi;	/* key range */
	int	sz;	/* size of range */

	/* Flowing up from flush */
	int	op;	/* change done along path */
	Blk	*m;	/* node merged against, for post-update free */
	Blk	*nl;	/* new left */
	Blk	*nr;	/* new right, if we split or rotated */
	int	midx;	/* modification index */
	int	npull;	/* number of messages successfully pulled */
	int	pullsz;	/* size of pulled messages */
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
	int	overflow;
	int	present;
	Kvp	kv;
	Key	pfx;
	char	kvbuf[Kvmax];
	char	pfxbuf[Keymax];
	Scanp	*path;
};

struct Blk {
	/* cache entry */
	Blk	*cnext;
	Blk	*cprev;
	Blk	*hnext;

	/* Freelist entry */
	Blk	*fnext;

	long	flag;

	/* serialized to disk in header */
	short	type;	/* @0, for all */
	short	nval;	/* @2, for Leaf, Pivot: data[0:2] */
	short	valsz;	/* @4, for Leaf, Pivot: data[2:4] */
	short   nbuf;	/* @6, for Pivot */
	short   bufsz;	/* @8, for Pivot */

	vlong	logsz;	/* for allocation log */
	vlong	lognxt;	/* for allocation log */

	uintptr	freed;	/* debug */

	Bptr	bp;
	long	ref;
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
