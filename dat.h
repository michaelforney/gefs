typedef struct Blk	Blk;
typedef struct Gefs	Gefs;
typedef struct Msg	Msg;
typedef struct Key	Key;
typedef struct Val	Val;
typedef struct Kvp	Kvp;
typedef struct Path	Path;

enum {
	// buffer for the whole block
	Blksz	= 256,
	// will store what type of block
	Hdrsz	= 16,
	Blkspc	= Blksz - Hdrsz,
	// space for message buffer
	Bufspc  = Blkspc / 2,
	// space for pivot keys and offsets
	Pivspc	= Blkspc - Bufspc,
	Leafspc = Blkspc,
	Keymax	= 16,
	Inlmax	= 64,
	Ptrsz	= 18,
	Kvmax	= Keymax + Inlmax,	/* Key and value */
	Kpmax	= Keymax + Ptrsz,	/* Key and pointer */
	Msgmax  = 1 + (Kvmax > Kpmax ? Kvmax : Kpmax)
};

enum {
	Bdirty = 1 << 0,
};

#define Efs	"i will not buy this fs, it is scratched"
#define Eexist	"does not exist"
#define Ebotch	"protocol botch"
#define Emode	"unknown mode"
#define Efull	"file system full"
#define Eauth	"authentication failed"
#define Elength	"name too long"
#define Eperm	"permission denied"

/*
 * The type of block. Pivot nodes are internal to the tree,
 * while leaves inhabit the edges. Pivots are split in half,
 * containing a buffer for the data, and keys to direct the
 * searches. Their buffers contain messages en-route to the
 * leaves.
 *
 * Leaves contain the key, and some chunk of data as the
 * value.
 */
enum {
	Pivot,
	Leaf,
};

enum {
	Vinl,	/* Inline value */
	Vref,	/* Block pointer */
};

enum {
	Ocreate,
	Odelete,
	Owrite,
	Owstat,
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

	int	height;
	Blk	*root;
	
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
	Blk	*ml;	/* merged/rotated left */
	Blk	*mr;	/* merged/rotated right */
	int	midx;	/* merged index */
	char	split;	/* did we split? */
	char	merge;	/* did we merge? */
};

struct Blk {
	char	type;
	char	flag;
	short	nent;
	short	valsz;
	short   nmsg;
	short   bufsz;
	vlong	off;	/* -1 for unallocated */
	int	refs;	/* TODO: move out */
	char	data[Blksz];
};
