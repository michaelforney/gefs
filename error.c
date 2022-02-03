#include <u.h>
#include <libc.h>
#include <avl.h>
#include <fcall.h>
#include "dat.h"

char Eimpl[]	= "not implemented";
char Ebotch[]	= "protocol botch";
char Eio[]	= "i/o error";
char Efid[]	= "unknown fid";
char Etype[]	= "invalid fid type";
char Edscan[]	= "invalid dir scan offset";
char Eexist[]	= "directory entry not found";
char Emode[]	= "unknown mode";
char Efull[]	= "file system full";
char Eauth[]	= "authentication failed";
char Elength[]	= "name too long";
char Eperm[]	= "permission denied";
char Einuse[]	= "resource in use";
char Ebadf[]	= "invalid file";
char Emem[]	= "out of memory";
char Ename[]	= "create/wstat -- bad character in file name";
char Enomem[]	= "out of memory";
char Eattach[]	= "attach required";
char Enosnap[]	= "attach -- bad specifier";
char Edir[]	= "invalid directory";
char Esyntax[]	= "syntax error";
char Enouser[]	= "user does not exist";
char Efsize[]	= "file too big";
char Ebadu[]	= "attach -- unknown user or failed authentication";
char Erdonly[]	= "file system read only";
char Elocked[]	= "open/create -- file is locked";
char Eauthp[]	= "authread -- auth protocol not finished";
char Eauthd[]	= "authread -- not enough data";
char Ephase[]	= "auth phase error";
char Enone[]	= "auth -- user 'none' requires no authentication";
char Enoauth[]	= "auth -- authentication disabled";

char Ewstatb[]	= "wstat -- unknown bits in qid.type/mode";
char Ewstatd[]	= "wstat -- attempt to change directory";
char Ewstatg[]	= "wstat -- not in group";
char Ewstatl[]	= "wstat -- attempt to make length negative";
char Ewstatm[]	= "wstat -- attempt to change muid";
char Ewstato[]	= "wstat -- not owner or group leader";
char Ewstatp[]	= "wstat -- attempt to change qid.path";
char Ewstatq[]	= "wstat -- qid.type/dir.mode mismatch";
char Ewstatu[]	= "wstat -- not owner";
char Ewstatv[]	= "wstat -- attempt to change qid.vers";
char Enempty[]	= "directory is not empty";

//char Echar[]		= "bad character in directory name";
//char Eopen[]		= "read/write -- on non open fid";
//char Ecount[]		= "read/write -- count too big";
//char Ealloc[]		= "phase error -- directory entry not allocated";
//char Eqid[]		= "phase error -- qid does not match";
//char Eaccess[]	= "access permission denied";
//char Eentry[]		= "directory entry not found";
//char Emode[]		= "open/create -- unknown mode";
//char Edir1[]		= "walk -- in a non-directory";
//char Edir2[]		= "create -- in a non-directory";
//char Ephase[]		= "phase error -- cannot happen";
//char Eexist[]		= "create/wstat -- file exists";
//char Edot[]		= "create/wstat -- . and .. illegal names";
//char Ewalk[]		= "walk -- too many (system wide)";
//char Eoffset[]	= "read/write -- offset negative";
//char Ebroken[]	= "read/write -- lock is broken";
//char Eauth[]		= "attach -- authentication failed";
//char Eauth2[]		= "read/write -- authentication unimplemented";
//char Etoolong[]	= "name too long";
//char Efidinuse[]	= "fid in use";
//char Econvert[]	= "protocol botch";
//char Eversion[]	= "version conversion";
//char Eauthnone[]	= "auth -- user 'none' requires no authentication";
//char Eauthdisabled[]	= "auth -- authentication disabled";	/* development */
//char Eauthfile[]	= "auth -- out of auth files";
