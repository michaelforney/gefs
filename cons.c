#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <avl.h>

#include "dat.h"
#include "fns.h"

void
runcons(void *pfd)
{
	char buf[256], *arg[4];
	int fd, n, narg;

	fd = (uintptr)pfd;
	while(1){
		if((n = read(fd, buf, sizeof(buf)-1)) == -1)
			break;
		buf[n] = 0;
		narg = tokenize(buf, arg, nelem(arg));
		if(narg == 0 || strlen(arg[0]) == 0)
			continue;
		if(strcmp(arg[0], "show") == 0){
			switch(narg){
			case 1:
				showfs(fd, "show");
				break;
			case 2:
				if(strcmp(arg[1], "fid") == 0){
					showfids(fd);
					break;
				}
				if(strcmp(arg[1], "cache") == 0){
					showcache(fd);
					break;
				}
				/* wet floor */
			default:
				fprint(fd, "show me yours first\n");
			}
		}else if(strcmp(arg[0], "check") == 0)
			checkfs();
		else if(strcmp(arg[0], "dbg") && narg == 2)
			debug = atoi(arg[1]);
		else
			fprint(fd, "unknown command %s\n", arg[0]);
	}
}
