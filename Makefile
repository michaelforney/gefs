.POSIX:

CC=gcc
CFLAGS+=-fplan9-extensions -I compat
CFLAGS+=-Wall
CFLAGS+=-Wno-shift-count-overflow -Wno-sizeof-array-div -Wno-overflow
CFLAGS+=-Wno-int-to-pointer-cast -Wno-unknown-pragmas -Wno-parentheses
CFLAGS+=-Wno-unused-value -Wno-missing-braces -Wno-char-subscripts
LDLIBS=-lpthread

-include config.mk

OFILES=\
	blk.o\
	cache.o\
	check.o\
	cons.o\
	dump.o\
	error.o\
	fs.o\
	hash.o\
	load.o\
	main.o\
	pack.o\
	ream.o\
	snap.o\
	tree.o\
	user.o\
	compat/argv0.o\
	compat/atom.o\
	compat/avl.o\
	compat/convM2D.o\
	compat/convM2S.o\
	compat/convS2M.o\
	compat/dofmt.o\
	compat/encodefmt.o\
	compat/errfmt.o\
	compat/errstr.o\
	compat/exits.o\
	compat/fcallfmt.o\
	compat/fltfmt.o\
	compat/fmt.o\
	compat/fmtfd.o\
	compat/fmtlock.o\
	compat/fmtprint.o\
	compat/fprint.o\
	compat/getcallerpc.o\
	compat/lock.o\
	compat/malloc.o\
	compat/nsec.o\
	compat/print.o\
	compat/readn.o\
	compat/rune.o\
	compat/seprint.o\
	compat/snprint.o\
	compat/sprint.o\
	compat/strecpy.o\
	compat/sysfatal.o\
	compat/tokenize.o\
	compat/u16.o\
	compat/u32.o\
	compat/u64.o\
	compat/utflen.o\
	compat/utfrune.o\
	compat/vfprint.o\
	compat/vseprint.o\
	compat/vsnprint.o\
	compat/binit.o\
	compat/bread.o\
	compat/bflush.o\
	compat/blethal.o

HFILES=\
	dat.h\
	fns.h\
	compat/u.h\
	compat/libc.h\
	compat/avl.h\
	compat/fcall.h

.PHONY: all
all: gefs

gefs: $(OFILES)
	$(CC) $(LDFLAGS) -o $@ $(OFILES) $(LDLIBS)

$(OFILES): $(HFILES)

.PHONY: clean
clean:
	rm -rf gefs $(OFILES)
