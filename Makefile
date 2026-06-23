CC      ?= cc
CFLAGS  ?= -O2 -Wall -Wextra -std=c11
PREFIX  ?= /usr/local
BINDIR  ?= $(PREFIX)/bin
MANDIR  ?= $(PREFIX)/share/man/man1

coalesce: coalesce.c
	$(CC) $(CFLAGS) -o $@ $<

install: coalesce coalesce.1
	install -d $(DESTDIR)$(BINDIR)
	install -d $(DESTDIR)$(MANDIR)
	install -m 755 coalesce $(DESTDIR)$(BINDIR)/coalesce
	install -m 644 coalesce.1 $(DESTDIR)$(MANDIR)/coalesce.1

uninstall:
	rm -f $(DESTDIR)$(BINDIR)/coalesce $(DESTDIR)$(MANDIR)/coalesce.1

lint: coalesce.1
	@mandoc -Tlint coalesce.1 2>&1 | grep -vE 'mandoc\.db|referenced manual not found' | grep . && exit 1 || exit 0

clean:
	rm -f coalesce

.PHONY: install uninstall lint clean
