EXTENSION = gofdw

MODULE_big = gofdw
OBJS = gofdw.o gofdw.a
DATA_built = gofdw--0.1.sql

EXTRA_CLEAN = gofdw.h gofdw--*.sql

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

gofdw.a: gofdw.go
	go build -buildmode=c-archive -linkshared -o $@ $^

gofdw.h: gofdw.a

gofdw.o: gofdw.h

gofdw--0.1.sql: gofdw.sql
	cp $< $@
