DATABASE = test
MODULES  = pa_grid

PG_CONFIG = pg_config
PGXS = $(shell $(PG_CONFIG) --pgxs)
INCLUDEDIR = $(shell $(PG_CONFIG) --includedir-server)
include $(PGXS)

$(MODULE).so: $(MODULE).o
	cc -shared -o $(MODULE).so $(MODULE).o

$(MODULE).o: $(MODULE).c
	cc -o $(MODULE).o -c $(MODULE).c $(CFLAGS) -g -I$(INCLUDEDIR)

sql:
	psql -d $(DATABASE) -f $(MODULES).sql

test: run_test
	$(RM) /tmp/LOG
	./run_test
	$(RM) ./run_test

run_test: run_test.c

new: clean install sql

backup:
	(cd ..;tar cvf - $(MODULES) | gzip > ~/$(MODULES).tar.gz; scp ~/$(MODULES).tar.gz silo3.ewi.utwente.nl:/local/flokstra/PostgresC)
