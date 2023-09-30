CC = gcc # compiler
CFLAGS = -Wall -g # compile flags
LIBS = -lpthread -lrt # libs

all: par_hash_table

par_hash_table: par_hash_table.o
	$(CC) -o par_hash_table par_hash_table.o $(LIBS)

%.o: %.c
	$(CC) $(CFLAGS) -c $*.c

%.o: %.c # generates the object files
	$(CC) $(CFLAGS) -c $*.c

clean: # cleans stuff
	rm -f par_hash_table *.o *~
