CC = gcc
CFLAGS = -Wall -Wextra -g

SOURCES = main.c btree/btree.c database/database.c
HEADERS = constants.h structs.h btree/btree.h database/database.h
OBJECTS = $(SOURCES:.c=.o)
EXECUTABLE = database_program

all: $(EXECUTABLE)

$(EXECUTABLE): $(OBJECTS)
	$(CC) $(CFLAGS) $(OBJECTS) -o $@

%.o: %.c $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJECTS) $(EXECUTABLE)
