CC = g++
CFLAGS = -Wall -Wextra -Wvla -Werror -std=c++11

CPP_FILES = uthreads.cpp Thread.cpp Scheduler.cpp
HEADER_FILES = uthreads.h Thread.h Scheduler.h
HEADER = Thread.h Scheduler.h

ALL_FILES = $(CPP_FILES) $(HEADER) README Makefile

all: uthreadslib

uthreadslib: $(CPP_FILES) $(HEADER_FILES)
	$(CC) $(CFLAGS) $(CPP_FILES) -c
	ar rs libuthreads.a *.o

clean:
	rm -rf *.o *.a *.tar

tar: $(ALL_FILES)
	tar -cvf ex2.tar $(ALL_FILES)
