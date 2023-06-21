CXX = g++

#SRCS = Barrier.h Barrier.cpp MapReduceFramework.cpp
SRCS = VirtualMemory.cpp
OBJS = $(SRCS:.cpp=.o)

INCS = -I.
CXXFLAGS = -Wall -Werror -Wextra -Wvla -std=c++11 -g $(INCS)

LIB = libVirtualMemory.a
TARGETS = $(LIB)

TAR = tar
TARFLAGS = -cvf
TARNAME = ex4.tar
TARSRCS = $(SRCS) Makefile README

all: $(TARGETS)

$(TARGETS): $(OBJS)
	ar rcs $@ $(OBJS)
	ranlib $@

clean:
	rm $(TARGETS) $(LIB) $(OBJS) *~ *core

depend:
	makedepend -- $(CXXFLAGS) -- $(SRCS)

tar:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)