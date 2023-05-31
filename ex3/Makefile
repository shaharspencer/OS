CXX = g++

SRCS = Barrier.h Barrier.cpp MapReduceFramework.cpp
OBJS = $(SRCS:.cpp=.o)

INCS = -I.
CXXFLAGS = -Wall -Werror -Wextra -Wvla -std=c++11 -g -pthread $(INCS)

LIB = libMapReduceFramework.a
TARGETS = $(LIB)

TAR = tar
TARFLAGS = -cvf
TARNAME = ex3.tar
TARSRCS = $(SRCS) Makefile README

all: $(TARGETS)

$(TARGETS): $(OBJS)
	ar rcs $@ $(OBJS)
	ranlib $@

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm $(TARGETS) $(LIB) $(OBJS) *~ *core

depend:
	makedepend -- $(CXXFLAGS) -- $(SRCS)

tar:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)