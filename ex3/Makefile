CXX = g++

SRCS = *.cpp
OBJS = $(SRCS:.cpp=.o)

INCS = -I.
CXXFLAGS = -Wall -Werror -Wextra -Wvla -std=c++11 -g -pthread $(INCS)

LIB = libMapReduceFramework.a
TARGETS = $(LIB)

TAR = tar
TARFLAGS = -cvf
TARNAME = ex3.tar
TARSRCS = Barrier.h Barrier.cpp MapReduceFramework.cpp Makefile README

all: $(TARGETS)

$(TARGETS): $(OBJS)
	ar rcs $@ $(OBJS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -rf *.o $(LIB)

depend:
	makedepend -- $(CXXFLAGS) -- $(SRCS)

tar:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)