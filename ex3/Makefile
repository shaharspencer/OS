CXX = g++
CXXFLAGS = -std=c++11 -Wall -Werror -Wextra -Wvla -g -I. -I..

all: libMapReduceFramework.a

libMapReduceFramework.a: MapReduceFramework.o Barrier.o
	ar rcs $@ $(OBJS)
	ranlib $@

MapReduceFramework.o: MapReduceFramework.h MapReduceClient.h Barrier.h MapReduceFramework.cpp
	$(CXX) $(CXXFLAGS) MapReduceFramework.cpp -c -o MapReduceFramework.o

Barrier.o: Barrier.h Barrier.cpp
	$(CXX) $(CXXFLAGS) Barrier.cpp -c -o Barrier.o

clean:
	rm -f *.o *.a
