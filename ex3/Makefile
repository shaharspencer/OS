CXX = g++
CXXFLAGS = -std=c++11 -Wall -Werror -Wextra -Wvla

all: libMapReduceFramework.a

libMapReduceFramework.a: MapReduceFramework.o Barrier.o
	ar rcs $@ $(OBJS)

MapReduceFramework.o: MapReduceFramework.h MapReduceClient.h Barrier/Barrier.h MapReduceFramework.cpp
	$(CXX) $(CXXFLAGS) MapReduceFramework.cpp -c -o MapReduceFramework.o

Barrier.o: Barrier/Barrier.h Barrier/Barrier.cpp
	$(CXX) $(CXXFLAGS) Barrier/Barrier.cpp -c -o Barrier.o

clean:
	rm -f *.o *.a
