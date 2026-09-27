# Makefile for MPI Distributed Library System

CC        = mpicc
CFLAGS    = -Wall -O2
TARGET    = distributed_library
SRC       = $(wildcard *.c)
OBJ       = $(SRC:.c=.o)
file ?=testfiles_hy486/testfile2/loaners_62_libs_25_np_88.txt

.PHONY: all clean run

all: $(TARGET)

#in my laptop i need also the flags: -g -O0
$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) -o -O0 $@ $^ 

%.o: %.c
	$(CC) $(CFLAGS) -c $<

run: $(TARGET)
	@echo "Usage: make run-<num_libs>  file=<testfile.txt>"
	@echo "Example: make run-N"
	@exit 1

# in my laptop i need: mpirun --oversubscribe -np $$NP ./$(TARGET) $* $(file)
run-%: $(TARGET)
	@echo "Running with N=$*"
	@NP=`echo "$* * $* + ($* * $* * $*) / 2 + 1" | bc`; \
	echo "Computed -np = $$NP"; \
	echo ""; \
	mpirun --oversubscribe -np $$NP ./$(TARGET) $* $(file)
	

clean:
	rm -f $(OBJ) $(TARGET)
