# Distributed Library System with MPI

This project models a network of libraries and borrowers as a distributed-memory C program. Each MPI process has a specific role: rank 0 coordinates the run, a group of processes manages libraries, and another group represents clients. The processes exchange typed messages to elect leaders, borrow and donate books, find the most popular book, and cross-check their loan totals.

The project is useful for exploring how a multi-step application can be split across MPI ranks and coordinated using point-to-point communication. It uses custom MPI datatypes, message tags, graph/grid topologies, and distributed tree-style traversals rather than relying on MPI collectives for the application logic.

## System model

The first command-line argument is `N`. It determines the library grid and the book inventory:

- There are `N × N` library processes, arranged as a two-dimensional grid.
- There are `floor(N³ / 2)` client processes.
- Rank 0 is the coordinator, so the total MPI process count is `N² + floor(N³ / 2) + 1`.
- Library ranks are `1` through `N²`; library ID is rank minus one.
- Client ranks follow the library ranks; client ID is rank minus one.
- Each library starts with `N` distinct book IDs and `N` copies of each. The book-ID range is assigned in consecutive blocks to libraries.

For example, `N=3` requires 23 MPI processes: rank 0, 9 libraries, and 13 clients. The included test cases use `N=3`, `N=4`, and `N=5`.

Each library knows its up/down/left/right grid neighbors. Client connections are instead defined by `CONNECT` records in the input file, producing an undirected graph. Clients use this graph for leader election, popular-book aggregation, and client-side loan counting. The graph should be connected for the distributed client operations to cover every client.

## Execution workflow

1. Every process enters `MPI_Init`. The program creates MPI datatypes for book records and popular-book results.
2. Rank 0 reads the input file in order. All other ranks remain in message-processing loops, waiting for tagged messages.
3. `CONNECT` commands construct the client graph. The coordinator asks one client to add its neighbor, and that client forwards the reciprocal connection request.
4. The library leader election starts across the library grid. The resulting leader ID and rank are made available to the processes that need to coordinate library operations.
5. The client leader election starts across the client graph. The leader is reported to the coordinator and propagated to client processes.
6. The coordinator issues borrowing, donation, popular-book, and loan-count operations as they appear in the input file. It waits for the relevant acknowledgements before processing the next command.
7. Once input processing ends, rank 0 sends a termination message to all worker ranks. Each process leaves its service loop and the program calls `MPI_Finalize`.

This ordering matters: commands that depend on a client or library leader should appear after the corresponding election command, and client graph connections should be established before the client election.

## Operations and message flow

### Client graph setup and leader elections

`CONNECT a b` adds a connection between client IDs `a` and `b`. The coordinator sends the request to client `a`; that process records `b`, sends a reciprocal request to client `b`, and acknowledges the coordinator. Neighbor lists avoid duplicate edges.

The library election begins with `START_LE_LIBR`. The library processes explore their grid-neighbor graph using messages carrying candidate leader IDs, parent notifications, and completion notifications. A higher candidate ID can replace a lower candidate during exploration; the completed traversal communicates the selected library leader.

The client election begins with `START_LE_LOANERS`. Client processes run the election over the configured client graph and report the selected client leader to the coordinator and the clients. The leader is then used as the entry point for graph-wide client operations.

### Borrowing a book

`TAKE_BOOK client_id book_id` asks a client to borrow a particular book. The client determines its assigned home library and sends the request there. The home library first checks its original inventory and donated inventory. If it cannot fulfill the request, it asks the library leader to locate a holder. The leader checks donated inventories and then the book's original library assignment; the selected library decrements its available copies and replies with the book record. The client records successful loans and how often it has borrowed each title. If no copy can be found, the client prints a no-available-copies message.

The home library is derived from the client's ID using the `CLIENT_FIRST_ID` and `CLIENT_LAST_ID` macros in `decl.h`; book ownership is derived from the book-ID range macros in the same file.

### Donating copies

`DONATE_BOOK client_id book_id copies` submits a donation through the specified client. If that client is not the client leader, it forwards the request to the leader. The leader distributes the requested number of copies across libraries in a round-robin pattern, acknowledging each receiving library. A library stores donated inventory separately from its original collection. The simulation assigns a generated price to donated book records; the input specifies the ID and number of copies, not a price.

### Finding the most popular book

`GET_MOST_POPULAR_BOOK` starts at the client leader and traverses the client graph. Each client reports the best book in its own rental history, while intermediate clients combine results from their subtrees and return one best result to their parent. Popularity is the number of times a client has rented a title; ties are resolved in favor of the higher price. The leader prints the winning book ID, price, and rental count.

### Checking the loan total

`CHECK_NUM_BOOKS_LOANED` asks the library side and client side to independently aggregate their loan counts. Each side traverses its respective topology and sends a total to rank 0. The coordinator prints success when the totals agree and failure otherwise. This provides a consistency check between the library-side fulfillment accounting and client-side successful-loan accounting.

## Input-file format

The input is a sequence of newline-separated commands. Commands are processed in file order:

```text
CONNECT <client_id> <client_id>
START_LE_LIBR
START_LE_LOANERS
TAKE_BOOK <client_id> <book_id>
DONATE_BOOK <client_id> <book_id> <copies>
GET_MOST_POPULAR_BOOK
CHECK_NUM_BOOKS_LOANED
```

The operations are optional, but their dependencies and ordering must be respected. In particular, construct the client graph before starting its election; run elections before commands that require the leaders; and use valid client IDs for the chosen `N`. The parser stops processing when it encounters an unrecognized line, so avoid adding explanatory comments or blank lines unless the parser is updated to ignore them.

## Build and run

The program requires a C compiler with MPI support (`mpicc`) and an MPI runtime (`mpirun` or the equivalent provided by the MPI implementation). From this directory, a direct build that bypasses the current Makefile link rule is:

```sh
mpicc -Wall -O2 -o distributed_library main.c client_functions.c lib_functions.c list_functions.c mpi_datatypes.c
```

Run a scenario with the exact process count expected by the program. For example:

```sh
mpirun --oversubscribe -np 23 ./distributed_library 3 testfiles_hy486/testfile0/loaners_13_libs_9_np_23.txt
```

For the other included tests, use `-np 49` with `N=4` and testfile 1, or `-np 88` with `N=5` and testfile 2. `--oversubscribe` is useful on a local machine when the requested MPI ranks exceed the available CPU slots; some MPI implementations do not support that option, in which case remove it or use the implementation's equivalent.

The Makefile provides `make`, `make clean`, and `make run-N` style targets, and defaults its input to testfile 2. However, the current link recipe is `$(CC) $(CFLAGS) -o -O0 $@ $^`; because `-o` expects the next token to be the output filename, this appears malformed and can prevent the target from linking. The direct `mpicc` command above is a working form of the intended build command. The `run-%` target also computes the process count from `N`, but only works once the executable has built successfully and the required MPI tools (and `bc`) are available.

## MPI design details

The protocol uses explicit tags defined in `decl.h` to distinguish connection setup, election messages, book lookup and transfer, donation acknowledgements, popular-book aggregation, loan-count aggregation, and termination. Workers use `MPI_Probe` to inspect the next incoming message and dispatch based on its tag. Most operations use blocking `MPI_Send`/`MPI_Recv` request-and-reply sequences, so both the coordinator's command order and the workers' matching receive paths are important.

The `book` MPI datatype transmits `book_id`, `cost`, and `copies`; it does not transmit `from_lib`. On donation receipt, the receiving library sets `from_lib` locally. The popular-book datatype nests the book datatype with a `times_loaned` integer. These derived types avoid assuming that C struct padding is identical to a packed sequence of primitive values.

The library and client loan totals are accumulated independently: libraries increment their count when a copy is fulfilled, while clients increment theirs only when a successful book response arrives. Comparing the two totals is intended to expose protocol/accounting inconsistencies.

## Current scope and caveats

- The scenario parser expects recognized commands and does not gracefully skip arbitrary comments or blank lines.
- The program assumes a valid positive `N`, a sufficiently large MPI world, and client IDs and book IDs in the ranges implied by that `N`.
- Client graph operations assume the supplied connections form one connected graph; disconnected components can leave leader-dependent work incomplete.
- Prices and initial book prices are generated at runtime, so displayed prices can differ between runs.
- MPI execution behavior can vary by implementation and platform. The provided examples use the common `mpicc`/`mpirun` interface; a cluster may require scheduler-specific launch commands.
- The Makefile's link rule currently appears incorrect; see the build section before relying on its `make` targets.
