#include <mpi.h>
#include "structs.h"
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <assert.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#define NO_NEIGHBOR -50
#define CONNECT_TAG 100
#define NEIGHBOR_TAG 101
#define ACK_TAG 102
#define DONE_TAG 103
#define START_LEADER_ELECTION 104
#define LE_LIBR_DONE 105
#define LEADER 106
#define PARENT 107
#define NO_PARENT -100
#define REMOVED -2
#define ALREADY 108
#define START_LE_LOANERS 109
#define LE_LOANERS_DONE 110
#define NO_CHILD -99
#define ELECT  111
#define NO_COPIES -3
#define RIGHT_LIB 112
#define CAN_BORROW 113
#define LEND_BOOK 114
#define GET_BOOK 115
#define FIND_BOOK 116
#define BOOK_REQUEST 117
#define ACK_TB 118
#define DONE_FIND_BOOK 119
#define SEARCH_DONATED 120
#define DONATE_BOOK 121
#define CAN_DONATE 122
#define ACK_DB 123
#define GET_MOST_POPULAR_BOOK 124
#define GET_MOST_POPULAR_BOOK_DONE 125
#define GET_POPULAR_BK_INFO 126
#define SEARCH_RENTED 127
#define CHECK_NUM_BOOKS_LOANED 128
#define NUM_BOOKS_LOANED 129
#define CHECK_NUM_BOOKS_LOAN_DONE 130

#define CLIENT_FIRST_ID(N, LID) ((N)*(N) + (LID*N)/2)
#define CLIENT_LAST_ID(N, LID) ((N)*(N) + ((LID+1)*N)/2)
#define BOOK_FIRST_ID(N, LID) ((LID)*(N))
#define BOOK_LAST_ID(N, LID) ((LID+1)*(N)-1)
#define MAX_BOOK_ID(N) ((N)*(N)*(N) - 1)


Lib* initialize_lib(int rank,int N);
Client* initialize_client(int rank, int N);
bool list_insert(book new_book,void * list,bool for_lib);
bool init_list(void * list,bool for_lib);
book take_book(void * list,int b_id,bool for_lib);
bool add_neighbor(Client* cl, int neighbor,int N);
bool explore(int unexplored_ranks[4],int parent,int leader,int my_rank);
void create_mpi_book_type(MPI_Datatype *mpi_book);
void create_mpi_most_popular_book_type(MPI_Datatype *type, MPI_Datatype book_type); 
void make_donation(int num_libs,book to_donate,int N);
bool search_for_book(void * list, int b_id,bool for_lib);
most_popular_book take_my_fav_book(client_book_list * list);