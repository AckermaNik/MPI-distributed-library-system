#include "decl.h"


Client* initialize_client(int rank, int N){

    Client* cl = malloc(sizeof(Client)+1);

    if (!cl) {
        perror("malloc for client failed");
        exit(1);
    }

    cl->neighbors = NULL;

    cl->rented_books=malloc (sizeof(client_book_list));
    assert(cl->rented_books!=NULL);

    init_list(cl->rented_books,false);
    
    cl->num_of_neighbors=0;
    cl->num_rented_books=0;
    cl->size=0;
    cl->my_rank=rank;
    cl->c_id=rank-1;

    return cl;
}

bool add_neighbor(Client* cl, int neighbor,int N){
    int i;

    if(cl->num_of_neighbors>0){
        for(i=0;i< cl->num_of_neighbors;i++){
            if (neighbor==cl->neighbors[i]){
                return true;
            }
        }
    }

    cl->num_of_neighbors++;
    if(cl->size < cl->num_of_neighbors){

        int *temp = realloc(cl->neighbors, (cl->size + N) * sizeof(int));
        if (temp != NULL) {
            cl->neighbors = temp;
            cl->size+=N;
        } else {
            perror("realloc in neighbors failed");
            exit(1);
            }
    }

    //add 
    cl->neighbors[cl->num_of_neighbors-1]=neighbor;
    return false;
}

void make_donation(int num_libs,book to_donate,int N){
    MPI_Datatype MPI_BOOK;
    int total_copies,donate_next=0,to_lib,b_id;

    create_mpi_book_type(&MPI_BOOK);
    total_copies=to_donate.copies;
    b_id=to_donate.book_id;

    to_donate.cost=5 + rand() % 96;
    to_donate.copies=1;
    

    while(donate_next < total_copies){
        to_lib= donate_next % num_libs;

        if(b_id>=BOOK_FIRST_ID(N,to_lib) && b_id<=BOOK_LAST_ID(N,to_lib)){

            //printf("!!! THE BOOK %d TO BE DONATED ALREADY EXISTS IN LIB: %d  \n",to_donate.book_id,to_lib);
            total_copies++;
            donate_next++;
            continue;

        }

        MPI_Send(&to_donate, 1, MPI_BOOK, to_lib + 1 , DONATE_BOOK, MPI_COMM_WORLD);

        MPI_Recv(NULL,0,MPI_BYTE,to_lib + 1,ACK_DB,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        donate_next++;
    }
}

most_popular_book take_my_fav_book(client_book_list * list){
    most_popular_book max_rented;

    max_rented.times_loaned=NO_COPIES;

     struct client_book_node *cur;

        cur=list->head->next;

        while(cur->elem.book_id!=INT_MAX){

            if(max_rented.times_loaned==NO_COPIES || max_rented.times_loaned<cur->rent_times){

                        max_rented.book=cur->elem;
                        max_rented.times_loaned=cur->rent_times;

            }else if (max_rented.times_loaned==cur->rent_times){

                if(cur->elem.cost>max_rented.book.cost){
                    max_rented.book=cur->elem;
                }
            }

            cur=cur->next;
        }
    
        return max_rented;

}