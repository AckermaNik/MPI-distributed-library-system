
#include "decl.h"

Lib* initialize_lib(int rank,int N){

    int l_id,i;
    Lib* lib = malloc(sizeof(Lib));

    if (!lib) {
        perror("malloc for library failed");
        exit(1);
    }

    l_id = rank - 1;
    lib->rank=rank;
    lib->lib_id = l_id;
    lib->x = l_id / N;
    lib->y = l_id % N;

    lib->my_books = malloc (sizeof(lib_book_list));
    lib->my_books->num_books=N;
    lib->num_of_books_rented=0;
    assert(lib->my_books!=NULL);
    init_list(lib->my_books,true);

    lib->donated_books = malloc (sizeof(lib_book_list));
    assert(lib->donated_books!=NULL);
    init_list(lib->donated_books,true);

    for (i=l_id*N ; i<(l_id+1)*N ; i++){
        book new_b;
        new_b.cost=5 + rand() % 96;
        new_b.book_id=i;
        new_b.copies=N;
        new_b.from_lib=l_id;

        if(!list_insert(new_b,lib->my_books,true)) printf ("Book already in the Library with id: %d \n",l_id);
    }

    if(lib->x==0){
        if(lib->y==0){
            lib->up_n.x=1;
            lib->up_n.y=0;
            lib->right_n.x=0;
            lib->right_n.y=1;
            lib->down_n.x=NO_NEIGHBOR;
            lib->left_n.x=NO_NEIGHBOR;
            lib->num_of_neighbors=2;
        }else if (lib->y==N-1){
            lib->left_n.x=0;
            lib->left_n.y=N-2;
            lib->up_n.x=1;
            lib->up_n.y=N-1;
            lib->down_n.x=NO_NEIGHBOR;
            lib->right_n.x=NO_NEIGHBOR;
            lib->num_of_neighbors=2;
        }else{
            lib->left_n.x= 0;
            lib->left_n.y= lib->y - 1;
            lib->right_n.x= 0;
            lib->right_n.y= lib->y + 1;
            lib->up_n.x= 1;
            lib->up_n.y= lib->y;
            lib->down_n.x=NO_NEIGHBOR;
            lib->num_of_neighbors=3;
        }
    }else if (lib->x==N-1){
        if(lib->y==0){
            lib->right_n.x=N-1;
            lib->right_n.y=1;
            lib->down_n.x= N-2;
            lib->down_n.y=0;
            lib->up_n.x=NO_NEIGHBOR;
            lib->left_n.x=NO_NEIGHBOR;
            lib->num_of_neighbors=2;
        }else if (lib->y==N-1){
            lib->left_n.x=N-1;
            lib->left_n.y=N-2;
            lib->down_n.x=N-2;
            lib->down_n.y=N-1;
            lib->up_n.x=NO_NEIGHBOR;
            lib->right_n.x=NO_NEIGHBOR;
            lib->num_of_neighbors=2;
        }else{
            lib->left_n.x= N-1;
            lib->left_n.y= lib->y - 1;
            lib->right_n.x= N-1;
            lib->right_n.y= lib->y + 1;
            lib->down_n.x= N-2;
            lib->down_n.y= lib->y;
            lib->up_n.x=NO_NEIGHBOR;
            lib->num_of_neighbors=3;
        }
    }else if(lib->y==0){
        lib->down_n.x= lib->x - 1;
        lib->down_n.y= 0;
        lib->up_n.x= lib->x + 1;
        lib->up_n.y= 0;
        lib->right_n.x= lib->x;
        lib->right_n.y= 1;
        lib->left_n.x=NO_NEIGHBOR;
        lib->num_of_neighbors=3;

    }else if(lib->y==N-1){
        lib->down_n.x= lib->x - 1;
        lib->down_n.y= N-1;
        lib->up_n.x= lib->x + 1;
        lib->up_n.y= N-1;
        lib->left_n.x= lib->x;
        lib->left_n.y= N-2;
        lib->right_n.x=NO_NEIGHBOR;
        lib->num_of_neighbors=3;
    }else{
        lib->down_n.x= lib->x - 1;
        lib->down_n.y= lib->y;
        lib->up_n.x= lib->x + 1;
        lib->up_n.y= lib->y;
        lib->left_n.x= lib->x;
        lib->left_n.y= lib->y-1;
        lib->right_n.x= lib->x;
        lib->right_n.y= lib->y+1;
        lib->num_of_neighbors=4;
    }

    return lib;
    
}


bool explore(int unexplored_ranks[4],int parent,int leader,int my_rank){

    int next_pr=-1,i;


    for(i=0;i<4;i++){
        if(unexplored_ranks[i]!=REMOVED) {
            next_pr=unexplored_ranks[i];
            unexplored_ranks[i]=REMOVED;
            break;
        }
    }
    
    if(next_pr!=-1){
        MPI_Send(&leader, 1, MPI_INT, next_pr, LEADER, MPI_COMM_WORLD);
    }else{
        if(parent!=NO_PARENT){
            if(parent!=my_rank){
                MPI_Send(&leader, 1, MPI_INT, parent, PARENT, MPI_COMM_WORLD);
            }else {   //otherwise I am the root / leader
                for(i=0;i<my_rank;i++){ // to coordinator and all other libs
                    MPI_Send(&leader, 1, MPI_INT, i, LE_LIBR_DONE, MPI_COMM_WORLD);
                }
                return true;
            }
        }
    } 

    return false;
}

