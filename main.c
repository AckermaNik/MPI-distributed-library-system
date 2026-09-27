#include "decl.h"

int main(int argc, char *argv[]) {

    if (argc < 3) {
        fprintf(stderr, "Usage: %s <N> <inputfile>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    MPI_Init(&argc, &argv);
    srand(time(NULL));
    
    int N,num_libs,num_clients,rank,i,total_proc;
    int lib_leader_id=-1,lib_leader_rank;
    int client_leader_id=-1,client_leader_rank=-1;

    MPI_Datatype MPI_BOOK;
    MPI_Datatype MPI_MOST_POPULAR_BOOK;

    create_mpi_book_type(&MPI_BOOK);
    create_mpi_most_popular_book_type(&MPI_MOST_POPULAR_BOOK,MPI_BOOK);

    N = atoi(argv[1]);
    char* input= argv[2];

    num_libs = N * N;
    num_clients = (N * N * N) / 2;
    total_proc= num_libs + num_clients;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0) { //only reads input and sends messages 
        int c1, c2 ,b2,copies;

        FILE *fp = fopen(input, "r");
        if (!fp) {
            fprintf(stderr, "Failed to open file\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        
        char line[56];
        while (fgets(line, sizeof(line), fp)) {
            
            if (sscanf(line, "CONNECT %d %d", &c1, &c2) == 2) {
        
                MPI_Send(&c2, 1, MPI_INT, c1 + 1, CONNECT_TAG, MPI_COMM_WORLD);

                // Wait for ACK from c1
                MPI_Recv(NULL, 0, MPI_BYTE, c1 + 1, ACK_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            }else if(strncmp(line, "START_LE_LIBR", strlen("START_LE_LIBR")) == 0){ 
                
                printf("------  Connecting users done  ------\n");

                for ( i = 1; i <= num_libs; i++) {
                    MPI_Send(NULL, 0, MPI_BYTE, i, START_LEADER_ELECTION, MPI_COMM_WORLD);
                }

                MPI_Recv(&lib_leader_id, 1, MPI_INT, MPI_ANY_SOURCE, LE_LIBR_DONE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                lib_leader_rank=lib_leader_id+1;
                printf("------  LIB LEADER ID: %d DONE ------\n",lib_leader_rank - 1);
                
            }else if(strncmp(line, "START_LE_LOANERS", strlen("START_LE_LOANERS")) == 0){
                
                for ( i = num_libs+1; i <= total_proc; i++) {
                    MPI_Send(NULL, 0, MPI_BYTE, i, START_LE_LOANERS, MPI_COMM_WORLD);
                }

                MPI_Recv(&client_leader_id, 1, MPI_INT,MPI_ANY_SOURCE, LE_LOANERS_DONE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                client_leader_rank= client_leader_id + 1;
                printf("------  CLIENT LEADER ID: %d DONE ------\n",client_leader_rank - 1);
                
            }else if (sscanf(line, "TAKE_BOOK %d %d", &c1, &b2) == 2){
                MPI_Send(&b2, 1, MPI_INT, c1 + 1, CAN_BORROW, MPI_COMM_WORLD);
                MPI_Recv(NULL,0, MPI_BYTE, c1 + 1, DONE_FIND_BOOK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                //printf("------  Take book: %d done  ------\n",b2);

            }else if(sscanf(line, "DONATE_BOOK %d %d %d", &c1, &b2, &copies) == 3){

                book to_donate;

                to_donate.book_id=b2;
                to_donate.copies=copies;

                MPI_Send(&to_donate, 1, MPI_BOOK, c1+1 , CAN_DONATE , MPI_COMM_WORLD);

                MPI_Recv(NULL,0, MPI_BYTE, client_leader_rank, ACK_DB, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                printf("------  DONATE_BOOK: %d DONE  ------\n",b2);

            }else if(strncmp(line, "GET_MOST_POPULAR_BOOK", strlen("GET_MOST_POPULAR_BOOK")) == 0){
                MPI_Send(NULL, 0, MPI_BYTE, client_leader_rank, GET_MOST_POPULAR_BOOK, MPI_COMM_WORLD);

                MPI_Recv(NULL,0, MPI_BYTE, client_leader_rank, GET_MOST_POPULAR_BOOK_DONE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                printf("------ GET_MOST_POPULAR_BOOK   DONE  ------\n");

            }else if(strncmp(line, "CHECK_NUM_BOOKS_LOANED", strlen("CHECK_NUM_BOOKS_LOANED")) == 0){

                int from_libs,from_loaners;
                MPI_Send(NULL, 0, MPI_BYTE, lib_leader_rank, CHECK_NUM_BOOKS_LOANED, MPI_COMM_WORLD);
                MPI_Send(NULL, 0, MPI_BYTE, client_leader_rank, CHECK_NUM_BOOKS_LOANED, MPI_COMM_WORLD);

                MPI_Recv(&from_libs,1, MPI_INT, lib_leader_rank, CHECK_NUM_BOOKS_LOAN_DONE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                MPI_Recv(&from_loaners,1, MPI_INT, client_leader_rank, CHECK_NUM_BOOKS_LOAN_DONE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                if(from_libs==from_loaners){
                    printf("------  CHECK_NUM_BOOKS_LOANED   SUCCESS  ------\n");
                }else{
                    printf("------  CHECK_NUM_BOOKS_LOANED   FAILED  ------\n");
                }

            }else{ 
                break; 
            }
        }

        for (int all_rank = 1; all_rank <= total_proc ; all_rank++) {
            MPI_Send(NULL, 0, MPI_BYTE, all_rank, DONE_TAG, MPI_COMM_WORLD);
        }
        
        fclose(fp);


    } else if (rank >= 1 && rank <= num_libs)  {

        Lib* my_lib; 
        my_lib=initialize_lib(rank,N);

        bool if_leader=false;

        lib_leader_id=my_lib->lib_id;
        int unexplored_ranks[4],neighbor_ranks[4],*children,num_children=0,ch_size=N;
        int j=0,i;
        int parent=NO_PARENT,potential_leader_id; //rank
        int src;
        int tag;
        

        children=malloc(N*sizeof(int));

        //unexplored nodes= all my neighbors
        if(my_lib->right_n.x!=NO_NEIGHBOR){
            unexplored_ranks[j]=N*my_lib->right_n.x + my_lib->right_n.y+1;
        }else{
            unexplored_ranks[j]=REMOVED; //mini hack
        }
        neighbor_ranks[j]= unexplored_ranks[j];
        j++;

        if(my_lib->left_n.x!=NO_NEIGHBOR){
            unexplored_ranks[j]=N*my_lib->left_n.x + my_lib->left_n.y+1;
        }else{
            unexplored_ranks[j]=REMOVED; //mini hack
        }
        neighbor_ranks[j]= unexplored_ranks[j];
        j++;

        if(my_lib->down_n.x!= NO_NEIGHBOR){
            unexplored_ranks[j]=N*my_lib->down_n.x + my_lib->down_n.y + 1;
        }else{
            unexplored_ranks[j]=REMOVED; //mini hack
        }
        neighbor_ranks[j]= unexplored_ranks[j];
        j++;

        if(my_lib->up_n.x!= NO_NEIGHBOR){
            unexplored_ranks[j]=N*my_lib->up_n.x + my_lib->up_n.y +1;
        }else{
            unexplored_ranks[j]=REMOVED; //mini hack
        }
        neighbor_ranks[j]= unexplored_ranks[j];
        j++;

        while(1){
            MPI_Status status;
            MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status); //peek incoming mes

            src = status.MPI_SOURCE;
            tag = status.MPI_TAG;

            if(tag==DONE_TAG){
            
                break;

            }else if(tag==START_LEADER_ELECTION){

                MPI_Recv(NULL, 0, MPI_BYTE, 0, START_LEADER_ELECTION, MPI_COMM_WORLD, &status);

                if (parent==NO_PARENT){
                    lib_leader_id=my_lib->lib_id;
                    parent=lib_leader_id+1;
                    explore(unexplored_ranks,parent,lib_leader_id,lib_leader_id+1);
                }

            }else if(tag==LEADER){

                MPI_Recv(&potential_leader_id, 1, MPI_INT, src, LEADER, MPI_COMM_WORLD, &status);

                if(potential_leader_id>lib_leader_id){ // attach to new subtree
                    lib_leader_id=potential_leader_id;
                    parent=src;

                    //reset children
                    for(i=0;i<num_children;i++){
                        children[i]=NO_CHILD;
                    }

                    num_children=0;

                    //reset my unexplored neighbors
                    for(i=0;i<4;i++){
                        unexplored_ranks[i]=neighbor_ranks[i];
                    }
                    for(i=0;i<4;i++){
                        if(unexplored_ranks[i]==parent) unexplored_ranks[i]= REMOVED;
                    }
                    
                    explore(unexplored_ranks,parent,lib_leader_id,my_lib->lib_id+1);
                    

                }else if(potential_leader_id==lib_leader_id){
                    MPI_Send(&potential_leader_id, 1, MPI_INT, src, ALREADY, MPI_COMM_WORLD);
                }
            
            }else if(tag==ALREADY){ //since a neighbor of mine already has the same leader as me, just explore  

                MPI_Recv(&potential_leader_id, 1, MPI_INT, src, ALREADY, MPI_COMM_WORLD, &status); 
                if(lib_leader_id==potential_leader_id) {
                    if_leader=explore(unexplored_ranks,parent,lib_leader_id,my_lib->lib_id+1);
                    if(if_leader){
                        lib_leader_id=my_lib->lib_id;
                        lib_leader_rank=my_lib->rank;
                    }
                }

            }else if(tag==PARENT){

                MPI_Recv(&potential_leader_id, 1, MPI_INT, src, PARENT, MPI_COMM_WORLD, &status);
                //printf("I %d am parent of %d because of %d  \n",my_lib->rank,src,potential_leader_id);

                int flag=0;
                for(i=0;i<num_children;i++){
                        if(src==children[i]){
                            // printf("********111");
                            flag=1;
                            break;
                        }
                }

                if(potential_leader_id==lib_leader_id && flag==0){

                    num_children++;
                    if(ch_size < num_children){

                        int *temp = realloc(children, (ch_size + N) * sizeof(int));
                        if (temp != NULL) {
                            children = temp;
                            ch_size+=N;
                        } else {
                            perror("realloc failed");
                            exit(1);
                        }
                    }
                    //add 
                    children[num_children-1]=src;
                    if_leader=explore(unexplored_ranks,parent,lib_leader_id,my_lib->lib_id+1);
                    if(if_leader){
                        lib_leader_id=my_lib->lib_id;
                        lib_leader_rank=my_lib->rank;
                    }
                }
            }else if(tag==LE_LIBR_DONE){

                MPI_Recv(&lib_leader_id, 1, MPI_INT, src, LE_LIBR_DONE, MPI_COMM_WORLD, &status);
                lib_leader_rank=lib_leader_id+1;

            }else if(tag==LEND_BOOK){ // IM MY CLIENTS LIB
                int req_book;
                book to_be_sent_book;
                MPI_Recv(&req_book, 1, MPI_INT, src, LEND_BOOK , MPI_COMM_WORLD, &status);
                
                to_be_sent_book.copies=NO_COPIES; // IF THERE IS NOWHERE THE BOOK
                to_be_sent_book.book_id=req_book;

                if(req_book>=BOOK_FIRST_ID(N,my_lib->lib_id) && req_book<=BOOK_LAST_ID(N,my_lib->lib_id)){

                    //printf("I HAVE IT! lib: %d book:%d \n ",my_lib->lib_id,req_book);
                    to_be_sent_book=take_book(my_lib->my_books,req_book,true);


                }else if(search_for_book(my_lib->donated_books,req_book,true)){

                    //printf("In original lib donated from: %d\n",my_lib->lib_id);
                    to_be_sent_book=take_book(my_lib->donated_books,req_book,true);

                }
                
                if(to_be_sent_book.copies==NO_COPIES){

                    //As a lib I dont have it

                    int proper_lib,yes_no;

                    //FIND the lib id that has the book
                    //if Im not the leader notify to him to do it
                    if(lib_leader_rank!=my_lib->lib_id + 1){

                        MPI_Send(&req_book, 1, MPI_INT, lib_leader_rank, FIND_BOOK, MPI_COMM_WORLD);
                        MPI_Recv(&proper_lib, 1, MPI_INT, lib_leader_rank, RIGHT_LIB , MPI_COMM_WORLD, &status);

                    }else{ 

                        //otherwise Im the leader and I just search for the proper lib

                        proper_lib=NO_COPIES;   // if no lib has it

                        // FIRST SEE IF ANY LIB HAS IT IN ITS DONATED BOOKS
                        for(i=0; i < num_libs ;i++){
                            
                            if(i==src-1 || i==my_lib->lib_id) continue;

                            //printf("case 2 donate search i: %d\n",i);
                            MPI_Send(&req_book, 1, MPI_INT, i+1, SEARCH_DONATED, MPI_COMM_WORLD);
                            MPI_Recv(&yes_no, 1, MPI_INT, i+1, SEARCH_DONATED , MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                            if(yes_no==1){ // the lib has it but in its donated books
                                proper_lib=i+1;
                                break;
                            }
                        }

                        //SECOND SEE IF ANY LIB HAS IT IN ITS ORIGINAL BOOKS
                        if(proper_lib==NO_COPIES){      
                            
                            for(i=0; i < num_libs ;i++){
                                
                                if(i==src-1 || i==my_lib->lib_id) continue;

                                if(req_book>=BOOK_FIRST_ID(N,i) && req_book<=BOOK_LAST_ID(N,i)){
                                    //printf("case 1\n");
                                    proper_lib=i+1;
                                    break;
                                }
                            }
                        }
                    }

                    //printf("POTENTIAL RIGHT LIB %d\n",proper_lib-1);
                    if(proper_lib!=NO_COPIES){
                        MPI_Send(&req_book, 1, MPI_INT, proper_lib, BOOK_REQUEST, MPI_COMM_WORLD);
                        MPI_Recv(&to_be_sent_book, 1, MPI_BOOK, proper_lib, ACK_TB , MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                    }
                }else{
                    my_lib->num_of_books_rented++;
                }
                    
                MPI_Send(&to_be_sent_book, 1, MPI_BOOK, src, GET_BOOK, MPI_COMM_WORLD);

            }else if(tag==FIND_BOOK){ // only for leader to ONLY find the id of the proper lib

                int req_book,i,proper_lib;
                int yes_no;
                MPI_Recv(&req_book, 1, MPI_INT, src, FIND_BOOK , MPI_COMM_WORLD, &status);


                proper_lib=NO_COPIES;   // if no lib has it


                // FIRST SEE IF ANY LIB HAS IT IN ITS DONATED BOOKS
                for(i=0; i < num_libs ;i++){
                    
                    if(i==src-1) continue;

                    if(i!=my_lib->lib_id){
                        //printf("case 2 donate search i: %d\n",i);
                        MPI_Send(&req_book, 1, MPI_INT, i+1, SEARCH_DONATED, MPI_COMM_WORLD);
                        MPI_Recv(&yes_no, 1, MPI_INT, i+1, SEARCH_DONATED , MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                        if(yes_no==1){ // the lib has it but in its donated books
                            proper_lib=i+1;
                            break;
                        }
                    }else{
                        //printf("case 3 leader donate search i: %d\n",i);

                        if(search_for_book(my_lib->donated_books,req_book,true)){
                            proper_lib=i+1;
                            break;
                        }
                    }
                }

                //SECOND SEE IF ANY LIB HAS IT IN ITS ORIGINAL BOOKS
                if(proper_lib==NO_COPIES){      
                    
                    for(i=0; i < num_libs ;i++){
                        
                        if(i==src-1) continue;

                        if(req_book>=BOOK_FIRST_ID(N,i) && req_book<=BOOK_LAST_ID(N,i)){
                            //printf("case 1\n");
                            proper_lib=i+1;
                            break;
                        }
                    }
                }

                MPI_Send(&proper_lib, 1, MPI_INT, src, RIGHT_LIB, MPI_COMM_WORLD);

            }else if(tag==SEARCH_DONATED){
                int req_book;
                int yes_no=0;

                MPI_Recv(&req_book, 1, MPI_INT, src, SEARCH_DONATED , MPI_COMM_WORLD, &status);

                if(search_for_book(my_lib->donated_books,req_book,true)){ // i found it in my donate books
                    yes_no=1;
                }

                MPI_Send(&yes_no, 1, MPI_INT, src, SEARCH_DONATED, MPI_COMM_WORLD);

            }else if(tag==BOOK_REQUEST){ // I have the book for sure - take the actual book
                book to_be_sent_book;
                int req_book;

                MPI_Recv(&req_book, 1, MPI_INT, src, BOOK_REQUEST , MPI_COMM_WORLD, &status);
                
                if(req_book>=BOOK_FIRST_ID(N,my_lib->lib_id) && req_book<=BOOK_LAST_ID(N,my_lib->lib_id)){

                    to_be_sent_book=take_book(my_lib->my_books,req_book,true);

                }else if(search_for_book(my_lib->donated_books,req_book,true)){

                    to_be_sent_book=take_book(my_lib->donated_books,req_book,true);
                    //printf("\t %d found donated %d of copies: %d\n",my_lib->lib_id,to_be_sent_book.book_id,to_be_sent_book.copies);

                }

                if(to_be_sent_book.copies!=NO_COPIES){ my_lib->num_of_books_rented++;}

                MPI_Send(&to_be_sent_book,1, MPI_BOOK, src, ACK_TB, MPI_COMM_WORLD);

            }else if(tag==DONATE_BOOK){
                book donated_book;

                MPI_Recv(&donated_book, 1, MPI_BOOK, src, DONATE_BOOK , MPI_COMM_WORLD, &status);

                donated_book.from_lib=my_lib->lib_id;
                list_insert(donated_book,my_lib->donated_books,true);

                MPI_Send(NULL,0, MPI_BYTE, src, ACK_DB, MPI_COMM_WORLD);

            }else if(tag==CHECK_NUM_BOOKS_LOANED){

                MPI_Recv(NULL, 0, MPI_BYTE, src, CHECK_NUM_BOOKS_LOANED , MPI_COMM_WORLD, &status);

                int total_rented_books=0,i;
                int libs_rented=0;

                if(my_lib->lib_id==lib_leader_id){ // FOR LEADER
                    MPI_Send(NULL,0, MPI_BYTE, 1, CHECK_NUM_BOOKS_LOANED, MPI_COMM_WORLD); // send to lib 0

                    total_rented_books+=my_lib->num_of_books_rented;

                    for(i=0; i < num_libs ;i++){

                        if(i==my_lib->lib_id) continue;

                        MPI_Recv(&libs_rented, 1, MPI_INT, i+1, NUM_BOOKS_LOANED , MPI_COMM_WORLD, &status);
                        total_rented_books+=libs_rented;
                    }

                    printf("------  BOOKS LENT BY ALL LIBS: %d  ------\n",total_rented_books);
                    MPI_Send(&total_rented_books, 1 , MPI_INT, 0, CHECK_NUM_BOOKS_LOAN_DONE, MPI_COMM_WORLD);

                }else if(my_lib->x % 2 == 0 && my_lib->y == N-1){
                    int to = neighbor_ranks[up];

                    MPI_Send(&my_lib->num_of_books_rented, 1 , MPI_INT, lib_leader_rank, NUM_BOOKS_LOANED, MPI_COMM_WORLD);
                    
                    if(to!=lib_leader_rank && to!=REMOVED ){

                        MPI_Send(NULL,0, MPI_BYTE, to, CHECK_NUM_BOOKS_LOANED, MPI_COMM_WORLD);

                    }else if(to==lib_leader_rank){

                        MPI_Send(NULL,0, MPI_BYTE, to - 1, CHECK_NUM_BOOKS_LOANED, MPI_COMM_WORLD);
                    }

                    
                }else if(my_lib->x % 2 == 0){

                    int to = neighbor_ranks[right];

                    MPI_Send(&my_lib->num_of_books_rented, 1 , MPI_INT, lib_leader_rank, NUM_BOOKS_LOANED, MPI_COMM_WORLD);
                    
                    if(to!=lib_leader_rank && to!=REMOVED){

                        MPI_Send(NULL,0, MPI_BYTE, to, CHECK_NUM_BOOKS_LOANED, MPI_COMM_WORLD);
                    }

                }else if(my_lib->x % 2 == 1 && my_lib->y == 0){

                    int to = neighbor_ranks[up];

                    MPI_Send(&my_lib->num_of_books_rented, 1 , MPI_INT, lib_leader_rank, NUM_BOOKS_LOANED, MPI_COMM_WORLD);

                    if(to!=lib_leader_rank && to!=REMOVED){

                        MPI_Send(NULL,0, MPI_BYTE, to, CHECK_NUM_BOOKS_LOANED, MPI_COMM_WORLD);

                    }else if(to==lib_leader_rank){

                        MPI_Send(NULL,0, MPI_BYTE, to - 1, CHECK_NUM_BOOKS_LOANED, MPI_COMM_WORLD);
                    }

                }else if(my_lib->x % 2 == 1){

                    int to = neighbor_ranks[left];

                    MPI_Send(&my_lib->num_of_books_rented, 1 , MPI_INT, lib_leader_rank, NUM_BOOKS_LOANED, MPI_COMM_WORLD);

                    if(to!=lib_leader_rank && to!=REMOVED){

                        MPI_Send(NULL,0, MPI_BYTE, to, CHECK_NUM_BOOKS_LOANED, MPI_COMM_WORLD);

                    }
                }

            }
        
        }
        
    }else{

        Client* my_client;
        my_client=initialize_client(rank,N);
        int n_id,first_time=0,sent=0,last_neighbor,remain_neighbors_num, *remain_neighbors; //neighbors id 
        int flag;
        int src;
        int tag;
        
        while (1) {

            MPI_Status status;
            MPI_Status temp_status;
            MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            
            src = status.MPI_SOURCE;
            tag = status.MPI_TAG;

            if (tag == DONE_TAG) {

                MPI_Recv(NULL, 0, MPI_BYTE, 0, DONE_TAG, MPI_COMM_WORLD, &status);


                struct client_book_node *cur;
                cur=my_client->rented_books->head->next;

                if(cur->next==NULL){
                    break;
                }

                printf("\n");
                while(cur->next!=NULL){
                    printf("Rented book of Client: %d  b_id: %d copies: %d \n",my_client->c_id,cur->elem.book_id,cur->rent_times);
                    cur=cur->next;
                }
                printf("\n\n");

                break;

            } else if (tag== CONNECT_TAG) {
                
                MPI_Recv(&n_id, 1, MPI_INT, 0, CONNECT_TAG, MPI_COMM_WORLD, &status);
                int already=add_neighbor(my_client,n_id,N);

                if(!already){
                    MPI_Send(&(my_client->c_id), 1, MPI_INT,n_id+1, NEIGHBOR_TAG, MPI_COMM_WORLD);
                    // Wait for ACK from c2
                    MPI_Recv(NULL, 0, MPI_BYTE, n_id+1, ACK_TAG, MPI_COMM_WORLD, &status);
                }
                
            
                //send ACK to coordinator
                MPI_Send(NULL, 0, MPI_BYTE,0, ACK_TAG, MPI_COMM_WORLD);

            } else if (tag == NEIGHBOR_TAG) {

                MPI_Recv(&n_id, 1, MPI_INT, src, NEIGHBOR_TAG, MPI_COMM_WORLD, &status);
                add_neighbor(my_client,n_id,N);
                MPI_Send(NULL, 0, MPI_BYTE,src, ACK_TAG, MPI_COMM_WORLD);

            }else if(tag==START_LE_LOANERS){

                MPI_Recv(NULL, 0, MPI_BYTE, 0, START_LE_LOANERS, MPI_COMM_WORLD, &status);

                if(my_client->num_of_neighbors==1){ // a leaf
                    MPI_Send(NULL, 0, MPI_BYTE,my_client->neighbors[0]+1, ELECT, MPI_COMM_WORLD);
                }
            }else if(tag==ELECT){

                MPI_Recv(NULL, 0, MPI_BYTE, src , ELECT, MPI_COMM_WORLD, &status);

                if(first_time==0){

                    first_time=1;

                    remain_neighbors_num=my_client->num_of_neighbors;
                    remain_neighbors=malloc(remain_neighbors_num*sizeof(int));
                    memcpy(remain_neighbors, my_client->neighbors, remain_neighbors_num * sizeof(int));

                }

                for(i=0; i< my_client->num_of_neighbors ; i++ ){
                    if(remain_neighbors[i]+1==src){
                        remain_neighbors[i]=REMOVED;
                        break;
                    }
                }

                remain_neighbors_num--;

                if(remain_neighbors_num==1){ //ONLY MY PARENT left

                        usleep(100000); // wait to see if you have final ELECT, if yes u are the leader
                        MPI_Iprobe(MPI_ANY_SOURCE, ELECT, MPI_COMM_WORLD, &flag, &temp_status);
                        if(!flag){
                            for(i=0; i< my_client->num_of_neighbors ; i++ ){
                                if(remain_neighbors[i]!=REMOVED){
                                    last_neighbor=remain_neighbors[i];
                                    MPI_Send(NULL, 0, MPI_BYTE,last_neighbor+1, ELECT, MPI_COMM_WORLD);
                                    sent=1;
                                    break;
                                }
                            }
                        }


                }else if(remain_neighbors_num==0){ // im the leader / got elect from all my neighbors, without me sending one 

                    if(sent==0){ //be aware of LE_LOANERS_DONE
                        int leader=my_client->c_id;
                        client_leader_id=my_client->c_id;
                        client_leader_rank=my_client->my_rank;
                        for(i=num_libs+1 ; i<=total_proc; i++){
                            if(i!=client_leader_rank){
                                MPI_Send(&leader, 1, MPI_INT, i, LE_LOANERS_DONE, MPI_COMM_WORLD);
                            }
                        }
                        MPI_Send(&leader, 1, MPI_INT, 0, LE_LOANERS_DONE, MPI_COMM_WORLD);

                    }else{
                        if(src<my_client->c_id+1){
                            int leader=my_client->c_id;
                            client_leader_id=my_client->c_id;
                            client_leader_rank=my_client->my_rank;
                            for(i=num_libs+1 ; i<=total_proc; i++){
                                if(i!=client_leader_rank){
                                    MPI_Send(&leader, 1, MPI_INT, i, LE_LOANERS_DONE, MPI_COMM_WORLD);
                                }
                            }
                            MPI_Send(&leader, 1, MPI_INT, 0, LE_LOANERS_DONE, MPI_COMM_WORLD);
                        }
                    }
                }

            }else if(tag==LE_LOANERS_DONE){
                MPI_Recv(&client_leader_id, 1, MPI_INT, src, LE_LOANERS_DONE, MPI_COMM_WORLD,  &status);
                client_leader_rank=client_leader_id+1;
                //printf("!Client leader from:%d %d\n",my_client->c_id+1,client_leader_id);


            }else if (tag==CAN_BORROW){
                int req_book;
                MPI_Recv(&req_book, 1, MPI_INT, 0, CAN_BORROW , MPI_COMM_WORLD, &status);

                int my_id=my_client->c_id;
                for(i=0;i<num_libs;i++){
                    if(my_id>=CLIENT_FIRST_ID(N,i) && my_id<CLIENT_LAST_ID(N,i)){
                        my_client->my_lib=i;
                        break;
                    }
                }

                //printf("Client: %d lib: %d\n",my_client->c_id, my_client->my_lib);
                MPI_Send(&req_book, 1, MPI_INT, my_client->my_lib + 1 , LEND_BOOK, MPI_COMM_WORLD);

            }else if(tag==GET_BOOK){
                book req_book;

                MPI_Recv(&req_book, 1, MPI_BOOK, my_client->my_lib+1, GET_BOOK , MPI_COMM_WORLD, &status);

                if(req_book.copies!=NO_COPIES){
                    my_client->num_rented_books++;
                    if(list_insert(req_book,my_client->rented_books,false)){
                        //printf("Book with id: %d was rented FIRST TIME by costumer: %d\n",req_book.book_id,my_client->c_id);
                    }else{
                        //printf("Book with id: %d was rented AGAIN by costumer: %d\n",req_book.book_id,my_client->c_id);
                    }
                }else{
                    printf("NO AVAILABLE COPIES OF BOOK: %d\n",req_book.book_id);
                }


                MPI_Send(NULL, 0, MPI_BYTE, 0 , DONE_FIND_BOOK, MPI_COMM_WORLD);

            }else if(tag==CAN_DONATE){
                book to_donate;

                MPI_Recv(&to_donate, 1, MPI_BOOK, 0, CAN_DONATE , MPI_COMM_WORLD, &status);

                if(my_client->my_rank!=client_leader_rank){
                    MPI_Send(&to_donate, 1, MPI_BOOK, client_leader_rank , DONATE_BOOK, MPI_COMM_WORLD);
                }else{  // IM ALREADY THE LEADER
                    
                    make_donation(num_libs,to_donate,N);
                
                    MPI_Send(NULL, 0, MPI_BYTE, 0 , ACK_DB, MPI_COMM_WORLD);
                }

            }else if(tag==DONATE_BOOK){ // only for the leader
                book to_donate;

                MPI_Recv(&to_donate, 1, MPI_BOOK, src, DONATE_BOOK , MPI_COMM_WORLD, &status);

                make_donation(num_libs,to_donate,N);
                
                MPI_Send(NULL, 0, MPI_BYTE, 0 , ACK_DB, MPI_COMM_WORLD);

            }else if(tag==GET_MOST_POPULAR_BOOK){ //only for leader

                MPI_Recv(NULL, 0, MPI_BYTE, 0, GET_MOST_POPULAR_BOOK , MPI_COMM_WORLD, &status);

                int capacity=my_client->num_of_neighbors+2,i;
                most_popular_book* populars = malloc(capacity * sizeof(most_popular_book));

                assert(populars!=NULL);

                for(i=0;i<my_client->num_of_neighbors;i++){
                    MPI_Send(NULL, 0, MPI_BYTE, my_client->neighbors[i] + 1 , SEARCH_RENTED, MPI_COMM_WORLD);
                }


                for(i=0;i<my_client->num_of_neighbors;i++){
                    MPI_Recv(&populars[i], 1, MPI_MOST_POPULAR_BOOK, my_client->neighbors[i] + 1, GET_POPULAR_BK_INFO , MPI_COMM_WORLD, MPI_STATUS_IGNORE); 
                }

                populars[i]=take_my_fav_book(my_client->rented_books);

                most_popular_book max_rented;
                max_rented.times_loaned=NO_COPIES;

                for(i=0;i<=my_client->num_of_neighbors;i++){

                    if(max_rented.times_loaned==NO_COPIES || max_rented.times_loaned<populars[i].times_loaned){
                        
                        max_rented=populars[i];

                    }else if (max_rented.times_loaned==populars[i].times_loaned){

                        if(populars[i].book.cost>max_rented.book.cost){
                            max_rented=populars[i];
                        }
                    }

                }
                printf("------ MOST POPULAR BOOK: %d COST: %.2f TIMES_RENTED: %d ------\n",max_rented.book.book_id,max_rented.book.cost,max_rented.times_loaned);
                MPI_Send(NULL, 0, MPI_BYTE, 0 , GET_MOST_POPULAR_BOOK_DONE, MPI_COMM_WORLD);

            }else if(tag==SEARCH_RENTED){

                MPI_Recv(NULL, 0, MPI_BYTE, src, SEARCH_RENTED , MPI_COMM_WORLD, &status);

                most_popular_book pop_book;

                if(my_client->num_of_neighbors==1){ //only my parent

                    pop_book=take_my_fav_book(my_client->rented_books);
                    MPI_Send(&pop_book, 1, MPI_MOST_POPULAR_BOOK, src , GET_POPULAR_BK_INFO, MPI_COMM_WORLD);
                    
                    //printf("------ MOST POPULAR BOOK FROM LEAF: %d COST: %f TIMES_RENTED: %d FROM cid: %d ------\n",pop_book.book.book_id,pop_book.book.cost,pop_book.times_loaned,my_client->c_id);
                }else{

                    most_popular_book temp;
                    temp.times_loaned=NO_COPIES;
                    int capacity=my_client->num_of_neighbors+2,i;
                    most_popular_book* populars = malloc(capacity * sizeof(most_popular_book));

                    assert(populars!=NULL);

                    for(i=0;i<my_client->num_of_neighbors;i++){
                        if(my_client->neighbors[i]==src-1) continue;
                        MPI_Send(NULL, 0, MPI_BYTE, my_client->neighbors[i] + 1 , SEARCH_RENTED, MPI_COMM_WORLD);
                    }

                    for(i=0;i<my_client->num_of_neighbors;i++){
                        if(my_client->neighbors[i]==src-1){
                            populars[i]=temp;
                            continue;
                        }
                        MPI_Recv(&populars[i], 1, MPI_MOST_POPULAR_BOOK, my_client->neighbors[i] + 1, GET_POPULAR_BK_INFO , MPI_COMM_WORLD, MPI_STATUS_IGNORE); 
                    }

                    populars[i]=take_my_fav_book(my_client->rented_books);

                    most_popular_book max_rented;
                    max_rented.times_loaned=NO_COPIES;

                    for(i=0;i<=my_client->num_of_neighbors;i++){

                        if(max_rented.times_loaned==NO_COPIES || max_rented.times_loaned<populars[i].times_loaned){
                            
                            max_rented=populars[i];

                        }else if (max_rented.times_loaned==populars[i].times_loaned){

                            if(populars[i].book.cost>max_rented.book.cost){
                                max_rented=populars[i];
                            }
                        }

                    }

                    //printf("------ MOST POPULAR BOOK: %d COST: %f TIMES_RENTED: %d ------\n",max_rented.book.book_id,max_rented.book.cost,max_rented.times_loaned);
                    MPI_Send(&max_rented, 1, MPI_MOST_POPULAR_BOOK, src , GET_POPULAR_BK_INFO, MPI_COMM_WORLD);

                }
            }else if(tag==CHECK_NUM_BOOKS_LOANED){ //for leader and others

                int i ,total_rented=0, received_count;

                MPI_Recv(NULL, 0, MPI_BYTE, src, CHECK_NUM_BOOKS_LOANED , MPI_COMM_WORLD, &status);


                if(my_client->num_of_neighbors==1){

                    total_rented+=my_client->num_rented_books;
                    MPI_Send(&total_rented, 1, MPI_INT, src , NUM_BOOKS_LOANED, MPI_COMM_WORLD);

                }else{

                    for(i=0;i<my_client->num_of_neighbors;i++){
                        if( my_client->neighbors[i] != src -1 ){
                            MPI_Send(NULL, 0, MPI_BYTE, my_client->neighbors[i] + 1 , CHECK_NUM_BOOKS_LOANED, MPI_COMM_WORLD);
                        }
                    }

                    for(i=0;i<my_client->num_of_neighbors;i++){
                        if(my_client->neighbors[i]==src-1){
                            continue;
                        }
                        MPI_Recv(&received_count, 1, MPI_INT, my_client->neighbors[i] + 1, NUM_BOOKS_LOANED , MPI_COMM_WORLD, MPI_STATUS_IGNORE); 

                        total_rented+=received_count;
                    }

                    total_rented+=my_client->num_rented_books;

                    if(my_client->c_id == client_leader_rank - 1){
                        printf("------  RENTED BOOKS BY ALL LOANERS: %d  ------\n",total_rented);
                        MPI_Send(&total_rented, 1, MPI_INT, 0 , CHECK_NUM_BOOKS_LOAN_DONE, MPI_COMM_WORLD);
                    }else{
                        MPI_Send(&total_rented, 1, MPI_INT, src , NUM_BOOKS_LOANED, MPI_COMM_WORLD);
                    }
                }
            }
        }
    }

    MPI_Finalize();
    return 0;
}






        // printf("Lib id: %d \n",my_lib->lib_id);
        // printf("Lib id:%d Lib loc: [%d %d] \n",my_lib->lib_id, my_lib->y, my_lib->x);
        // // printf("Lib id:%d  FIRST CLIENT: %d\n",my_lib->lib_id, my_lib->first_client_id);
        // // printf("Lib id:%d  LAST CLIENT: %d\n",my_lib->lib_id, my_lib->last_client_id);

        // printf("Lib id:%d  left_n: (%d,%d)\n",my_lib->lib_id,my_lib->left_n.y,my_lib->left_n.x);
        // printf("Lib id:%d  right_n: (%d,%d)\n",my_lib->lib_id,my_lib->right_n.y,my_lib->right_n.x);
        // printf("Lib id:%d  up_n: (%d,%d)\n",my_lib->lib_id,my_lib->up_n.y,my_lib->up_n.x);
        // printf("Lib id:%d  down_n: (%d,%d)\n",my_lib->lib_id,my_lib->down_n.y,my_lib->down_n.x);
        //printf("Lib id:%d  num of books: %d\n",my_lib->lib_id,my_lib->num_books);

         // struct lib_book_node *cur;
        // cur=my_lib->my_books->head;
        // while(cur->next!=NULL){
        //     printf("Lib id:%d  b_id: %d cost: %f copies: %d here: %d \n",my_lib->lib_id,cur->elem.book_id,cur->elem.cost,cur->elem.copies,cur->here);
        //     cur=cur->next;
        // }


            //to see if the connections of clients are correct
         // if(my_client->neighbors!=NULL){
                //     printf("C_ID:%d ",my_client->c_id); 
                //     for (i=0;i<my_client->num_of_neighbors;i++){
                //         printf("n_%d  ",my_client->neighbors[i]);
                //     }
                //     printf("\n");
                // }