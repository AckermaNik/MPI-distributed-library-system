typedef enum {
    false = 0,
    true = 1
} bool;

typedef enum {
    right = 0,
    left = 1,
    down = 2,
    up = 3
} direction;

typedef struct {
    int book_id; 
    float cost;
    int copies;
    int from_lib; //the Id
}book;

struct lib_book_node{
    book elem;
    struct lib_book_node* next;
    bool   here; //if I actually have the book now 
};

typedef struct {
    struct lib_book_node* head; //centinel nodes
    struct lib_book_node* tail;
    int num_books;
}lib_book_list;

struct neighbor{

    int x,y; //(y,x)
};

typedef struct {

    int x , y; //(y,x)
    int  lib_id;           
    int rank;
    
    lib_book_list *my_books;  
    lib_book_list *donated_books;      
    
    
    struct neighbor right_n; //all neighbors
    struct neighbor left_n;
    struct neighbor up_n;
    struct neighbor down_n;

    int num_of_neighbors;

    int num_of_books_rented;

    // each lib has N^2 clients
    
} Lib;

struct client_book_node{
    book elem;
    struct client_book_node* next;
    int rent_times;
};

typedef struct {
    struct client_book_node* head; //centinel nodes
    struct client_book_node* tail;
}client_book_list;


typedef struct {
    int c_id,my_rank; 
    int my_lib;              
    
    client_book_list *rented_books;
    int num_rented_books;  

    int num_of_neighbors;
    int* neighbors;
    int size;

} Client;


typedef struct{

    book book;
    int times_loaned;

}most_popular_book;