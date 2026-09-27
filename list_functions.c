#include "decl.h"

#define INVALID_BOOK -1

bool list_insert(book new_book,void * list, bool for_lib){

    if(for_lib){

        struct lib_book_node* pred, *cur;

        lib_book_list * list_ = (lib_book_list *)list;

        struct lib_book_node* new_node;

        pred=list_->head;
        cur=list_->head->next;

        while(cur->elem.book_id < new_book.book_id){
            pred=cur;
            cur=cur->next;
        }

        if(cur->elem.book_id == new_book.book_id){
            cur->elem.copies++; // add a copy
            return false; //already in the list
        }

        new_node=malloc(sizeof(struct lib_book_node));
        assert(new_node!=NULL);
        new_node->elem=new_book;
        new_node->here=true;
        new_node->next=cur;
        pred->next=new_node;

        return true; 

    }else{

        struct client_book_node* pred, *cur;

        client_book_list * list_ = (client_book_list*)list;

        struct client_book_node* new_node;

        pred=list_->head;
        cur=list_->head->next;

        while(cur->elem.book_id < new_book.book_id){
            pred=cur;
            cur=cur->next;
        }

        if(cur->elem.book_id == new_book.book_id){
            cur->rent_times++;
            return false; //already in the list
        }

        //first time rented
        new_node=malloc(sizeof(struct client_book_node));
        assert(new_node!=NULL);
        new_node->elem=new_book;
        new_node->rent_times=1;
        new_node->next=cur;
        pred->next=new_node;

        return true; //in Client who calls this if it returns true num_books++

    }

}

bool init_list(void * list,bool for_lib){
    
    book book_;

    if(for_lib){

        lib_book_list * list_ = (lib_book_list *)list;

        book_.book_id=INVALID_BOOK;
        list_->head= malloc (sizeof(struct lib_book_node));

        assert(list_->head!=NULL);

        list_->head->elem=book_;
        list_->tail= malloc (sizeof(struct lib_book_node));

        assert(list_->tail!=NULL);

        book_.book_id= INT_MAX;
        list_->tail->elem=book_;

        list_->head->next=list_->tail;
        list_->tail->next=NULL;

        return true;

    }else{

        client_book_list * list_ = (client_book_list *)list;

        book_.book_id=INVALID_BOOK;
        list_->head= malloc (sizeof(struct client_book_node));

        assert(list_->head!=NULL);

        list_->head->elem=book_;
        list_->tail= malloc (sizeof(struct client_book_node));

        assert(list_->tail!=NULL);

        book_.book_id= INT_MAX;
        list_->tail->elem=book_;

        list_->head->next=list_->tail;
        list_->tail->next=NULL;

        return true;
    }

}
bool search_for_book(void * list, int b_id,bool for_lib){
    
    if(for_lib){

        lib_book_list * list_ = (lib_book_list *)list;

        struct lib_book_node *cur;

        cur=list_->head->next;

    
        while(cur->elem.book_id < b_id){
            cur=cur->next;
        }

        if(cur->elem.book_id == b_id && cur->here==true){
            return true;
        }

        return false;

    }
}

book take_book(void * list, int b_id,bool for_lib){

    book no_book;
    
    if(for_lib){

        lib_book_list * list_ = (lib_book_list *)list;

        struct lib_book_node *cur;

        cur=list_->head->next;

    
        while(cur->elem.book_id < b_id){
            cur=cur->next;
        }

        if(cur->elem.book_id == b_id && cur->here==true){

            no_book=cur->elem;

            cur->elem.copies--;
            //printf("MINUS 1 FOR BOOK: %d with copies:%d \n",b_id,no_book.copies);
            if( cur->elem.copies == 0 ){
                //list_->num_books--;
                cur->here=false;
            }
            
        }else{
            no_book.book_id=b_id;
            no_book.copies=NO_COPIES;
        }

        return no_book;

    }

}