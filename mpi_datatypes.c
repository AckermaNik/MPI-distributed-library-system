#include "decl.h"

void create_mpi_book_type(MPI_Datatype *mpi_book) {
    book b;

    int block_lengths[3] = {1, 1, 1};
    MPI_Aint displacements[3]; //fields offsets
    MPI_Datatype types[3] = {MPI_INT, MPI_FLOAT, MPI_INT};

    MPI_Aint base_address;
    MPI_Get_address(&b, &base_address);                      // Address of the whole struct
    MPI_Get_address(&b.book_id, &displacements[0]);         // Address of book_id
    MPI_Get_address(&b.cost, &displacements[1]);            // Address of cost
    MPI_Get_address(&b.copies, &displacements[2]);          // Address of copies

    // Calculating each field's offset from struct's address
    for (int i = 0; i < 3; i++) {
        displacements[i] -= base_address;
    }

    MPI_Type_create_struct(3, block_lengths, displacements, types, mpi_book);
    MPI_Type_commit(mpi_book);
}

void create_mpi_most_popular_book_type(MPI_Datatype *type, MPI_Datatype book_type) {
    int block_lengths[2] = {1, 1};
    MPI_Datatype types[2] = {book_type, MPI_INT};
    MPI_Aint displacements[2]; //relative address of each struct's filed from the struct's starting address

    most_popular_book mp;
    MPI_Aint base;
    MPI_Get_address(&mp, &base);
    MPI_Get_address(&mp.book,         &displacements[0]);
    MPI_Get_address(&mp.times_loaned, &displacements[1]);

    for (int i = 0; i < 2; i++) displacements[i] -= base;

    MPI_Type_create_struct(2, block_lengths, displacements, types, type);
    MPI_Type_commit(type);
}