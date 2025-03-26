#include <stdio.h>
#include <string.h>
#include "database.h"
#include "../btree/btree.h"
#include "../structs.h"

// Global file pointers
FILE *metadata_fp, *data_fp;

// Extern declaration for header from btree.c
extern Header header;

/**
 * Initialize the database files.
 */
void init_database() {
    // Metadata file
    metadata_fp = fopen("metadata.dat", "r+");
    if (!metadata_fp) {
        metadata_fp = fopen("metadata.dat", "w+");
        fprintf(metadata_fp, "table:users\ncolumn:id:int:primary_key\ncolumn:name:string:%d\n", NAME_LEN);
    }
    fclose(metadata_fp);

    // Data file
    data_fp = fopen("data.dat", "r+b");
    if (!data_fp) {
        data_fp = fopen("data.dat", "w+b");
    }
    fclose(data_fp);

    // B+ tree
    init_btree();
}

/**
 * Append a row to the data file and return its offset.
 * @param id Row ID
 * @param name Row name
 * @return Offset where the row is written
 */
long append_row_to_file(int id, char* name) {
    data_fp = fopen("data.dat", "r+b");
    fseek(data_fp, 0, SEEK_END);
    long offset = ftell(data_fp);

    Row row;
    row.id = id;
    strncpy(row.name, name, NAME_LEN-1);
    row.name[NAME_LEN-1] = '\0'; // Ensure null-terminated
    fwrite(&row, sizeof(Row), 1, data_fp);
    fflush(data_fp);
    fclose(data_fp);
    return offset;
}

/**
 * Insert a row into the database.
 * @param id Row ID
 * @param name Row name
 */
void insert_row(int id, char* name) {
    long existing_offset = search(id, header.root_id);
    if (existing_offset != -1) {
        printf("Error: Duplicate ID %d\n", id);
        return;
    }
    long offset = append_row_to_file(id, name);
    btree_insert(id, offset);
    printf("Inserted: id=%d, name=%s at offset=%ld\n", id, name, offset);
}

/**
 * Select and display a row by ID.
 * @param id Row ID to find
 */
void select_row(int id) {
    long offset = search(id, header.root_id);
    if (offset == -1) {
        printf("ID %d not found\n", id);
        return;
    }
    data_fp = fopen("data.dat", "rb");
    fseek(data_fp, offset, SEEK_SET);
    Row row;
    fread(&row, sizeof(Row), 1, data_fp);
    printf("Found: id=%d, name=%s\n", row.id, row.name);
    fclose(data_fp);
}
