#ifndef DATABASE_H
#define DATABASE_H

#include <stdio.h>
#include "../structs.h"

// --- Global Schema Storage ---
// Make schema globally accessible or pass it around. Global for simplicity here.
extern TableSchema database_schema[MAX_TABLES];
extern int num_tables;

// Initialization
void init_database();
void load_schema(); // New function to load schema from metadata.dat

// Schema Lookup
TableSchema* find_table_schema(const char* table_name);
const ColumnDefinition* find_column(const TableSchema* schema, const char* col_name);

// Row Operations (Generic)
long append_row_to_file(const TableSchema* schema, const void* row_data);
void insert_row(const char* table_name, const void* row_data);
void select_row(const char* table_name, int primary_key_value); // Still assumes int PK for lookup

// Helper for printing a generic row
void print_row(const TableSchema* schema, const void* row_data);

// Helper to get PK value (assuming int)
int get_int_pk_value(const TableSchema* schema, const void* row_data);


#endif
