#ifndef DATABASE_H
#define DATABASE_H

#include <stdio.h>
#include "../structs.h" // Adjust path

// --- Global Schema Storage ---
extern TableSchema database_schema[MAX_TABLES];
extern int num_tables;

// --- Function Prototypes ---

// Initialization & Cleanup
int init_database(); // Return status (0 success, -1 error)
void shutdown_database(); // Close files, free handles
int load_schema(); // Return status

// Schema Lookup (no change needed)
TableSchema* find_table_schema(const char* table_name);
const ColumnDefinition* find_column(const TableSchema* schema, const char* col_name);

int select_scan(const char* table_name, const char* filter_col_name, const char* filter_val_str);

// Row Operations (Take table name, data file path is in schema)
long append_row_to_file(const TableSchema* schema, const void* row_data);
int insert_row(const char* table_name, const void* row_data); // Return status
int select_row(const char* table_name, int primary_key_value, void** row_data_out);

// Helpers (no change needed)
void print_row(const TableSchema* schema, const void* row_data);
int get_int_pk_value(const TableSchema* schema, const void* row_data);

// Path Helper
void build_path(char *dest, size_t dest_size, const char *part1, const char *part2, const char *part3);


#endif // DATABASE_H
