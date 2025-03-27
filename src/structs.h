#ifndef STRUCTS_H
#define STRUCTS_H

#include "constants.h"
#include <stdio.h>
#include <stdlib.h>

// // Row structure for users table
// typedef struct {
//     int id;
//     char name[NAME_LEN];
// } Row;

// Supported Column Types
typedef enum {
    COL_TYPE_INT,
    COL_TYPE_STRING
    // Add more types here (FLOAT, DATE, etc.)
} ColumnType;

// Column Definition
typedef struct {
    char name[MAX_COLUMN_NAME_LEN];
    ColumnType type;
    size_t size;       // Size in bytes (e.g., sizeof(int), or string buffer length)
    size_t offset;     // Offset within the row buffer
    int is_primary_key;
} ColumnDefinition;

// Header structure for metadata
typedef struct {
    int magic;         // Magic number for file identification
    int version;       // File format version
    int node_size;     // Size of each node in bytes
    int root_id;       // ID of the root node
    int next_id;       // Next available node ID
    // Add padding if needed to ensure consistent HEADER_SIZE
    char padding[HEADER_SIZE - (5 * sizeof(int))];
} BTreeHeader;

// Structure to hold state for one B+ Tree instance
typedef struct {
    FILE *fp;               // File pointer for this specific index file
    BTreeHeader header;     // Header info for this index file
    char index_path[MAX_PATH_LEN]; // Path to the index file (for error messages)
} BTreeHandle;

// Table Schema Definition
typedef struct TableSchema { // Give it a name for self-reference if needed
    char name[MAX_TABLE_NAME_LEN];
    ColumnDefinition columns[MAX_COLUMNS];
    int num_columns;
    size_t row_size;
    int pk_column_index;
    BTreeHandle* pk_index; // Pointer to the handle for the primary key index
    char table_dir[MAX_PATH_LEN]; // Directory path for this table
    char data_path[MAX_PATH_LEN]; // Path to the data file
} TableSchema;

// Node structure for both leaf and internal nodes
typedef struct {
    int is_leaf;       // 1 if leaf, 0 if internal
    int num_keys;      // Number of keys currently in the node
    int keys[M-1];     // Array of keys (max M-1 keys)
    long offsets[M-1]; // Array of offsets (only used if leaf)
    int children[M];   // Child node IDs (used if not leaf)
    int next_leaf;     // ID of the next leaf node (used if leaf)
} Node;

// Structure to hold insertion result
typedef struct {
    int split_occurred;  // 1 if split occurred, 0 otherwise
    int separator_key;   // Key to insert into parent if split
    int new_node_id;     // ID of the new node if split
} InsertResult;

#endif
