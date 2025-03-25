#ifndef STRUCTS_H
#define STRUCTS_H

#include "constants.h"

// Row structure for users table
typedef struct {
    int id;
    char name[NAME_LEN];
} Row;

// Node structure for both leaf and internal nodes
typedef struct {
    int is_leaf;       // 1 if leaf, 0 if internal
    int num_keys;      // Number of keys currently in the node
    int keys[M-1];     // Array of keys (max M-1 keys)
    long offsets[M-1]; // Array of offsets (only used if leaf)
    int children[M];   // Child node IDs (used if not leaf)
    int next_leaf;     // ID of the next leaf node (used if leaf)
} Node;

// Header structure for metadata
typedef struct {
    int magic;         // Magic number for file identification
    int version;       // File format version
    int node_size;     // Size of each node in bytes
    int root_id;       // ID of the root node
    int next_id;       // Next available node ID
} Header;

// Structure to hold insertion result
typedef struct {
    int split_occurred;  // 1 if split occurred, 0 otherwise
    int separator_key;   // Key to insert into parent if split
    int new_node_id;     // ID of the new node if split
} InsertResult;

#endif
