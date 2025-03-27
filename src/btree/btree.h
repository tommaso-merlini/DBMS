#ifndef BTREE_H
#define BTREE_H

#include <stdio.h>
#include "../structs.h" // Adjust path if structs.h moves

// --- Function Prototypes (Now take BTreeHandle*) ---

// Initialize/Open a B+ Tree index file
BTreeHandle* init_btree(const char* index_path);

// Close a B+ Tree index file and free handle
void close_btree(BTreeHandle* handle);

// Read/Write Nodes for a specific tree
Node* read_node(BTreeHandle* handle, int node_id);
void write_node(BTreeHandle* handle, int node_id, Node* node);

// Search within a specific tree
long search(BTreeHandle* handle, int key); // Entry point for search
long search_recursive(BTreeHandle* handle, int key, int node_id); // Internal recursive part

// Insert into a specific tree
void btree_insert(BTreeHandle* handle, int key, long offset); // Entry point
InsertResult insert_into_node(BTreeHandle* handle, int key, long offset, int node_id); // Internal recursive part

// Helper functions (internal or public if needed)
void update_btree_header(BTreeHandle* handle);
int allocate_node(BTreeHandle* handle);


#endif // BTREE_H
