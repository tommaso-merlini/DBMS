#ifndef BTREE_H
#define BTREE_H

#include "../structs.h"

// Function prototypes for B+ tree operations
void init_btree(void);
Node* read_node(int id);
void write_node(int id, Node* node);
int allocate_node(void);
long search(int key, int node_id);
InsertResult insert_into_node(int key, long offset, int node_id);
void btree_insert(int key, long offset);
void update_header(void);

#endif
