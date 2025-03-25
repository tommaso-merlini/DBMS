#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "btree.h"
#include "../constants.h"
#include "../structs.h"

// Global file pointer and header
FILE *btree_fp;
Header header;

/**
 * Update the header in the file
 */
void update_header() {
    fseek(btree_fp, 0, SEEK_SET);
    fwrite(&header, sizeof(Header), 1, btree_fp);
    fflush(btree_fp);
}

/**
 * Initialize the B+ tree by opening or creating the file.
 */
void init_btree() {
    btree_fp = fopen("btree.dat", "r+b"); // Open for reading and writing
    if (btree_fp == NULL) {
        // File doesn't exist, create it
        btree_fp = fopen("btree.dat", "w+b");
        if (btree_fp == NULL) {
            perror("Failed to create btree.dat");
            exit(1);
        }

        // Initialize header
        header.magic = MAGIC;
        header.version = 1;
        header.node_size = sizeof(Node);
        header.root_id = 0;
        header.next_id = 1;
        
        // Write header
        update_header();

        // Create an empty leaf node as the initial root
        Node root;
        memset(&root, 0, sizeof(Node));
        root.is_leaf = 1;
        root.num_keys = 0;
        root.next_leaf = -1; // No next leaf
        write_node(0, &root);
    } else {
        // Read existing header
        fread(&header, sizeof(Header), 1, btree_fp);
        if (header.magic != MAGIC) {
            printf("Error: Invalid file format\n");
            exit(1);
        }
    }
}

/**
 * Read a node from disk given its ID.
 * @param id Node ID
 * @return Pointer to the node (must be freed by caller)
 */
Node* read_node(int id) {
    Node* node = malloc(sizeof(Node));
    if (!node) {
        perror("Memory allocation failed");
        exit(1);
    }
    fseek(btree_fp, HEADER_SIZE + id * sizeof(Node), SEEK_SET);
    size_t read_count = fread(node, sizeof(Node), 1, btree_fp);
    if (read_count != 1) {
        perror("Failed to read node");
        free(node);
        exit(1);
    }
    return node;
}

/**
 * Write a node to disk at the specified ID.
 * @param id Node ID
 * @param node Pointer to the node to write
 */
void write_node(int id, Node* node) {
    fseek(btree_fp, HEADER_SIZE + id * sizeof(Node), SEEK_SET);
    fwrite(node, sizeof(Node), 1, btree_fp);
    fflush(btree_fp);
}

/**
 * Allocate a new node ID and update the header.
 * @return New node ID
 */
int allocate_node() {
    int id = header.next_id++;
    update_header();
    return id;
}

/**
 * Search for a key in the B+ tree.
 * @param key Key to search for
 * @param node_id ID of the current node
 * @return Offset if found, -1 if not
 */
long search(int key, int node_id) {
    Node* node = read_node(node_id);
    
    if (node->is_leaf) {
        // Search in leaf node
        for (int i = 0; i < node->num_keys; i++) {
            if (node->keys[i] == key) {
                long offset = node->offsets[i];
                free(node);
                return offset;
            }
        }
        free(node);
        return -1; // Key not found
    } else {
        // Find the appropriate child node
        int i;
        for (i = 0; i < node->num_keys; i++) {
            if (key < node->keys[i]) {
                break;
            }
        }
        int child_id = node->children[i];
        free(node);
        return search(key, child_id);
    }
}

/**
 * Insert a key and offset into a node, handling splits if necessary.
 * @param key Key to insert
 * @param offset Offset of the row in data file
 * @param node_id ID of the current node
 * @return InsertResult indicating if a split occurred
 */
InsertResult insert_into_node(int key, long offset, int node_id) {
    Node* node = read_node(node_id);
    InsertResult result = {0, 0, 0};

    if (node->is_leaf) {
        // Handle insertion into a leaf node
        if (node->num_keys < M-1) {
            // Simple case: Insert key and offset into the leaf node
            int i;
            for (i = node->num_keys - 1; i >= 0 && key < node->keys[i]; i--) {
                node->keys[i+1] = node->keys[i];
                node->offsets[i+1] = node->offsets[i];
            }
            node->keys[i+1] = key;
            node->offsets[i+1] = offset;
            node->num_keys++;
            write_node(node_id, node);
            free(node);
            return result;
        } else {
            // Leaf is full, need to split
            // Create temporary arrays including the new key/offset
            int temp_keys[M];
            long temp_offsets[M];
            
            // Copy existing keys/offsets and insert the new one in order
            int i, j;
            for (i = 0; i < M-1 && key > node->keys[i]; i++) {
                temp_keys[i] = node->keys[i];
                temp_offsets[i] = node->offsets[i];
            }
            
            // Insert the new key/offset
            temp_keys[i] = key;
            temp_offsets[i] = offset;
            
            // Copy the rest
            for (j = i; j < M-1; j++) {
                temp_keys[j+1] = node->keys[j];
                temp_offsets[j+1] = node->offsets[j];
            }
            
            // Create a new leaf node
            Node new_leaf;
            memset(&new_leaf, 0, sizeof(Node));
            new_leaf.is_leaf = 1;
            
            // Split the keys between the nodes
            // For M=3: left takes 1, right takes 2
            node->num_keys = (M / 2);
            new_leaf.num_keys = M - (M / 2);
            
            // Original node keeps the first half
            for (i = 0; i < node->num_keys; i++) {
                node->keys[i] = temp_keys[i];
                node->offsets[i] = temp_offsets[i];
            }
            
            // New node gets the second half
            for (i = 0; i < new_leaf.num_keys; i++) {
                new_leaf.keys[i] = temp_keys[i + node->num_keys];
                new_leaf.offsets[i] = temp_offsets[i + node->num_keys];
            }
            
            // Update leaf node links
            new_leaf.next_leaf = node->next_leaf;
            node->next_leaf = header.next_id;
            
            // Allocate new node in file
            int new_node_id = allocate_node();
            write_node(new_node_id, &new_leaf);
            write_node(node_id, node);
            
            // Return result indicating split
            result.split_occurred = 1;
            result.separator_key = new_leaf.keys[0]; // First key in right node
            result.new_node_id = new_node_id;
            
            free(node);
            return result;
        }
    } else {
        // Internal node: find child to insert into
        int i;
        for (i = 0; i < node->num_keys; i++) {
            if (key < node->keys[i]) {
                break;
            }
        }
        int child_id = node->children[i];
        InsertResult child_result = insert_into_node(key, offset, child_id);

        if (child_result.split_occurred) {
            if (node->num_keys < M-1) {
                // There's room in this node for the new separator key
                for (int j = node->num_keys; j > i; j--) {
                    node->keys[j] = node->keys[j-1];
                    node->children[j+1] = node->children[j];
                }
                node->keys[i] = child_result.separator_key;
                node->children[i+1] = child_result.new_node_id;
                node->num_keys++;
                write_node(node_id, node);
                
                result.split_occurred = 0; // No further split needed
            } else {
                // Internal node is full, need to split it
                // Create temporary arrays including the new key/child
                int temp_keys[M];
                int temp_children[M+1];
                
                // Copy existing keys/children
                int j;
                for (j = 0; j < i; j++) {
                    temp_keys[j] = node->keys[j];
                    temp_children[j] = node->children[j];
                }
                
                // Insert new separator key and child
                temp_keys[i] = child_result.separator_key;
                temp_children[i] = node->children[i];
                temp_children[i+1] = child_result.new_node_id;
                
                // Copy the rest
                for (j = i+1; j <= M-1; j++) {
                    temp_keys[j] = node->keys[j-1];
                    temp_children[j+1] = node->children[j];
                }
                
                // Create new internal node
                Node new_internal;
                memset(&new_internal, 0, sizeof(Node));
                new_internal.is_leaf = 0;
                
                // Split position for internal node
                int split_pos = M / 2;
                
                // Middle key goes up to parent
                int middle_key = temp_keys[split_pos];
                
                // Update current node with first part
                node->num_keys = split_pos;
                for (j = 0; j < split_pos; j++) {
                    node->keys[j] = temp_keys[j];
                    node->children[j] = temp_children[j];
                }
                node->children[split_pos] = temp_children[split_pos];
                
                // Update new node with second part
                new_internal.num_keys = M - split_pos - 1;
                for (j = 0; j < new_internal.num_keys; j++) {
                    new_internal.keys[j] = temp_keys[j + split_pos + 1];
                    new_internal.children[j] = temp_children[j + split_pos + 1];
                }
                new_internal.children[new_internal.num_keys] = temp_children[M];
                
                // Allocate new node in file
                int new_node_id = allocate_node();
                write_node(new_node_id, &new_internal);
                write_node(node_id, node);
                
                // Return result indicating split
                result.split_occurred = 1;
                result.separator_key = middle_key;
                result.new_node_id = new_node_id;
            }
        }
        
        free(node);
        return result;
    }
}

/**
 * Insert a key and offset into the B+ tree.
 * @param key Key to insert
 * @param offset Offset of the row in data file
 */
void btree_insert(int key, long offset) {
    InsertResult result = insert_into_node(key, offset, header.root_id);

    if (result.split_occurred) {
        // Root was split, create a new root
        Node new_root;
        memset(&new_root, 0, sizeof(Node));
        new_root.is_leaf = 0;
        new_root.num_keys = 1;
        new_root.keys[0] = result.separator_key;
        new_root.children[0] = header.root_id;
        new_root.children[1] = result.new_node_id;

        int new_root_id = allocate_node();
        write_node(new_root_id, &new_root);

        // Update root in header
        header.root_id = new_root_id;
        update_header();
    }
}
