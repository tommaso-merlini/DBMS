#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "btree.h"
#include "../constants.h" // Adjust path if needed
#include "../structs.h"  // Adjust path if needed

// --- Remove Global Variables ---
// FILE *btree_fp;
// Header header; // Now part of BTreeHandle

/**
 * Update the header in the specific B+ Tree file.
 * @param handle The B+ Tree instance handle.
 */
void update_btree_header(BTreeHandle* handle) {
    if (!handle || !handle->fp) return;
    fseek(handle->fp, 0, SEEK_SET);
    fwrite(&handle->header, sizeof(BTreeHeader), 1, handle->fp);
    fflush(handle->fp); // Ensure header is written
}

/**
 * Initialize or open a B+ Tree index file.
 * @param index_path Path to the index file.
 * @return Pointer to a BTreeHandle structure, or NULL on failure.
 */
BTreeHandle* init_btree(const char* index_path) {
    BTreeHandle* handle = malloc(sizeof(BTreeHandle));
    if (!handle) {
        perror("Failed to allocate memory for BTreeHandle");
        return NULL;
    }
    strncpy(handle->index_path, index_path, MAX_PATH_LEN - 1);
    handle->index_path[MAX_PATH_LEN - 1] = '\0';
    handle->fp = NULL; // Initialize fp

    handle->fp = fopen(index_path, "r+b"); // Open existing for read/write binary
    if (handle->fp == NULL) {
        // File doesn't exist, create it
        handle->fp = fopen(index_path, "w+b"); // Create new for read/write binary
        if (handle->fp == NULL) {
            fprintf(stderr, "Failed to create B+ Tree index file '%s': %s\n", index_path, strerror(errno));
            free(handle);
            return NULL;
        }

        // Initialize header for new file
        memset(&handle->header, 0, sizeof(BTreeHeader)); // Zero out header
        handle->header.magic = MAGIC;
        handle->header.version = 1;
        handle->header.node_size = sizeof(Node); // Store size of Node struct
        handle->header.root_id = 0;             // Root is initially node 0
        handle->header.next_id = 1;             // Next available node ID is 1
        update_btree_header(handle);

        // Create an empty leaf node as the initial root (node 0)
        Node root_node;
        memset(&root_node, 0, sizeof(Node));
        root_node.is_leaf = 1;
        root_node.num_keys = 0;
        root_node.next_leaf = -1; // No next leaf yet
        write_node(handle, 0, &root_node); // Write root node at ID 0

        printf("Initialized new B+ Tree index file: %s\n", index_path);

    } else {
        // File exists, read header
        size_t read_count = fread(&handle->header, sizeof(BTreeHeader), 1, handle->fp);
        if (read_count != 1 || handle->header.magic != MAGIC) {
            fprintf(stderr, "Error: Invalid or corrupted B+ Tree index file '%s'. Magic: %x\n", index_path, handle->header.magic);
            fclose(handle->fp);
            free(handle);
            return NULL;
        }
        if (handle->header.node_size != sizeof(Node)) {
             fprintf(stderr, "Warning: Node size mismatch in '%s'. File: %d, Expected: %zu. Trying to continue.\n",
                     index_path, handle->header.node_size, sizeof(Node));
             // Decide how critical this is. Maybe exit? For now, warn.
        }
         printf("Opened existing B+ Tree index file: %s (Root ID: %d, Next ID: %d)\n",
                index_path, handle->header.root_id, handle->header.next_id);
    }

    return handle;
}

/**
 * Close the B+ Tree file and free the handle.
 * @param handle The B+ Tree instance handle.
 */
void close_btree(BTreeHandle* handle) {
    if (handle) {
        if (handle->fp) {
            fclose(handle->fp);
        }
        free(handle);
    }
}


/**
 * Read a node from a specific B+ Tree file.
 * @param handle The B+ Tree instance handle.
 * @param id Node ID.
 * @return Pointer to the node (must be freed by caller), or NULL on failure.
 */
Node* read_node(BTreeHandle* handle, int id) {
    if (!handle || !handle->fp) return NULL;

    Node* node = malloc(handle->header.node_size); // Use size from header
    if (!node) {
        perror("Memory allocation failed for node");
        return NULL;
    }

    // Calculate offset: Header size + node_id * node_size
    long offset = (long)HEADER_SIZE + (long)id * handle->header.node_size;
    if (fseek(handle->fp, offset, SEEK_SET) != 0) {
        fprintf(stderr, "Error seeking to node %d in '%s': %s\n", id, handle->index_path, strerror(errno));
        free(node);
        return NULL;
    }

    size_t read_count = fread(node, handle->header.node_size, 1, handle->fp);
    if (read_count != 1) {
        // Check for EOF vs error
        if(feof(handle->fp)) {
             fprintf(stderr, "Error: Unexpected EOF reading node %d from '%s' at offset %ld.\n", id, handle->index_path, offset);
        } else {
             fprintf(stderr, "Error reading node %d from '%s': %s\n", id, handle->index_path, strerror(errno));
        }
        free(node);
        return NULL;
    }
    return node;
}

/**
 * Write a node to a specific B+ Tree file.
 * @param handle The B+ Tree instance handle.
 * @param id Node ID.
 * @param node Pointer to the node to write.
 */
void write_node(BTreeHandle* handle, int id, Node* node) {
     if (!handle || !handle->fp || !node) return;

    // Calculate offset: Header size + node_id * node_size
    long offset = (long)HEADER_SIZE + (long)id * handle->header.node_size;
     if (fseek(handle->fp, offset, SEEK_SET) != 0) {
        fprintf(stderr, "Error seeking to write node %d in '%s': %s\n", id, handle->index_path, strerror(errno));
        return; // Or maybe exit?
    }

    size_t write_count = fwrite(node, handle->header.node_size, 1, handle->fp);
    if (write_count != 1) {
         fprintf(stderr, "Error writing node %d to '%s': %s\n", id, handle->index_path, strerror(errno));
         // What to do here? File might be corrupted.
    }
    fflush(handle->fp); // Ensure data is written
}

/**
 * Allocate a new node ID for a specific B+ Tree.
 * @param handle The B+ Tree instance handle.
 * @return New node ID.
 */
int allocate_node(BTreeHandle* handle) {
    if (!handle) return -1; // Should not happen
    int id = handle->header.next_id++;
    update_btree_header(handle); // Save the updated header (with new next_id)
    return id;
}


/**
 * Search for a key in a specific B+ tree (Recursive part).
 * @param handle The B+ Tree instance handle.
 * @param key Key to search for.
 * @param node_id ID of the current node.
 * @return Offset if found, -1 if not.
 */
long search_recursive(BTreeHandle* handle, int key, int node_id) {
    if (!handle) return -1;
    Node* node = read_node(handle, node_id);
    if (!node) {
         fprintf(stderr, "Search failed: Could not read node %d in '%s'\n", node_id, handle->index_path);
         return -1; // Indicate error/not found
    }

    long offset = -1; // Default to not found

    if (node->is_leaf) {
        // Search in leaf node
        for (int i = 0; i < node->num_keys; i++) {
            if (node->keys[i] == key) {
                offset = node->offsets[i];
                break; // Found it
            }
        }
    } else {
        // Internal node: Find the appropriate child node
        int i;
        for (i = 0; i < node->num_keys; i++) {
            if (key < node->keys[i]) {
                break;
            }
        }
        // Recursively search in the determined child
        // Note: Child ID is node->children[i]
        int child_id = node->children[i];
        free(node); // Free current node *before* recursive call
        return search_recursive(handle, key, child_id);
    }

    // Free the node read in this function call
    free(node);
    return offset;
}

/**
 * Search for a key in a specific B+ tree (Public entry point).
 * @param handle The B+ Tree instance handle.
 * @param key Key to search for.
 * @return Offset if found, -1 if not.
 */
long search(BTreeHandle* handle, int key) {
     if (!handle) return -1;
     // Start search from the root node specified in the handle's header
     return search_recursive(handle, key, handle->header.root_id);
}


/**
 * Insert a key/offset into a node in a specific tree, handling splits.
 * @param handle The B+ Tree instance handle.
 * @param key Key to insert.
 * @param offset Offset of the row in data file.
 * @param node_id ID of the current node.
 * @return InsertResult indicating if a split occurred.
 */
InsertResult insert_into_node(BTreeHandle* handle, int key, long offset, int node_id) {
    InsertResult result = {0, 0, 0}; // Initialize result (no split yet)
    if (!handle) return result; // Should not happen

    Node* node = read_node(handle, node_id);
     if (!node) {
         fprintf(stderr, "Insert failed: Could not read node %d in '%s'\n", node_id, handle->index_path);
         // How to signal fatal error? For now, return no-split result, but this is bad.
         return result;
     }

    if (node->is_leaf) {
        // Handle insertion into a leaf node
        if (node->num_keys < M - 1) {
            // Simple case: Insert key and offset into the leaf node
            int i;
            // Shift keys/offsets greater than the new key
            for (i = node->num_keys - 1; i >= 0 && key < node->keys[i]; i--) {
                node->keys[i + 1] = node->keys[i];
                node->offsets[i + 1] = node->offsets[i];
            }
            // Insert the new key/offset
            node->keys[i + 1] = key;
            node->offsets[i + 1] = offset;
            node->num_keys++;
            write_node(handle, node_id, node);
            // result remains {0, 0, 0}
        } else {
            // Leaf is full, need to split
            int temp_keys[M];
            long temp_offsets[M];

            // Merge existing keys/offsets and the new one into temp arrays
             int i = 0, j = 0;
             while (i < M - 1 && node->keys[i] < key) {
                 temp_keys[j] = node->keys[i];
                 temp_offsets[j] = node->offsets[i];
                 i++; j++;
             }
             temp_keys[j] = key; // Insert the new key/offset
             temp_offsets[j] = offset;
             j++;
             while (i < M - 1) {
                 temp_keys[j] = node->keys[i];
                 temp_offsets[j] = node->offsets[i];
                 i++; j++;
             }

            // Create a new leaf node
            Node new_leaf;
            memset(&new_leaf, 0, sizeof(Node));
            new_leaf.is_leaf = 1;

            // Split point (integer division)
            int split_point = M / 2; // For M=3, split_point = 1. Left gets 1, right gets 2.

            // Update original node (left node)
            node->num_keys = split_point;
            for (i = 0; i < split_point; i++) {
                node->keys[i] = temp_keys[i];
                node->offsets[i] = temp_offsets[i];
            }
             // Zero out unused parts of original node for clarity (optional)
            memset(&node->keys[split_point], 0, (M - 1 - split_point) * sizeof(int));
            memset(&node->offsets[split_point], 0, (M - 1 - split_point) * sizeof(long));


            // Fill new node (right node)
            new_leaf.num_keys = M - split_point;
            for (i = 0; i < new_leaf.num_keys; i++) {
                new_leaf.keys[i] = temp_keys[i + split_point];
                new_leaf.offsets[i] = temp_offsets[i + split_point];
            }

            // Allocate ID for the new node
            int new_node_id = allocate_node(handle);

            // Update leaf node links
            new_leaf.next_leaf = node->next_leaf;
            node->next_leaf = new_node_id;

            // Write both nodes back to disk
            write_node(handle, node_id, node);
            write_node(handle, new_node_id, &new_leaf);

            // Prepare result for parent
            result.split_occurred = 1;
            result.separator_key = new_leaf.keys[0]; // First key of the new right node goes up
            result.new_node_id = new_node_id;
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

        // Recursively insert into the child
        InsertResult child_result = insert_into_node(handle, key, offset, child_id);

        // Check if the child split
        if (child_result.split_occurred) {
            // Try to insert the separator key from child into the current internal node
            if (node->num_keys < M - 1) {
                // Room available in current node
                // Shift keys and children pointers to make space
                for (int j = node->num_keys; j > i; j--) {
                    node->keys[j] = node->keys[j - 1];
                    node->children[j + 1] = node->children[j];
                }
                // Insert separator key and new child pointer
                node->keys[i] = child_result.separator_key;
                node->children[i + 1] = child_result.new_node_id;
                node->num_keys++;
                write_node(handle, node_id, node);
                // result remains {0, 0, 0} - split was handled here
            } else {
                // Internal node is full, need to split it
                int temp_keys[M];
                int temp_children[M + 1];

                // Merge existing keys/children and the new one from child split
                int k = 0, c = 0; // Index for temp arrays
                // Copy keys/children before insertion point
                 for(int j=0; j < i; ++j) {
                    temp_keys[k++] = node->keys[j];
                    temp_children[c++] = node->children[j];
                 }
                 // Add the first child pointer at the split point
                 temp_children[c++] = node->children[i];

                 // Insert the new separator key and child pointer
                 temp_keys[k++] = child_result.separator_key;
                 temp_children[c++] = child_result.new_node_id;

                 // Copy keys/children after insertion point
                 for(int j=i; j < M-1; ++j) {
                    temp_keys[k++] = node->keys[j];
                    temp_children[c++] = node->children[j+1];
                 }


                // Create new internal node
                Node new_internal;
                memset(&new_internal, 0, sizeof(Node));
                new_internal.is_leaf = 0;

                // Split point for internal node (key that moves up)
                int split_key_index = M / 2; // For M=3, index 1 (middle key)
                int middle_key = temp_keys[split_key_index];

                // Update current node (left node)
                node->num_keys = split_key_index;
                for (int j = 0; j < node->num_keys; j++) {
                    node->keys[j] = temp_keys[j];
                    node->children[j] = temp_children[j];
                }
                node->children[node->num_keys] = temp_children[node->num_keys]; // Copy the last child pointer
                 // Zero out unused parts (optional)
                 memset(&node->keys[node->num_keys], 0, (M - 1 - node->num_keys) * sizeof(int));
                 memset(&node->children[node->num_keys + 1], 0, (M - (node->num_keys + 1)) * sizeof(int));


                // Fill new internal node (right node)
                new_internal.num_keys = M - 1 - split_key_index; // M-1 total keys, minus left keys, minus middle key
                for (int j = 0; j < new_internal.num_keys; j++) {
                    new_internal.keys[j] = temp_keys[j + split_key_index + 1];
                    new_internal.children[j] = temp_children[j + split_key_index + 1];
                }
                new_internal.children[new_internal.num_keys] = temp_children[M]; // Copy the last child pointer


                // Allocate ID for the new internal node
                int new_node_id = allocate_node(handle);

                // Write nodes
                write_node(handle, node_id, node);
                write_node(handle, new_node_id, &new_internal);

                // Prepare result for parent (this node split)
                result.split_occurred = 1;
                result.separator_key = middle_key; // The middle key goes up
                result.new_node_id = new_node_id;
            }
        }
        // else: Child did not split, nothing more to do here. result remains {0,0,0}
    }

    free(node); // Free the node read in this function call
    return result;
}


/**
 * Insert a key and offset into a specific B+ tree (Public entry point).
 * @param handle The B+ Tree instance handle.
 * @param key Key to insert.
 * @param offset Offset of the row in data file.
 */
void btree_insert(BTreeHandle* handle, int key, long offset) {
    if (!handle) return;

    // Start insertion from the root node
    InsertResult result = insert_into_node(handle, key, offset, handle->header.root_id);

    // Check if the root node itself was split
    if (result.split_occurred) {
        // Create a new root node
        Node new_root;
        memset(&new_root, 0, sizeof(Node));
        new_root.is_leaf = 0; // New root is always internal (unless tree has only 1 node total, handled implicitly)
        new_root.num_keys = 1;
        new_root.keys[0] = result.separator_key;    // The key that came up from the split
        new_root.children[0] = handle->header.root_id; // Old root is the left child
        new_root.children[1] = result.new_node_id;     // New node from split is the right child

        // Allocate an ID for the new root
        int new_root_id = allocate_node(handle);

        // Write the new root node to disk
        write_node(handle, new_root_id, &new_root);

        // Update the handle's header to point to the new root
        handle->header.root_id = new_root_id;
        update_btree_header(handle); // Save the updated header
        printf("Root split. New root ID: %d\n", new_root_id);
    }
}
