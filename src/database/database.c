#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>   // For mkdir
#include <sys/types.h> // For mkdir types
#include "database.h"
#include "../btree/btree.h" // Include new btree prototypes
#include "../constants.h"
#include "../structs.h"

// --- Global Schema Storage ---
TableSchema database_schema[MAX_TABLES];
int num_tables = 0;

// --- Remove Global File Pointers ---
// FILE *metadata_fp, *data_fp;

// --- Path Helper ---
void build_path(char *dest, size_t dest_size, const char *part1, const char *part2, const char *part3) {
    if (!dest || dest_size == 0) return;
    dest[0] = '\0'; // Start with empty string

    if (part1) {
        strncat(dest, part1, dest_size - strlen(dest) - 1);
    }
    if (part2) {
        if (strlen(dest) > 0 && dest[strlen(dest)-1] != '/') {
             strncat(dest, "/", dest_size - strlen(dest) - 1);
        }
        strncat(dest, part2, dest_size - strlen(dest) - 1);
    }
     if (part3) {
        if (strlen(dest) > 0 && dest[strlen(dest)-1] != '/') {
             strncat(dest, "/", dest_size - strlen(dest) - 1);
        }
        strncat(dest, part3, dest_size - strlen(dest) - 1);
    }
}

// --- Directory Creation Helper ---
int ensure_directory_exists(const char *path) {
    struct stat st = {0};
    if (stat(path, &st) == -1) {
        // Directory doesn't exist, attempt to create it
        #ifdef _WIN32
            if (mkdir(path) != 0) {
        #else
            if (mkdir(path, 0775) != 0) { // Read/Write/Execute for owner/group, Read/Execute for others
        #endif
            fprintf(stderr, "Failed to create directory '%s': %s\n", path, strerror(errno));
            return -1; // Failure
        }
        printf("Created directory: %s\n", path);
    } else if (!S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Error: Path '%s' exists but is not a directory.\n", path);
        return -1; // Failure - path exists but isn't a directory
    }
    return 0; // Success or directory already exists
}


// --- Schema Management (Modified load_schema) ---
/**
 * @brief Finds a table schema by name.
 * @param table_name Name of the table.
 * @return Pointer to the TableSchema, or NULL if not found.
 */
TableSchema* find_table_schema(const char* table_name) {
    for (int i = 0; i < num_tables; ++i) {
        if (strcmp(database_schema[i].name, table_name) == 0) {
            return &database_schema[i];
        }
    }
    return NULL;
}

/**
 * @brief Finds a column definition within a table schema by name.
 * @param schema Pointer to the table schema.
 * @param col_name Name of the column.
 * @return Pointer to the ColumnDefinition, or NULL if not found.
 */
const ColumnDefinition* find_column(const TableSchema* schema, const char* col_name) {
    if (!schema) return NULL;
    for (int i = 0; i < schema->num_columns; ++i) {
        if (strcmp(schema->columns[i].name, col_name) == 0) {
            return &schema->columns[i];
        }
    }
    return NULL;
}

/**
 * Loads table schemas from the metadata file, initializes BTree handles.
 * Return 0 on success, -1 on error.
 */
/**
 * Loads table schemas from the metadata file, initializes BTree handles.
 * Creates a default metadata file if it doesn't exist.
 * Return 0 on success, -1 on error.
 */
int load_schema() {
    char metadata_path[MAX_PATH_LEN];
    build_path(metadata_path, sizeof(metadata_path), DATA_DIR, METADATA_FILE, NULL);

    FILE *meta_fp = fopen(metadata_path, "r");
    if (!meta_fp) {
        fprintf(stderr, "Metadata file '%s' not found. Creating default schema.\n", metadata_path);

        // --- START: Create Default Metadata File ---
        meta_fp = fopen(metadata_path, "w"); // Open for writing
        if (!meta_fp) {
             fprintf(stderr, "FATAL: Failed to create metadata file '%s': %s\n", metadata_path, strerror(errno));
             num_tables = 0; // Ensure num_tables is 0 on failure
             return -1; // Critical error
        }

        // Write default schema (e.g., users and products)
        fprintf(meta_fp, "# Default database schema\n");
        fprintf(meta_fp, "table:users\n");
        fprintf(meta_fp, "column:id:int:primary_key\n");
        fprintf(meta_fp, "column:name:string:%d\n", NAME_LEN); // Use constant if still defined, or hardcode like 50
        fprintf(meta_fp, "\n"); // Add a newline for readability
        fprintf(meta_fp, "table:products\n");
        fprintf(meta_fp, "column:prod_id:int:primary_key\n");
        fprintf(meta_fp, "column:description:string:100\n");
        fprintf(meta_fp, "column:price:int\n");

        fclose(meta_fp); // Close after writing

        // Now, reopen the file for reading to continue loading
        meta_fp = fopen(metadata_path, "r");
        if (!meta_fp) {
            // This should not happen if writing succeeded, but check anyway
            fprintf(stderr, "FATAL: Failed to reopen metadata file '%s' for reading after creating it.\n", metadata_path);
            num_tables = 0;
            return -1;
        }
        printf("Created and reopened '%s' for reading.\n", metadata_path);
        // --- END: Create Default Metadata File ---
    }

    // --- Parsing Loop (Same as before, but will now run after default creation) ---
    char line[256];
    TableSchema* current_schema = NULL;
    size_t current_offset = 0;
    num_tables = 0; // Reset count before parsing

    while (fgets(line, sizeof(line), meta_fp)) {
        line[strcspn(line, "\r\n")] = 0;
        if (strlen(line) == 0 || line[0] == '#') continue;

        char* token;
        char* rest = line;
        token = strtok_r(rest, ":", &rest);
        if (!token) continue;

        if (strcmp(token, "table") == 0) {
            if (num_tables >= MAX_TABLES) { /* error handling */ break; }
            current_schema = &database_schema[num_tables++]; // Increment num_tables HERE
            memset(current_schema, 0, sizeof(TableSchema));
            current_schema->pk_column_index = -1;
            current_schema->pk_index = NULL;

            token = strtok_r(rest, ":", &rest); // Get table name
            if (!token) { /* error handling */ num_tables--; current_schema=NULL; continue; }

            strncpy(current_schema->name, token, MAX_TABLE_NAME_LEN - 1);
            current_schema->name[MAX_TABLE_NAME_LEN - 1] = '\0';
            current_schema->num_columns = 0;
            current_schema->row_size = 0;
            current_offset = 0;

            build_path(current_schema->table_dir, sizeof(current_schema->table_dir), DATA_DIR, current_schema->name, NULL);
            if (ensure_directory_exists(current_schema->table_dir) != 0) {
                 /* error handling */ num_tables--; current_schema = NULL; continue;
            }
            char data_filename[MAX_TABLE_NAME_LEN + sizeof(TABLE_DATA_EXT)];
            snprintf(data_filename, sizeof(data_filename), "%s%s", current_schema->name, TABLE_DATA_EXT);
            build_path(current_schema->data_path, sizeof(current_schema->data_path), current_schema->table_dir, data_filename, NULL);
            printf("Loading schema for table: %s (Data: %s)\n", current_schema->name, current_schema->data_path);

        } else if (strcmp(token, "column") == 0) {
             if (!current_schema) { /* error handling */ continue; }
             if (current_schema->num_columns >= MAX_COLUMNS) { /* error handling */ continue; }

             ColumnDefinition* col = &(current_schema->columns[current_schema->num_columns]); // Get address BEFORE incrementing num_columns
             memset(col, 0, sizeof(ColumnDefinition));

             char* col_name = strtok_r(rest, ":", &rest);
             char* col_type = strtok_r(rest, ":", &rest);
             char* col_arg = strtok_r(rest, ":", &rest);  // Might be NULL
             char* col_flag = strtok_r(rest, ":", &rest); // Might be NULL

             // Only require name and type initially
             if (!col_name || !col_type) {
                 fprintf(stderr, "Error: Malformed 'column' line (missing name or type) for table '%s'\n", current_schema->name);
                 continue;
             }

             strncpy(col->name, col_name, MAX_COLUMN_NAME_LEN - 1);
             col->name[MAX_COLUMN_NAME_LEN - 1] = '\0';
             col->offset = current_offset; // Set offset before calculating size

             col->is_primary_key = 0;
             int is_pk = 0;

             if (strcmp(col_type, "int") == 0) {
                 col->type = COL_TYPE_INT;
                 col->size = sizeof(int);
                 // Check if col_arg exists and is "primary_key"
                 if (col_arg && strcmp(col_arg, "primary_key") == 0) {
                     is_pk = 1;
                 }
                 // Note: for 'int', col_flag is ignored currently
             } else if (strcmp(col_type, "string") == 0) {
                 col->type = COL_TYPE_STRING;
                 if (!col_arg) {
                      fprintf(stderr, "Error: Missing size argument for string column '%s' in table '%s'\n", col_name, current_schema->name);
                      continue; // Skip this invalid string column
                 }
                 col->size = atoi(col_arg);
                 if (col->size <= 0 || col->size > 1024*10) {
                     fprintf(stderr, "Warning: Invalid size %d for string column '%s'. Using default %d.\n", (int)col->size, col_name, NAME_LEN);
                     col->size = NAME_LEN;
                 }
                 // Check if col_flag exists and is "primary_key"
                 if (col_flag && strcmp(col_flag, "primary_key") == 0) {
                     is_pk = 1;
                 }
             } else {
                  fprintf(stderr, "Error: Unknown column type '%s' for column '%s'\n", col_type, col_name);
                  continue; // Skip unknown type
             }

             // Primary Key check logic (remains the same)
             if (is_pk) {
                if (current_schema->pk_column_index != -1) {
                     fprintf(stderr, "Warning: Multiple PKs for table '%s'. Ignoring PK for '%s'.\n", current_schema->name, col->name);
                 } else if (col->type != COL_TYPE_INT) {
                     fprintf(stderr, "Warning: PK '%s' in table '%s' is not INT. Indexing ignored.\n", col->name, current_schema->name);
                 } else {
                     col->is_primary_key = 1;
                     current_schema->pk_column_index = current_schema->num_columns; // Use current index BEFORE increment
                     printf("  -> Primary Key set to column: %s\n", col->name);
                 }
             }

             // Increment counts and update sizes AFTER successful parsing
             current_offset += col->size;
             current_schema->row_size = current_offset;
             current_schema->num_columns++; // Increment count HERE
             printf("    Column: %s, Type: %d, Size: %zu, Offset: %zu, PK: %d\n", col->name, col->type, col->size, col->offset, col->is_primary_key);
        } else {
            fprintf(stderr, "Warning: Unrecognized line type '%s' in metadata.dbm\n", token);
        }
    }
    fclose(meta_fp);

    // --- Initialize B+ Tree (Same as before) ---
    for (int i = 0; i < num_tables; ++i) {
        TableSchema* schema = &database_schema[i];
        if (schema->pk_column_index != -1) {
            char index_filename[MAX_TABLE_NAME_LEN + 10];
            snprintf(index_filename, sizeof(index_filename), "pk%s", PK_INDEX_EXT);
            char index_path[MAX_PATH_LEN];
            build_path(index_path, sizeof(index_path), schema->table_dir, index_filename, NULL);

            schema->pk_index = init_btree(index_path);
            if (!schema->pk_index) {
                fprintf(stderr, "FATAL: Failed to initialize primary key index for table '%s' at '%s'\n", schema->name, index_path);
                // Cleanup already opened B-trees
                for (int j = 0; j < i; ++j) {
                    if(database_schema[j].pk_index) {
                        close_btree(database_schema[j].pk_index);
                        database_schema[j].pk_index = NULL;
                    }
                }
                return -1;
            }
            printf("Initialized PK index for table '%s' at '%s'\n", schema->name, index_path);
        } else {
            /* warning */
        }
    }

    printf("Schema loading complete. %d table(s) loaded.\n", num_tables);
    return 0; // Success
}

// --- Database Initialization & Shutdown ---

/**
 * Initialize the database: Create data dir, load schema.
 * Return 0 on success, -1 on failure.
 */
int init_database() {
    printf("Initializing database in directory: %s\n", DATA_DIR);

    // Ensure the main data directory exists
    if (ensure_directory_exists(DATA_DIR) != 0) {
        return -1;
    }

    // Load schema (this will also create table dirs and init B-Trees)
    if (load_schema() != 0) {
        fprintf(stderr, "Database initialization failed during schema loading.\n");
        // Perform partial cleanup maybe?
        shutdown_database(); // Close any B-Trees that were opened
        return -1;
    }

    // Check if data files exist (optional, could be created on first write)
    for(int i=0; i < num_tables; ++i) {
        FILE *fp_check = fopen(database_schema[i].data_path, "ab"); // Try opening in append mode
        if(!fp_check) {
             fprintf(stderr, "Warning: Could not open/create data file %s: %s\n", database_schema[i].data_path, strerror(errno));
             // Decide if this is fatal
        } else {
            fclose(fp_check);
        }
    }

    printf("Database initialization complete.\n");
    return 0; // Success
}

/**
 * Shutdown the database: Close B-Tree files, free handles.
 */
void shutdown_database() {
    printf("Shutting down database...\n");
    for (int i = 0; i < num_tables; ++i) {
        if (database_schema[i].pk_index) {
            printf("Closing index for table '%s'\n", database_schema[i].name);
            close_btree(database_schema[i].pk_index);
            database_schema[i].pk_index = NULL; // Avoid double free
        }
    }
    num_tables = 0; // Reset table count
    printf("Database shutdown complete.\n");
}


// --- Row Operations (Using Schema Paths and BTree Handles) ---

/**
 * Append a generic row buffer to the table's data file.
 * @param schema Pointer to the table schema (contains data_path).
 * @param row_data Pointer to the raw row data buffer.
 * @return Offset where the row is written, or -1 on error.
 */
long append_row_to_file(const TableSchema* schema, const void* row_data) {
    if (!schema || !row_data) return -1;

    FILE *data_fp = fopen(schema->data_path, "ab"); // Open in append binary mode
    if (!data_fp) {
        fprintf(stderr, "Error opening data file '%s' for appending: %s\n", schema->data_path, strerror(errno));
        return -1;
    }

    // We are in append mode, ftell gives current end-of-file position *before* write
    long offset = ftell(data_fp);
    if (offset == -1) {
        perror("Error getting file position");
        fclose(data_fp);
        return -1;
    }

    size_t written = fwrite(row_data, schema->row_size, 1, data_fp);
    // fflush(data_fp); // Append mode often buffers less aggressively, but fflush ensures it
    fclose(data_fp);

    if (written != 1) {
        fprintf(stderr, "Error writing row data to '%s' (expected %zu bytes)\n", schema->data_path, schema->row_size);
        // Difficult to recover cleanly here. Offset is likely useless now.
        return -1;
    }

    return offset;
}

int get_int_pk_value(const TableSchema* schema, const void* row_data) {
    // ... (no changes needed, but ensure fatal errors are handled if desired)
     if (!schema || !row_data) { exit(EXIT_FAILURE); } // Example fatal exit
     if (schema->pk_column_index < 0) { exit(EXIT_FAILURE); }
     const ColumnDefinition* pk_col = &schema->columns[schema->pk_column_index];
     if (pk_col->type != COL_TYPE_INT) { exit(EXIT_FAILURE); }
     int pk_value;
     memcpy(&pk_value, (const char*)row_data + pk_col->offset, sizeof(int));
     return pk_value;
}

/**
 * Insert a generic row into the database.
 * @param table_name Name of the table to insert into.
 * @param row_data Pointer to the raw row data buffer.
 * @return 0 on success, -1 on error, 1 for duplicate key.
 */
int insert_row(const char* table_name, const void* row_data) {
    TableSchema* schema = find_table_schema(table_name);
    if (!schema) {
        fprintf(stderr, "Error: Table '%s' not found for insert.\n", table_name);
        return -1;
    }
    if (!schema->pk_index) { // Check if BTree handle exists
         fprintf(stderr, "Error: Cannot insert into table '%s' without a valid primary key index.\n", table_name);
         return -1;
     }

    // Extract primary key (must be int)
    int pk_value = get_int_pk_value(schema, row_data);

    // Check for duplicates using the table's specific B+ Tree
    long existing_offset = search(schema->pk_index, pk_value); // Use handle
    if (existing_offset != -1) {
        fprintf(stderr, "Error: Duplicate primary key value %d in table '%s'.\n", pk_value, table_name);
        return 1; // Indicate duplicate key
    }

    // Append row data to the table's data file
    long offset = append_row_to_file(schema, row_data);
    if (offset == -1) {
        fprintf(stderr, "Error: Failed to append row data for table '%s'.\n", table_name);
        return -1; // Data write failed
    }

    // Insert key (PK value) and offset into the table's B+ Tree
    btree_insert(schema->pk_index, pk_value, offset); // Use handle

    printf("Inserted into %s: PK=%d at offset=%ld (Data: %s, Index: %s)\n",
           table_name, pk_value, offset, schema->data_path, schema->pk_index->index_path);
    return 0; // Success
}

/**
 * Select and display a row by its primary key value.
 * @param table_name Name of the table.
 * @param primary_key_value The integer primary key value to find.
 * @return 0 if found, 1 if not found, -1 on error.
 */
int select_row(const char* table_name, int primary_key_value) {
    TableSchema* schema = find_table_schema(table_name);
    if (!schema) {
        fprintf(stderr, "Error: Table '%s' not found for select.\n", table_name);
        return -1;
    }
     if (!schema->pk_index) {
         fprintf(stderr, "Error: Cannot select from table '%s' without a valid primary key index.\n", table_name);
         return -1;
     }

    // Search the table's B+ Tree for the offset
    long offset = search(schema->pk_index, primary_key_value); // Use handle
    if (offset == -1) {
        printf("Record with PK %d not found in table '%s'\n", primary_key_value, table_name);
        return 1; // Not found
    }

    // Open the table's data file and seek to the offset
    FILE *data_fp = fopen(schema->data_path, "rb");
    if (!data_fp) {
        fprintf(stderr, "Error opening data file '%s' for reading: %s\n", schema->data_path, strerror(errno));
        return -1;
    }

    if (fseek(data_fp, offset, SEEK_SET) != 0) {
        fprintf(stderr, "Error seeking to offset %ld in '%s': %s\n", offset, schema->data_path, strerror(errno));
        fclose(data_fp);
        return -1;
    }

    // Allocate buffer and read the row data
    void* row_data = malloc(schema->row_size);
    if (!row_data) { 
        // TODO:  error handling ...
        fclose(data_fp); return -1; 
    }

    size_t read_count = fread(row_data, schema->row_size, 1, data_fp);
    fclose(data_fp);

    if (read_count != 1) {
        fprintf(stderr, "Error reading row at offset %ld from '%s'. Expected %zu bytes.\n", offset, schema->data_path, schema->row_size);
        free(row_data);
        return -1;
    }

    // Print the found row
    printf("Found in %s (PK=%d, Offset=%ld):\n", table_name, primary_key_value, offset);
    print_row(schema, row_data);

    free(row_data);
    return 0; // Found
}

/**
 * @brief Compares a filter value string with data in a buffer based on column definition.
 * @param col The column definition.
 * @param field_ptr Pointer to the start of the field data within the row buffer.
 * @param filter_val_str The filter value as a string from the query.
 * @return 1 if match, 0 if no match, -1 on error (e.g., bad filter value type).
 */
int compare_value(const ColumnDefinition* col, const void* field_ptr, const char* filter_val_str) {
    if (!col || !field_ptr || !filter_val_str) return -1;

    if (col->type == COL_TYPE_INT) {
        int stored_value;
        memcpy(&stored_value, field_ptr, sizeof(int));

        // Convert filter string to int
        char *endptr;
        errno = 0;
        long filter_val_long = strtol(filter_val_str, &endptr, 10);
        // Check conversion errors
        if (endptr == filter_val_str || *endptr != '\0' || errno == ERANGE || (errno != 0 && filter_val_long == 0)) {
            fprintf(stderr, "Error: Invalid integer filter value '%s' for column '%s'.\n", filter_val_str, col->name);
            return -1; // Indicate error
        }
        int filter_value = (int)filter_val_long; // Assume fits in int

        return (stored_value == filter_value); // Return 1 if match, 0 if not

    } else if (col->type == COL_TYPE_STRING) {
        // Compare strings carefully, up to the defined size.
        // The stored string might not be null-terminated if it fills the space.
        // The filter string *is* null-terminated.

        // Use strncmp for safety. Compare up to col->size bytes.
        // We need to check if the filter string *exactly matches* the potentially
        // non-null-terminated stored string within its boundary.

        int result = strncmp((const char*)field_ptr, filter_val_str, col->size);

        if (result == 0) {
            // First col->size bytes match. Now check if filter string is *also*
            // exactly col->size long OR if the stored string has a null terminator
            // right after the matched part.
            size_t filter_len = strlen(filter_val_str);
            if (filter_len == col->size) {
                // Filter string fills the buffer exactly, matches.
                return 1;
            } else if (filter_len < col->size) {
                // Filter string is shorter. Check if the stored data has a
                // null terminator or padding right after the filter length.
                return (((const char*)field_ptr)[filter_len] == '\0');
            } else {
                // Filter string is longer than the column size, cannot match exactly.
                return 0;
            }
        } else {
            // Initial strncmp failed.
            return 0;
        }

    } else {
        fprintf(stderr, "Error: Comparison not implemented for this column type.\n");
        return -1; // Error for unsupported type
    }
}


/**
 * @brief Performs a full table scan to find rows matching a filter condition.
 * Currently only supports equality check ('=').
 * @param table_name Name of the table to scan.
 * @param filter_col_name Name of the column to filter on.
 * @param filter_val_str The value to filter for (as a string).
 * @return Number of matching rows found, or -1 on error.
 */
int select_scan(const char* table_name, const char* filter_col_name, const char* filter_val_str) {
    printf("Executing Full Table Scan on %s WHERE %s = '%s'\n", table_name, filter_col_name, filter_val_str);

    // 1. Find Schema and Column Definition
    TableSchema* schema = find_table_schema(table_name);
    if (!schema) {
        fprintf(stderr, "Error: Table '%s' not found for scan.\n", table_name);
        return -1;
    }
    const ColumnDefinition* filter_col = find_column(schema, filter_col_name);
    if (!filter_col) {
        fprintf(stderr, "Error: Column '%s' not found in table '%s'.\n", filter_col_name, table_name);
        return -1;
    }

    // 2. Open Data File
    FILE *data_fp = fopen(schema->data_path, "rb");
    if (!data_fp) {
        fprintf(stderr, "Error opening data file '%s' for scanning: %s\n", schema->data_path, strerror(errno));
        return -1;
    }

    // 3. Allocate Row Buffer
    void* row_data = malloc(schema->row_size);
    if (!row_data) {
        perror("Error allocating memory for row buffer during scan");
        fclose(data_fp);
        return -1;
    }

    // 4. Scan Loop
    int found_count = 0;
    long current_offset = 0; // Keep track for potential debugging info
    size_t rows_read = 0;

    while (1) {
        // Read one row
        size_t read_count = fread(row_data, schema->row_size, 1, data_fp);

        if (read_count == 1) {
            rows_read++;
            // Get pointer to the specific field within the row buffer
            const void* field_ptr = (const char*)row_data + filter_col->offset;

            // Compare the value
            int match_result = compare_value(filter_col, field_ptr, filter_val_str);

            if (match_result == 1) {
                // Match found! Print the row.
                printf("Found Match at Offset ~%ld:\n", current_offset); // Offset is approximate start
                print_row(schema, row_data);
                found_count++;
            } else if (match_result == -1) {
                 // Error during comparison (e.g., bad filter value format)
                 fprintf(stderr, "Scan aborted due to comparison error.\n");
                 found_count = -1; // Signal error
                 break; // Stop scanning
            }
            // If match_result == 0, continue to next row

            current_offset += schema->row_size; // Update approximate offset

        } else {
            // fread returned 0 or less than 1
            if (feof(data_fp)) {
                // End Of File reached normally
                break;
            } else if (ferror(data_fp)) {
                // Actual read error occurred
                perror("Error reading from data file during scan");
                found_count = -1; // Signal error
                break;
            } else {
                // Short read (unexpected EOF?) - treat as error/end
                 fprintf(stderr, "Warning: Unexpected end of data file or short read after %zu rows.\n", rows_read);
                 break;
            }
        }
    } // End while loop

    // 5. Cleanup
    free(row_data);
    fclose(data_fp);

    return found_count; // Return number of matches found (or -1 on error)
}

/**
 * @brief Prints the content of a generic row buffer based on its schema.
 * @param schema Pointer to the table schema.
 * @param row_data Pointer to the raw row data buffer.
 */
void print_row(const TableSchema* schema, const void* row_data) {
    if (!schema || !row_data) return;

    const char* current_byte = (const char*)row_data;

    printf("  Row (size %zu bytes): {\n", schema->row_size);
    for (int i = 0; i < schema->num_columns; ++i) {
        const ColumnDefinition* col = &schema->columns[i];
        const void* field_ptr = current_byte + col->offset;

        printf("    %s (%s, size %zu): ", col->name,
               (col->type == COL_TYPE_INT ? "int" : "string"), col->size);

        if (col->type == COL_TYPE_INT) {
            int value;
            memcpy(&value, field_ptr, sizeof(int)); // Use memcpy for safety
            printf("%d", value);
        } else if (col->type == COL_TYPE_STRING) {
            // Print string, ensuring null termination within its allocated size
            // Create a temporary buffer to guarantee null termination for printing
            char* temp_str = malloc(col->size + 1);
            if(temp_str) {
                memcpy(temp_str, field_ptr, col->size);
                temp_str[col->size] = '\0'; // Ensure null termination
                printf("\"%s\"", temp_str);
                free(temp_str);
            } else {
                printf("[memory error]");
            }
            // Direct printing (less safe if data isn't null-terminated):
            // printf("\"%.*s\"", (int)col->size, (const char*)field_ptr);
        }
        // Add other types here
        // else if (col->type == COL_TYPE_FLOAT) { ... }

        if (col->is_primary_key) {
            printf(" [PK]");
        }
        printf("\n");
    }
    printf("  }\n");
}
