#include <stdio.h>
#include <stdlib.h> // For malloc, free, atoi
#include <string.h>
#include <errno.h>  // For errno
#include "database.h"
#include "../btree/btree.h"
#include "../structs.h" // Includes constants.h

// --- Global Variables ---
FILE *metadata_fp, *data_fp; // Keep these if managing files here
extern Header header; // From btree.c

// Global Schema Storage
TableSchema database_schema[MAX_TABLES];
int num_tables = 0;

// --- Schema Management ---

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
 * @brief Loads table schemas from the metadata file.
 */
void load_schema() {
    metadata_fp = fopen("metadata.dat", "r");
    if (!metadata_fp) {
        perror("Error opening metadata.dat for reading");
        // Attempt to create a default one? Or just exit?
        // Let's try creating a default if it couldn't be read.
        metadata_fp = fopen("metadata.dat", "w+");
        if (!metadata_fp) {
             perror("Failed to create metadata.dat");
             exit(EXIT_FAILURE);
        }
        fprintf(metadata_fp, "table:users\ncolumn:id:int:primary_key\ncolumn:name:string:%d\n", NAME_LEN);
        fprintf(metadata_fp, "table:products\ncolumn:prod_id:int:primary_key\ncolumn:description:string:100\ncolumn:price:int\n"); // Example second table
        rewind(metadata_fp); // Go back to the beginning to read what we just wrote
    }

    char line[256];
    TableSchema* current_schema = NULL;
    size_t current_offset = 0;
    num_tables = 0;

    while (fgets(line, sizeof(line), metadata_fp)) {
        // Remove trailing newline
        line[strcspn(line, "\r\n")] = 0;

        // Skip empty lines or comments (optional)
        if (strlen(line) == 0 || line[0] == '#') {
            continue;
        }

        char* token;
        char* rest = line;

        token = strtok_r(rest, ":", &rest);
        if (!token) continue;

        if (strcmp(token, "table") == 0) {
            if (num_tables >= MAX_TABLES) {
                fprintf(stderr, "Error: Maximum number of tables (%d) reached.\n", MAX_TABLES);
                break; // Stop parsing
            }
            current_schema = &database_schema[num_tables++];
            memset(current_schema, 0, sizeof(TableSchema)); // Clear the schema struct
            current_schema->pk_column_index = -1; // Initialize PK index

            token = strtok_r(rest, ":", &rest); // Get table name
            if (!token) {
                 fprintf(stderr, "Error: Malformed 'table' line in metadata.dat\n");
                 num_tables--; // Decrement count as this table is invalid
                 current_schema = NULL;
                 continue;
            }
            strncpy(current_schema->name, token, MAX_TABLE_NAME_LEN - 1);
            current_schema->name[MAX_TABLE_NAME_LEN - 1] = '\0';
            current_schema->num_columns = 0;
            current_schema->row_size = 0;
            current_offset = 0; // Reset offset for the new table
            printf("Loading schema for table: %s\n", current_schema->name);

        } else if (strcmp(token, "column") == 0) {
            if (!current_schema) {
                fprintf(stderr, "Error: 'column' definition found before 'table' in metadata.dat\n");
                continue;
            }
            if (current_schema->num_columns >= MAX_COLUMNS) {
                 fprintf(stderr, "Error: Maximum number of columns (%d) reached for table '%s'.\n", MAX_COLUMNS, current_schema->name);
                 continue; // Skip this column
            }

            ColumnDefinition* col = &(current_schema->columns[current_schema->num_columns]);
            memset(col, 0, sizeof(ColumnDefinition));

            // Parse column: name:type:size_or_flag:[flag]
            char* col_name = strtok_r(rest, ":", &rest);
            char* col_type = strtok_r(rest, ":", &rest);
            char* col_arg = strtok_r(rest, ":", &rest); // Size or primary_key
            char* col_flag = strtok_r(rest, ":", &rest); // Optional primary_key if size specified

            if (!col_name || !col_type || !col_arg) {
                fprintf(stderr, "Error: Malformed 'column' line for table '%s'\n", current_schema->name);
                continue;
            }

            // Column Name
            strncpy(col->name, col_name, MAX_COLUMN_NAME_LEN - 1);
            col->name[MAX_COLUMN_NAME_LEN - 1] = '\0';
            col->offset = current_offset; // Set offset *before* calculating size

            // Column Type and Size
            col->is_primary_key = 0; // Default
            int is_pk = 0;

            if (strcmp(col_type, "int") == 0) {
                col->type = COL_TYPE_INT;
                col->size = sizeof(int);
                if (col_arg && strcmp(col_arg, "primary_key") == 0) is_pk = 1;
            } else if (strcmp(col_type, "string") == 0) {
                col->type = COL_TYPE_STRING;
                col->size = atoi(col_arg); // Size must be provided for string
                if (col->size <= 0 || col->size > 1024*10) { // Add reasonable limits
                    fprintf(stderr, "Warning: Invalid size %ld for string column '%s' in table '%s'. Defaulting to %d.\n", col->size, col->name, current_schema->name, NAME_LEN);
                    col->size = NAME_LEN;
                }
                if (col_flag && strcmp(col_flag, "primary_key") == 0) is_pk = 1; // PK flag is optional 4th part for string
            }
            // Add other types here (e.g., float)
            // else if (strcmp(col_type, "float") == 0) { ... }
            else {
                fprintf(stderr, "Error: Unknown column type '%s' for column '%s' in table '%s'\n", col_type, col->name, current_schema->name);
                continue; // Skip this column
            }

            // Check for primary key
            if (is_pk) {
                if (current_schema->pk_column_index != -1) {
                    fprintf(stderr, "Error: Multiple primary keys defined for table '%s'. Ignoring PK flag for column '%s'.\n", current_schema->name, col->name);
                } else if (col->type != COL_TYPE_INT) {
                     fprintf(stderr, "Error: Primary key column '%s' in table '%s' is not of type INT. Current B+ Tree only supports INT keys. Ignoring PK flag.\n", col->name, current_schema->name);
                } else {
                    col->is_primary_key = 1;
                    current_schema->pk_column_index = current_schema->num_columns;
                }
            }

            // Update offsets and sizes
            current_offset += col->size;
            current_schema->row_size = current_offset; // Update total row size
            current_schema->num_columns++;
             printf("  -> Column: %s, Type: %d, Size: %zu, Offset: %zu, PK: %d\n", col->name, col->type, col->size, col->offset, col->is_primary_key);

        } else {
            fprintf(stderr, "Warning: Unrecognized line type '%s' in metadata.dat\n", token);
        }
    }

    fclose(metadata_fp);

    // Final validation: Ensure every table has a primary key (as B+ Tree needs one)
    for(int i=0; i < num_tables; ++i) {
        if (database_schema[i].pk_column_index == -1) {
             fprintf(stderr, "Error: Table '%s' does not have an INT primary_key defined. Indexing will not work correctly.\n", database_schema[i].name);
             // Decide how to handle this - maybe remove the table? For now, just warn.
        }
    }

    printf("Schema loading complete. %d table(s) loaded.\n", num_tables);
}


// --- Database Initialization ---

/**
 * Initialize the database files and load the schema.
 */
void init_database() {
    // Load schema first - it might create metadata.dat if it doesn't exist
    load_schema();

    // Data file - ensure it exists
    data_fp = fopen("data.dat", "r+b");
    if (!data_fp) {
        data_fp = fopen("data.dat", "w+b");
        if (!data_fp) {
            perror("Failed to create data.dat");
            exit(EXIT_FAILURE);
        }
    }
    fclose(data_fp); // Close for now, reopen as needed

    // B+ tree
    init_btree(); // Assumes btree.dat setup
}

// --- Row Operations (Generic) ---

/**
 * Append a generic row buffer to the data file and return its offset.
 * @param schema Pointer to the table schema.
 * @param row_data Pointer to the raw row data buffer.
 * @return Offset where the row is written, or -1 on error.
 */
long append_row_to_file(const TableSchema* schema, const void* row_data) {
    if (!schema || !row_data) {
        fprintf(stderr, "Error: Cannot append row with NULL schema or data.\n");
        return -1;
    }

    data_fp = fopen("data.dat", "r+b"); // Use "ab" for append-only binary? "r+b" allows seeking later maybe
    if (!data_fp) {
        perror("Error opening data.dat for appending");
        return -1;
    }

    // Seek to end isn't strictly necessary with "ab", but good practice with "r+b"
    if (fseek(data_fp, 0, SEEK_END) != 0) {
         perror("Error seeking to end of data.dat");
         fclose(data_fp);
         return -1;
    }

    long offset = ftell(data_fp);
    if (offset == -1) {
        perror("Error getting file position in data.dat");
        fclose(data_fp);
        return -1;
    }

    size_t written = fwrite(row_data, schema->row_size, 1, data_fp);
    fflush(data_fp); // Ensure data is written to disk
    fclose(data_fp);

    if (written != 1) {
        fprintf(stderr, "Error writing row data to data.dat (expected %zu bytes)\n", schema->row_size);
        // Should we attempt to truncate the file back? Complex.
        return -1;
    }

    return offset;
}

/**
 * Extracts the integer primary key value from a raw row buffer.
 * @param schema Pointer to the table schema.
 * @param row_data Pointer to the raw row data buffer.
 * @return The integer primary key value.
 * @note Exits fatally if PK is not found, not int, or data is NULL.
 */
int get_int_pk_value(const TableSchema* schema, const void* row_data) {
     if (!schema || !row_data) {
        fprintf(stderr, "FATAL: Cannot get PK value from NULL schema or data.\n");
        exit(EXIT_FAILURE);
    }
     if (schema->pk_column_index < 0 || schema->pk_column_index >= schema->num_columns) {
        fprintf(stderr, "FATAL: Table '%s' has no valid primary key index defined.\n", schema->name);
        exit(EXIT_FAILURE);
     }
     const ColumnDefinition* pk_col = &schema->columns[schema->pk_column_index];
     if (pk_col->type != COL_TYPE_INT) {
         fprintf(stderr, "FATAL: Primary key '%s' for table '%s' is not type INT.\n", pk_col->name, schema->name);
         exit(EXIT_FAILURE);
     }
     // Calculate address of PK field within the buffer and cast to int*
     int pk_value;
     memcpy(&pk_value, (const char*)row_data + pk_col->offset, sizeof(int));
     return pk_value;
}

/**
 * Insert a generic row into the database.
 * @param table_name Name of the table to insert into.
 * @param row_data Pointer to the raw row data buffer.
 */
void insert_row(const char* table_name, const void* row_data) {
    TableSchema* schema = find_table_schema(table_name);
    if (!schema) {
        fprintf(stderr, "Error: Table '%s' not found.\n", table_name);
        return;
    }
     if (schema->pk_column_index == -1) {
         fprintf(stderr, "Error: Cannot insert into table '%s' without an INT primary key.\n", table_name);
         return;
     }

    // Extract primary key (must be int for current B+ Tree)
    int pk_value = get_int_pk_value(schema, row_data);

    // Check for duplicates using B+ Tree
    long existing_offset = search(pk_value, header.root_id); // search is from btree.c
    if (existing_offset != -1) {
        fprintf(stderr, "Error: Duplicate primary key value %d in table '%s'.\n", pk_value, table_name);
        return;
    }

    // Append row data to data file
    long offset = append_row_to_file(schema, row_data);
    if (offset == -1) {
        fprintf(stderr, "Error: Failed to append row data for table '%s'.\n", table_name);
        return; // Don't insert into index if data write failed
    }

    // Insert key (PK value) and offset into B+ Tree
    btree_insert(pk_value, offset); // btree_insert is from btree.c

    printf("Inserted into %s: PK=%d at offset=%ld\n", table_name, pk_value, offset);
}

/**
 * Select and display a row by its primary key value.
 * @param table_name Name of the table.
 * @param primary_key_value The integer primary key value to find.
 */
void select_row(const char* table_name, int primary_key_value) {
    TableSchema* schema = find_table_schema(table_name);
    if (!schema) {
        fprintf(stderr, "Error: Table '%s' not found.\n", table_name);
        return;
    }
     if (schema->pk_column_index == -1) {
         fprintf(stderr, "Error: Cannot select from table '%s' without an INT primary key.\n", table_name);
         return;
     }

    // Search B+ Tree for the offset
    long offset = search(primary_key_value, header.root_id);
    if (offset == -1) {
        printf("Record with PK %d not found in table '%s'\n", primary_key_value, table_name);
        return;
    }

    // Open data file and seek to the offset
    data_fp = fopen("data.dat", "rb");
    if (!data_fp) {
        perror("Error opening data.dat for reading");
        return;
    }

    if (fseek(data_fp, offset, SEEK_SET) != 0) {
        perror("Error seeking in data.dat");
        fclose(data_fp);
        return;
    }

    // Allocate buffer to read the row data
    void* row_data = malloc(schema->row_size);
    if (!row_data) {
        fprintf(stderr, "Error: Failed to allocate memory (%zu bytes) for row buffer.\n", schema->row_size);
        fclose(data_fp);
        return;
    }

    // Read the row data
    size_t read_count = fread(row_data, schema->row_size, 1, data_fp);
    fclose(data_fp);

    if (read_count != 1) {
        // Check for EOF vs other errors
        if (feof(data_fp)) {
             fprintf(stderr, "Error: Unexpected end of file while reading row at offset %ld (expected %zu bytes).\n", offset, schema->row_size);
        } else if(ferror(data_fp)) {
            perror("Error reading row data from data.dat");
        } else {
             fprintf(stderr, "Error: Failed to read row at offset %ld (read_count=%zu, expected 1 of size %zu).\n", offset, read_count, schema->row_size);
        }
        free(row_data);
        return;
    }

    // Print the found row using the generic print function
    printf("Found in %s (PK=%d):\n", table_name, primary_key_value);
    print_row(schema, row_data);

    // Clean up
    free(row_data);
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

// --- B+ Tree code (btree.c) remains unchanged for now ---
// It still uses int keys and assumes the PK passed to it is int.
