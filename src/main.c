#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "database/database.h" // Check include paths

// Helper function to set a field value in a generic row buffer
// NOTE: Add more robust error checking as needed.
void set_field(const TableSchema* schema, void* row_data, const char* col_name, const void* value) {
    // Find the column definition using the function from database.h/database.c
    // Ensure database.h is included and find_column is declared there.
    const ColumnDefinition* col = find_column(schema, col_name);
    if (!col) {
        fprintf(stderr, "Error: Column '%s' not found in table '%s' for set_field.\n", col_name, schema->name);
        return; // Or handle error differently
    }
    if (!row_data) {
         fprintf(stderr, "Error: row_data buffer is NULL in set_field for column '%s'.\n", col_name);
         return;
    }


    // Calculate the destination address within the row buffer
    char* dest = (char*)row_data + col->offset;

    // Check if offset is valid within the row size (basic bounds check)
     if (col->offset + col->size > schema->row_size) {
         fprintf(stderr, "Error: Column '%s' offset/size exceeds row size in set_field.\n", col_name);
         return;
     }


    // Copy data based on type
    if (col->type == COL_TYPE_INT) {
        if (value) { // Check if value pointer is valid
           memcpy(dest, value, sizeof(int));
        } else {
            fprintf(stderr, "Warning: NULL value provided for INT column '%s'. Setting to 0.\n", col_name);
             int zero = 0;
             memcpy(dest, &zero, sizeof(int));
        }
    } else if (col->type == COL_TYPE_STRING) {
        if (value) { // Check if value pointer is valid
            // Copy string, ensuring fixed size and potential null padding/truncation
            strncpy(dest, (const char*)value, col->size);

            // Ensure null termination ONLY if the source string fits entirely AND there's space
            size_t src_len = strlen((const char*)value);
            if (src_len < col->size) {
                // Zero out the rest of the buffer for fixed-width fields / padding
                memset(dest + src_len, 0, col->size - src_len);
            } else if (col->size > 0) {
                 // If src is >= size, strncpy might not have null-terminated.
                 // Force null termination at the last byte for safety, potentially overwriting data.
                 dest[col->size - 1] = '\0';
            }
        } else {
             fprintf(stderr, "Warning: NULL value provided for STRING column '%s'. Setting to empty string.\n", col_name);
             if(col->size > 0) {
                 dest[0] = '\0'; // Start with null terminator
                 // Optionally zero out the rest
                 memset(dest + 1, 0, col->size - 1);
             }
        }
    }
    // Add other types here (e.g., float)
    // else if (col->type == COL_TYPE_FLOAT) { ... }
    else {
        fprintf(stderr, "Warning: set_field does not support type %d for column '%s'.\n", col->type, col_name);
    }
}


int main() {
    printf("Starting Database Engine...\n");

    // Initialize database (creates dirs, loads schema, opens indexes)
    if (init_database() != 0) {
        fprintf(stderr, "Database initialization failed. Exiting.\n");
        return 1;
    }

    printf("Database initialized successfully.\n\n");

    // --- Example Usage (Users Table) ---
    const char* users_table = "users";
    TableSchema* users_schema = find_table_schema(users_table);

    if (users_schema) {
        printf("--- Working with table: %s ---\n", users_table);
        void* user_row = malloc(users_schema->row_size);
        if (!user_row) { shutdown_database(); return 1;}

        // Insert User 1
        memset(user_row, 0, users_schema->row_size);
        int id1 = 101; char name1[] = "Alice";
        set_field(users_schema, user_row, "id", &id1);
        set_field(users_schema, user_row, "name", name1);
        printf("Attempting insert user 1 (PK=%d)...\n", id1);
        insert_row(users_table, user_row);

        // Insert User 2
        memset(user_row, 0, users_schema->row_size);
        int id2 = 102; char name2[] = "Bob";
        set_field(users_schema, user_row, "id", &id2);
        set_field(users_schema, user_row, "name", name2);
        printf("Attempting insert user 2 (PK=%d)...\n", id2);
        insert_row(users_table, user_row);

         // Insert User 3 (Duplicate)
        memset(user_row, 0, users_schema->row_size);
        int id3 = 101; char name3[] = "Alice Duplicate";
        set_field(users_schema, user_row, "id", &id3);
        set_field(users_schema, user_row, "name", name3);
        printf("Attempting insert user 3 (PK=%d - Duplicate)...\n", id3);
        insert_row(users_table, user_row);


        // Select User 1
        printf("\nSelecting user with ID %d:\n", id1);
        select_row(users_table, id1);

        // Select User 2
        printf("\nSelecting user with ID %d:\n", id2);
        select_row(users_table, id2);

        // Select Non-existent User
        printf("\nSelecting user with ID %d:\n", 999);
        select_row(users_table, 999);

        free(user_row);
        printf("--- Finished with table: %s ---\n\n", users_table);
    } else {
        printf("Schema for table '%s' not found.\n", users_table);
    }

    // --- Example Usage (Products Table) ---
    // ... (similar code for products) ...
    const char* products_table = "products";
    TableSchema* products_schema = find_table_schema(products_table);
    if (products_schema) {
        // ... allocate row, set fields, insert, select ...
        printf("--- Working with table: %s ---\n", products_table);
        void* product_row = malloc(products_schema->row_size);
        if (!product_row) { shutdown_database(); return 1;}

        memset(product_row, 0, products_schema->row_size);
        int pid1 = 5001; char pdesc1[]="Wrench"; int pprice1=20;
        set_field(products_schema, product_row, "prod_id", &pid1);
        set_field(products_schema, product_row, "description", pdesc1);
        set_field(products_schema, product_row, "price", &pprice1);
        printf("Attempting insert product 1 (PK=%d)...\n", pid1);
        insert_row(products_table, product_row);

        printf("\nSelecting product with ID %d:\n", pid1);
        select_row(products_table, pid1);

        free(product_row);
         printf("--- Finished with table: %s ---\n\n", products_table);
    } else {
         printf("Schema for table '%s' not found.\n", products_table);
    }


    // Shutdown database (closes files, frees handles)
    shutdown_database();

    return 0;
}
