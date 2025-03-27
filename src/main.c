#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "database/database.h" // Adjust path as needed

// Helper function to set a field value in a generic row buffer
// NOTE: Error checking omitted for brevity. Add checks for col != NULL, etc.
void set_field(const TableSchema* schema, void* row_data, const char* col_name, const void* value) {
    const ColumnDefinition* col = find_column(schema, col_name);
    if (!col) {
        fprintf(stderr, "Error: Column '%s' not found in table '%s' for set_field.\n", col_name, schema->name);
        return;
    }

    char* dest = (char*)row_data + col->offset;

    if (col->type == COL_TYPE_INT) {
        memcpy(dest, value, sizeof(int));
    } else if (col->type == COL_TYPE_STRING) {
        // Copy string, ensuring fixed size and potential null padding/truncation
        strncpy(dest, (const char*)value, col->size);
        // Optional: Ensure null termination within the buffer if space allows
        if (strlen((const char*)value) < col->size) {
             // If the source string is shorter, the rest is garbage from strncpy
             // We might want to zero out the rest of the buffer for consistency
             // Or ensure at least one null byte if possible. Let's zero pad.
             memset(dest + strlen((const char*)value), 0, col->size - strlen((const char*)value));
        } else {
            // If source is exactly col->size or longer, strncpy doesn't null terminate.
            // The fixed-size buffer is full. This is okay for fixed-width strings.
            // If we *require* null termination, the size in metadata should be N+1.
            // For now, we assume fixed width, print_row handles adding termination for display.
        }
    }
    // Add other types here
}

int main() {
    printf("Initializing database...\n");
    init_database();
    printf("Database initialized.\n\n");

    // --- Example: Users Table ---
    const char* users_table = "users";
    TableSchema* users_schema = find_table_schema(users_table);

    if (users_schema) {
        printf("--- Working with table: %s ---\n", users_table);
        // Allocate buffer for a 'users' row
        void* user_row = malloc(users_schema->row_size);
        if (!user_row) return 1; // Allocation failed

        // Insert User 1
        memset(user_row, 0, users_schema->row_size); // Good practice to zero out
        int id1 = 101;
        char name1[] = "Alice";
        set_field(users_schema, user_row, "id", &id1);
        set_field(users_schema, user_row, "name", name1);
        printf("Attempting insert user 1:\n");
        print_row(users_schema, user_row);
        insert_row(users_table, user_row);

        // Insert User 2
        memset(user_row, 0, users_schema->row_size);
        int id2 = 102;
        char name2[] = "Bob The Builder"; // Longer name, will be truncated by set_field if needed
        set_field(users_schema, user_row, "id", &id2);
        set_field(users_schema, user_row, "name", name2);
        printf("Attempting insert user 2:\n");
        print_row(users_schema, user_row);
        insert_row(users_table, user_row);

        // Insert User 3 (Duplicate ID)
        memset(user_row, 0, users_schema->row_size);
        int id3 = 101;
        char name3[] = "Charlie";
        set_field(users_schema, user_row, "id", &id3);
        set_field(users_schema, user_row, "name", name3);
        printf("Attempting insert user 3 (duplicate ID):\n");
        print_row(users_schema, user_row);
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

    printf("Cleanup/Shutdown potentially needed here.\n");
    // You might want a shutdown_database() function to ensure files are closed if kept open.

    return 0;
}
