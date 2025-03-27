#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h> // For toupper, isspace
#include "database/database.h"

#define MAX_INPUT_LEN 512

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


// Helper to trim leading/trailing whitespace and trailing semicolon
char* trim_whitespace(char *str) {
    char *end;

    // Trim leading space
    while (isspace((unsigned char)*str)) str++;

    if (*str == 0) // All spaces?
        return str;

    // Trim trailing space
    end = str + strlen(str) - 1;
    while (end > str && isspace((unsigned char)*end)) end--;

    // Trim trailing semicolon if present
    if (end > str && *end == ';') end--;

    // Write new null terminator
    *(end + 1) = 0;

    return str;
}

// Helper to convert string value based on column type and set in buffer
// Returns 0 on success, -1 on error
int set_value_by_index(const TableSchema* schema, void* row_data, int col_index, const char* value_str) {
    if (!schema || !row_data || !value_str) return -1;
    if (col_index < 0 || col_index >= schema->num_columns) {
        fprintf(stderr, "Error: Invalid column index %d.\n", col_index);
        return -1;
    }

    const ColumnDefinition* col = &schema->columns[col_index];
    char* dest = (char*)row_data + col->offset;

     // Check bounds
     if (col->offset + col->size > schema->row_size) {
         fprintf(stderr, "Error: Column '%s' offset/size exceeds row size.\n", col->name);
         return -1;
     }

    if (col->type == COL_TYPE_INT) {
        // Use strtol for better error checking than atoi
        char *endptr;
        long val = strtol(value_str, &endptr, 10);
        // Check for conversion errors
        if (endptr == value_str || *endptr != '\0' || errno == ERANGE) {
             fprintf(stderr, "Error: Invalid integer value '%s' for column '%s'.\n", value_str, col->name);
             return -1;
        }
        int int_val = (int)val; // Assuming long fits in int for simplicity here
        memcpy(dest, &int_val, sizeof(int));

    } else if (col->type == COL_TYPE_STRING) {
        // Trim whitespace from the value string itself before copying
        char* trimmed_value = trim_whitespace((char*)value_str); // Cast needed as value_str is const
        size_t len = strlen(trimmed_value);

        if (len >= col->size) {
            fprintf(stderr, "Warning: String value '%.*s...' too long for column '%s' (max %zu chars). Truncating.\n",
                    15, trimmed_value, col->name, col->size -1);
            memcpy(dest, trimmed_value, col->size - 1);
            dest[col->size - 1] = '\0'; // Ensure null termination on truncation
        } else {
            memcpy(dest, trimmed_value, len + 1); // Copy including null terminator
            // Zero out remaining buffer space (optional, good practice)
            if (len + 1 < col->size) {
                memset(dest + len + 1, 0, col->size - (len + 1));
            }
        }
    } else {
         fprintf(stderr, "Error: Unsupported column type %d for column '%s'.\n", col->type, col->name);
         return -1;
    }
    return 0; // Success
}


// --- Command Handlers ---

// Handle INSERT INTO table VALUES (val1, val2, ...);
// Helper function to skip leading whitespace
char* skip_whitespace(char* str) {
    while (*str != '\0' && isspace((unsigned char)*str)) {
        str++;
    }
    return str;
}

// Handle INSERT INTO table VALUES (val1, val2, ...);
void handle_insert(char* original_input) {
    char input_copy[MAX_INPUT_LEN];
    strncpy(input_copy, original_input, MAX_INPUT_LEN - 1);
    input_copy[MAX_INPUT_LEN - 1] = '\0';

    char *cursor = input_copy; // Pointer to traverse the command
    char table_name_buf[MAX_TABLE_NAME_LEN] = {0};
    char *values_part = NULL;
    char *end_values = NULL;

    // --- Parse Manually ---

    // 1. Skip leading spaces & check "INSERT" (case-insensitive)
    cursor = skip_whitespace(cursor);
    if (strncasecmp(cursor, "INSERT ", 7) != 0) goto syntax_error;
    cursor += 6; // Move past "INSERT"
    cursor = skip_whitespace(cursor); // Skip space after INSERT

    // 2. Check "INTO"
    if (strncasecmp(cursor, "INTO ", 5) != 0) goto syntax_error;
    cursor += 4; // Move past "INTO"
    cursor = skip_whitespace(cursor); // Skip space after INTO

    // 3. Extract table_name
    char *table_name_start = cursor;
    while (*cursor != '\0' && !isspace((unsigned char)*cursor)) {
        cursor++; // Find end of table name (space or end of string)
    }
    size_t table_name_len = cursor - table_name_start;
    if (table_name_len == 0 || table_name_len >= MAX_TABLE_NAME_LEN) {
        fprintf(stderr, "Error: Invalid or missing table name.\n");
        goto syntax_error;
    }
    strncpy(table_name_buf, table_name_start, table_name_len);
    table_name_buf[table_name_len] = '\0'; // Null terminate the extracted name
    cursor = skip_whitespace(cursor); // Skip space after table name

    // 4. Check "VALUES"
    if (strncasecmp(cursor, "VALUES", 6) != 0) goto syntax_error;
    cursor += 6; // Move past "VALUES"
    cursor = skip_whitespace(cursor); // Skip space after VALUES

    // 5. Find '('
    if (*cursor != '(') goto syntax_error;
    values_part = cursor + 1; // Point just after '('

    // 6. Find matching ')' - more robustly
    end_values = strrchr(values_part, ')'); // Find the LAST ')'
    if (!end_values || end_values < values_part) goto syntax_error; // Must exist and be after '('
    // Check for nested parentheses if needed (simplification: assume none)

    // Temporarily null-terminate at ')' to isolate the values string for strtok
    *end_values = '\0';

    // --- End Manual Parse ---


    // 7. Find table schema (using extracted table_name_buf)
    TableSchema* schema = find_table_schema(table_name_buf);
    if (!schema) {
        fprintf(stderr, "Error: Table '%s' not found.\n", table_name_buf);
        return;
    }

    // 8. Allocate row buffer
    void* row_data = malloc(schema->row_size);
    if (!row_data) {
        perror("Error allocating memory for row data");
        return;
    }
    memset(row_data, 0, schema->row_size); // Zero out buffer

    // 9. Parse values and populate buffer (using strtok on the isolated values_part)
    char *value_token;
    int col_index = 0;
    value_token = strtok(values_part, ","); // First value

    while (value_token != NULL) {
        if (col_index >= schema->num_columns) {
            fprintf(stderr, "Error: Too many values provided for table '%s'. Expected %d.\n", table_name_buf, schema->num_columns);
            free(row_data);
            // Restore ')' before returning if necessary, though buffer is discarded
            *end_values = ')'; // Restore ')' - good practice
            return;
        }

        char* trimmed_val = trim_whitespace(value_token); // Trim each value

        if (strlen(trimmed_val) == 0 && col_index < (schema->num_columns -1) ) {
             fprintf(stderr, "Warning: Empty value encountered for column %d. Behavior undefined.\n", col_index);
        }

        if (set_value_by_index(schema, row_data, col_index, trimmed_val) != 0) {
            // Error message already printed by set_value_by_index
            free(row_data);
            *end_values = ')'; // Restore ')'
            return;
        }

        col_index++;
        value_token = strtok(NULL, ","); // Get next value
    }

    // Restore ')' in the input_copy string
     *end_values = ')';

    // 10. Check if enough values were provided
    if (col_index < schema->num_columns) {
        fprintf(stderr, "Error: Not enough values provided for table '%s'. Expected %d, got %d.\n", table_name_buf, schema->num_columns, col_index);
        free(row_data);
        return;
    }

    // 11. Insert the row
    int result = insert_row(table_name_buf, row_data); // Use extracted table name
    if (result == 0) {
        printf("Inserted 1 row into %s.\n", table_name_buf);
    } else if (result == 1) {
        printf("Insert failed: Duplicate primary key.\n");
    } else {
        printf("Insert failed (error code %d).\n", result);
    }

    // 12. Clean up
    free(row_data);
    return; // Success or handled error

syntax_error:
    fprintf(stderr, "Syntax error parsing INSERT statement. Check format near: %s\n", cursor);
    fprintf(stderr, "Expected: INSERT INTO table VALUES (val1, val2, ...);\n");
}

// Handle SELECT * FROM table WHERE pk_col = value;
void handle_select(char* original_input) { // Receive original buffer
    char input_copy[MAX_INPUT_LEN]; // Make a local copy for strtok
    strncpy(input_copy, original_input, MAX_INPUT_LEN - 1);
    input_copy[MAX_INPUT_LEN - 1] = '\0'; // Ensure null termination

    // Use input_copy for strtok calls
    char *token;
    char *table_name = NULL;
    char *where_col = NULL;
    char *where_val_str = NULL;

    // 1. Get "SELECT"
    token = strtok(input_copy, " \t\n");
    if (!token || strcasecmp(token, "SELECT") != 0) goto syntax_error;

    // 2. Get "*"
    token = strtok(NULL, " \t\n");
    if (!token || strcmp(token, "*") != 0) goto syntax_error; // Only '*' supported

    // 3. Get "FROM"
    token = strtok(NULL, " \t\n");
    if (!token || strcasecmp(token, "FROM") != 0) goto syntax_error;

    // 4. Get table_name
    table_name = strtok(NULL, " \t\n");
    if (!table_name) goto syntax_error;

    // 5. Get "WHERE"
    token = strtok(NULL, " \t\n");
    if (!token || strcasecmp(token, "WHERE") != 0) goto syntax_error;

    // --- Revised Steps 6 & 7 ---

    // 6. Get where_col - Delimit only by whitespace initially
    where_col = strtok(NULL, " \t\n");
    if (!where_col) goto syntax_error;
    // Remove trailing '=' if it got included (e.g., "id=")
    size_t len = strlen(where_col);
    if (len > 0 && where_col[len - 1] == '=') {
        where_col[len - 1] = '\0'; // Remove trailing '='
    }
    where_col = trim_whitespace(where_col); // Trim just in case


    // 7. Expect '=' next, possibly surrounded by spaces
    token = strtok(NULL, " \t\n"); // Get the next token after the column name
    if (!token) goto syntax_error; // Nothing after column name

    // Allow token to be just "=" or potentially part of "=value" if no spaces
    if (strcmp(token, "=") != 0) {
        // If token wasn't just '=', check if it starts with '=' (e.g., "=1")
        if (token[0] == '=') {
            // It was something like "=1", move past '=' to get value
            where_val_str = token + 1;
            if (strlen(where_val_str) == 0) goto syntax_error; // Case like "col==value" - invalid
        } else {
            // '=' wasn't the next token and didn't start the token
            goto syntax_error; // Missing '='
        }
    } else {
        // Token was exactly '=', the value is the *next* token
        where_val_str = strtok(NULL, " \t\n");
        if (!where_val_str) goto syntax_error; // Missing value after '='
    }
    // --- End Revised Steps ---


    // Trim whitespace and potential trailing semicolon from the final value string
    where_val_str = trim_whitespace(where_val_str);
    if (strlen(where_val_str) == 0) goto syntax_error; // Value cannot be empty


    // 8. Find table schema
    TableSchema* schema = find_table_schema(table_name);
    if (!schema) {
        fprintf(stderr, "Error: Table '%s' not found.\n", table_name);
        return;
    }

    // 9. Validate WHERE clause
    if (schema->pk_column_index == -1) {
        fprintf(stderr, "Error: Cannot perform WHERE clause select on table '%s' without a primary key.\n", table_name);
        return;
    }
    const ColumnDefinition* pk_col_def = &schema->columns[schema->pk_column_index];
    // Case-insensitive compare might be nicer, but stick to exact for now
    if (strcmp(where_col, pk_col_def->name) != 0) {
        fprintf(stderr, "Error: WHERE clause must use the primary key column ('%s'), found '%s'.\n", pk_col_def->name, where_col);
        return;
    }
    if (pk_col_def->type != COL_TYPE_INT) {
         fprintf(stderr, "Error: WHERE clause only supported for INT primary keys currently.\n");
         return;
    }

    // 10. Convert value string to integer
    char *endptr;
    errno = 0; // Reset errno before calling strtol
    long pk_val_long = strtol(where_val_str, &endptr, 10);
    // Check for conversion errors more thoroughly
    if (endptr == where_val_str || *endptr != '\0' || errno == ERANGE || (errno != 0 && pk_val_long == 0)) {
         fprintf(stderr, "Error: Invalid integer value '%s' in WHERE clause.\n", where_val_str);
         return;
    }
     // Check if value fits in int (optional but good practice)
     if (pk_val_long > INT_MAX || pk_val_long < INT_MIN) {
        fprintf(stderr, "Error: Integer value '%s' out of range for int.\n", where_val_str);
        return;
     }
    int pk_val = (int)pk_val_long;

    // 11. Execute select
    printf("Executing: SELECT * FROM %s WHERE %s = %d\n", table_name, pk_col_def->name, pk_val);
    int result = select_row(table_name, pk_val);
    if (result == 0) {
        // Row printed by select_row
        printf("1 row found.\n");
    } else if (result == 1) {
         // Message "Record not found" printed by select_row
         printf("0 rows found.\n");
    } else {
         // Error message should have been printed by select_row or its callees
         printf("Select failed (error code %d).\n", result);
    }
    return; // Success or handled error

syntax_error:
    // Provide a general syntax error message for SELECT
    fprintf(stderr, "Syntax error parsing SELECT statement. Expected: SELECT * FROM table WHERE pk_col = value;\n");
}

// --- Main Loop ---

int main() {
    printf("Starting Mini Database Engine...\n");

    // Initialize database
    if (init_database() != 0) {
        fprintf(stderr, "Database initialization failed. Exiting.\n");
        return 1;
    }
    printf("Database initialized. Enter SQL-like commands.\n");
    printf("Supported:\n");
    printf("  INSERT INTO table VALUES (val1, val2, ...);\n");
    printf("  SELECT * FROM table WHERE pk_col = value;\n");
    printf("  EXIT; or QUIT;\n");


    char input_buffer[MAX_INPUT_LEN];
    // No need for command_copy here if we pass input_buffer

    while (1) {
        printf("db> ");
        if (fgets(input_buffer, sizeof(input_buffer), stdin) == NULL) {
            printf("\nEOF detected. Exiting.\n");
            break;
        }

        // --- FIX: Make a temporary copy JUST for the first word check ---
        char temp_copy[MAX_INPUT_LEN];
        strncpy(temp_copy, input_buffer, MAX_INPUT_LEN - 1);
        temp_copy[MAX_INPUT_LEN - 1] = '\0'; // Ensure null termination

        char* command_trimmed = trim_whitespace(temp_copy); // Trim the temp copy

        if (strlen(command_trimmed) == 0) {
            continue; // Skip empty lines
        }

        // Check the first word from the trimmed temp copy
        char* first_word = strtok(command_trimmed, " \t\n");

        if (!first_word) {
             continue; // Should not happen, but safety check
        }

        // --- FIX: Pass the ORIGINAL, UNMODIFIED input_buffer to handlers ---
        if (strcasecmp(first_word, "EXIT") == 0 || strcasecmp(first_word, "QUIT") == 0) {
            printf("Exiting.\n");
            break; // Exit the loop
        } else if (strcasecmp(first_word, "INSERT") == 0) {
             handle_insert(input_buffer); // Pass original buffer
        } else if (strcasecmp(first_word, "SELECT") == 0) {
             handle_select(input_buffer); // Pass original buffer
        } else {
            fprintf(stderr, "Error: Unknown command '%s'.\n", first_word);
        }
        // No need to free command_copy anymore
    }

    // Shutdown database
    shutdown_database();

    return 0;
}
