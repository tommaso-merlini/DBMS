#include <stdio.h>
#include "database.h"

// Extern declaration for btree_fp from btree.c
extern FILE *btree_fp;

/**
 * Main function with example usage.
 */
int main() {
    init_database();

    // Insert some rows
    insert_row(1, "Alice");
    insert_row(2, "Bob");
    insert_row(3, "Charlie");
    insert_row(4, "David");
    insert_row(5, "Eve");

    // Test lookups
    select_row(1);  // Should print "Found: id=1, name=Alice"
    select_row(2);  // Should print "Found: id=2, name=Bob"
    select_row(3);  // Should print "Found: id=3, name=Charlie"
    select_row(4);  // Should print "Found: id=4, name=David"
    select_row(5);  // Should print "Found: id=5, name=Eve"
    select_row(6);  // Should print "ID 6 not found"

    fclose(btree_fp);
    return 0;
}
