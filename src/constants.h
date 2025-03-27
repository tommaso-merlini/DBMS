#ifndef CONSTANTS_H
#define CONSTANTS_H

// Constants
#define M 3             // Order of the B+ tree (max children)
#define HEADER_SIZE 32  // Fixed size for the file header
#define MAGIC 0x12345678 // Magic number to identify the file format
#define NAME_LEN 50      // Max length for name field NOTE: remove if unused
#define MAX_TABLE_NAME_LEN 64
#define MAX_COLUMN_NAME_LEN 64
#define MAX_COLUMNS 32 // Max columns per table
#define MAX_TABLES 16 // Max tables in the database

#define DATA_DIR "db_data"       // Root directory for database files
#define METADATA_FILE "metadata.dbm"
#define TABLE_DATA_EXT ".tbl"
#define PK_INDEX_EXT ".idx"
#define MAX_PATH_LEN 256

#endif
