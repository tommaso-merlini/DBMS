#ifndef DATABASE_H
#define DATABASE_H

void init_database(void);
long append_row_to_file(int id, char* name);
void insert_row(int id, char* name);
void select_row(int id);

#endif
