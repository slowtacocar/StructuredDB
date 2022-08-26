#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>

enum type {
    INT,
    ID,
    STRING,
    FIXED_STRING,
    BOOL,
};

#define cast(type, value) ( \
    (type) == ID ? *(size_t *) (value) : \
    (type) == INT ? *(int *) (value) :   \
    (type) == STRING ? *(char **) (value) :  \
    (type) == FIXED_STRING ? (char *) (value) : \
    (type) == BOOL ? *(bool *) (value) : 0)
#define formatter(type) (type) == ID ? "%zu" : (type) == INT ? "%d" : (type) == STRING ? "%s" : (type) == FIXED_STRING ? "%s" : (type) == BOOL ? "%d" : 0
#define hashType(type, value) ((type) == INT ? *(unsigned int *) (value) : (type) == STRING ? hashString(*(char **) (value)) : (type) == FIXED_STRING ? hashString((char *) (value)) : (type) == BOOL ? *(unsigned int *) (value) : 0)
#define hashCompareType(type, value) ((type) == INT ? *(unsigned int *) (value) : (type) == STRING ? hashString((char *) (value)) : (type) == FIXED_STRING ? hashString((char *) (value)) : (type) == BOOL ? *(unsigned int *) (value) : 0)
#define compareType(type, value1, value2) ((type) == INT ? *(int *) (value1) == *(int *) (value2) : (type) == STRING ? strcmp((char *) (value1), *(char **) (value2)) == 0 : (type) == FIXED_STRING ? strcmp((char *) (value1), (char *) (value2)) == 0 : (type) == BOOL ? *(bool *) (value1) == *(bool *) (value2) : 0)

struct ColumnDef {
    char *name;
    enum type type;
    size_t size;
    bool index;
};

struct InsertDef {
    char *columnName;
    void *value;
};

struct HashTable {
    void **entries;
    size_t numRows;
};

struct Column {
    size_t start;
    size_t size;
    char *name;
    enum type type;
    struct HashTable index;
};

struct Table {
    void *entries;
    size_t numRows;
    size_t allocatedRows;
    size_t rowSize;
    struct HashTable colLocs;
    size_t numColumns;
    struct Column **columnDefs;
};

unsigned int powi(unsigned int a, unsigned int b) {
    unsigned int ret = 1;
    for (unsigned int i = 0; i < b; i++) {
        ret *= a;
    }
    return ret;
}

unsigned int hashString(const char *string) {
    unsigned int ret = 0;
    for (size_t i = 0; string[i] != '\0'; i++) {
        ret += string[i] * powi(31, i);
    }
    return ret;
}

struct HashTable newHashTable(size_t size) {
    struct HashTable ret = {
            .entries = malloc(sizeof(void *) * size * 2),
            .numRows = size,
    };
    return ret;
}

void *getByKey(const char *key, struct HashTable *table) {
    unsigned int hash = hashString(key) % table->numRows;
    while (table->entries[hash * 2] != NULL) {
        if (strcmp(key, table->entries[hash * 2]) == 0) {
            return table->entries[hash * 2 + 1];
        }
        hash = (hash + 1) % table->numRows;
    }
    return NULL;
}

unsigned int countByKey(enum type type, void *key, struct HashTable *table) {
    unsigned int hash = hashCompareType(type, key) % table->numRows;
    unsigned int count = 0;
    while (table->entries[hash * 2] != NULL) {
        if (compareType(type, key, table->entries[hash * 2])) {
            count++;
        }
        hash = (hash + 1) % table->numRows;
    }
    return count;
}

void putKey(unsigned int h, void *key, void *value, struct HashTable *table) {
    unsigned int hash = h % table->numRows;
    while (table->entries[hash * 2] != NULL) {
        hash = (hash + 1) % table->numRows;
    }
    table->entries[hash * 2] = key;
    table->entries[hash * 2 + 1] = value;
}

struct Table newTable(struct ColumnDef columns[], int numColumns) {
    size_t rowSize = sizeof(size_t);
    struct HashTable colLocs = newHashTable(64);
    struct Column **columnDefs = malloc(sizeof(struct Column *) * numColumns);
    struct Column *id = malloc(sizeof(struct Column));
    id->start = 0;
    id->size = sizeof(size_t);
    id->name = "id";
    id->type = ID;
    putKey(hashString("id"), "id", (void *) id, &colLocs);
    for (int i = 0; i < numColumns; i++) {
        struct Column *col = malloc(sizeof(struct Column));
        col->start = rowSize;
        col->size = columns[i].size;
        col->name = columns[i].name;
        col->type = columns[i].type;
        if (columns[i].index) {
            col->index = newHashTable(64);
        }
        putKey(hashString(columns[i].name), columns[i].name, (void *) col, &colLocs);
        rowSize += col->size;
        columnDefs[i] = col;
    }
    struct Table ret = {
            .entries = malloc(rowSize * 128),
            .numRows = 0,
            .allocatedRows = 128,
            .rowSize = rowSize,
            .colLocs = colLocs,
            .numColumns = numColumns,
            .columnDefs = columnDefs,
    };
    return ret;
}

void insert(struct Table *table, struct InsertDef inserts[], int numInserts) {
    void *row = table->entries + table->rowSize * table->numRows++;
    if (table->numRows >= table->allocatedRows) {
        table->entries = realloc(table->entries, table->rowSize * (table->allocatedRows * 2));
        table->allocatedRows *= 2;
    }
    *(size_t *) row = table->numRows - 1;
    for (int i = 0; i < numInserts; i++) {
        struct Column *col = getByKey(inserts[i].columnName, &table->colLocs);
        memcpy(row + col->start, inserts[i].value, col->size);
        if (col->index.entries != NULL) {
            putKey(hashType(col->type, inserts[i].value), inserts[i].value, row, &col->index);
        }
    }
}

struct Table select(struct Table *table, char *columnNames[], int numColumns) {
    struct Column cols[numColumns];
    struct Column **selectDefs = malloc(sizeof(struct Column *) * numColumns);
    struct HashTable colLocs = newHashTable(64);
    size_t selectSize = 0;
    for (int i = 0; i < numColumns; i++) {
        struct Column *col = getByKey(columnNames[i], &table->colLocs);
        struct Column *selectCol = malloc(sizeof(struct Column));
        selectCol->start = selectSize;
        selectCol->size = col->size;
        selectCol->name = col->name;
        selectCol->type = col->type;
        cols[i] = *col;
        selectSize += selectCol->size;
        selectDefs[i] = selectCol;
        putKey(hashString(columnNames[i]), columnNames[i], (void *) selectCol, &colLocs);
    }
    void *ret = malloc(selectSize * table->numRows);
    for (int i = 0; i < table->numRows; i++) {
        void *row = table->entries + table->rowSize * i;
        void *selectRow = ret + selectSize * i;
        for (int j = 0; j < numColumns; j++) {
            memcpy(selectRow + selectDefs[j]->start, row + cols[j].start, cols[j].size);
        }
    }
    struct Table retTable = {
            .entries = ret,
            .numRows = table->numRows,
            .allocatedRows = table->numRows,
            .rowSize = selectSize,
            .colLocs = colLocs,
            .columnDefs = selectDefs,
            .numColumns = numColumns,
    };
    return retTable;
}

struct Table selectByID(struct Table *table, char *columnNames[], int numColumns, size_t id) {
    struct Column cols[numColumns];
    struct Column **selectDefs = malloc(sizeof(struct Column *) * numColumns);
    struct HashTable colLocs = newHashTable(64);
    size_t selectSize = 0;
    for (int i = 0; i < numColumns; i++) {
        struct Column *col = getByKey(columnNames[i], &table->colLocs);
        struct Column *selectCol = malloc(sizeof(struct Column));
        selectCol->start = selectSize;
        selectCol->size = col->size;
        selectCol->name = col->name;
        selectCol->type = col->type;
        cols[i] = *col;
        selectSize += selectCol->size;
        selectDefs[i] = selectCol;
        putKey(hashString(columnNames[i]), columnNames[i], (void *) selectCol, &colLocs);
    }
    void *ret = malloc(selectSize);
    void *row = table->entries + table->rowSize * id;
    for (int j = 0; j < numColumns; j++) {
        memcpy(ret + selectDefs[j]->start, row + cols[j].start, cols[j].size);
    }
    struct Table retTable = {
            .entries = ret,
            .numRows = 1,
            .allocatedRows = 1,
            .rowSize = selectSize,
            .colLocs = colLocs,
            .columnDefs = selectDefs,
            .numColumns = numColumns,
    };
    return retTable;
}

struct Table selectWhere(struct Table *table, char *columnNames[], int numColumns, char *whereColumn, void *value) {
    struct Column cols[numColumns];
    struct Column **selectDefs = malloc(sizeof(struct Column *) * numColumns);
    struct HashTable colLocs = newHashTable(64);
    size_t selectSize = 0;
    for (int i = 0; i < numColumns; i++) {
        struct Column *col = getByKey(columnNames[i], &table->colLocs);
        struct Column *selectCol = malloc(sizeof(struct Column));
        selectCol->start = selectSize;
        selectCol->size = col->size;
        selectCol->name = col->name;
        selectCol->type = col->type;
        cols[i] = *col;
        selectSize += selectCol->size;
        selectDefs[i] = selectCol;
        putKey(hashString(columnNames[i]), columnNames[i], (void *) selectCol, &colLocs);
    }
    struct Column *whereDef = getByKey(whereColumn, &table->colLocs);
    unsigned int count = countByKey(whereDef->type, value, &whereDef->index);
    void *ret = malloc(selectSize * count);
    unsigned int hash = hashCompareType(whereDef->type, value) % whereDef->index.numRows;
    unsigned int k = 0;
    while (whereDef->index.entries[hash * 2] != NULL) {
        if (compareType(whereDef->type, value, whereDef->index.entries[hash * 2])) {
            void *selectRow = ret + selectSize * k;
            for (int j = 0; j < numColumns; j++) {
                memcpy(selectRow + selectDefs[j]->start, whereDef->index.entries[hash * 2 + 1] + cols[j].start, cols[j].size);
            }
            k++;
        }
        hash = (hash + 1) % whereDef->index.numRows;
    }
    struct Table retTable = {
            .entries = ret,
            .numRows = count,
            .allocatedRows = count,
            .rowSize = selectSize,
            .colLocs = colLocs,
            .columnDefs = selectDefs,
            .numColumns = numColumns,
    };
    return retTable;
}

void printTable(struct Table *table) {
    for (int i = 0; i < table->numColumns; i++) {
        printf("%s\t", table->columnDefs[i]->name);
    }
    printf("\n");
    for (int i = 0; i < table->numRows; i++) {
        void *row = table->entries + table->rowSize * i;
        for (int j = 0; j < table->numColumns; j++) {
            printf(formatter(table->columnDefs[j]->type), cast(table->columnDefs[j]->type, row + table->columnDefs[j]->start));
            printf("\t");
        }
        printf("\n");
    }
}

int main() {
    struct ColumnDef columns[] = {
            {.name = "name", .type = STRING, .size = sizeof(char *), .index = true},
            {.name = "age", .type = INT, .size = sizeof(int), .index = false},
    };
    struct Table table = newTable(columns, 2);
    char *name = "John Doe";
    struct InsertDef inserts[] = {
            {.columnName = "name", .value = &name},
            {.columnName = "age", .value = &(int) {42}},
    };
    insert(&table, inserts, 2);
    char *name2 = "Jane Doe";
    struct InsertDef inserts2[] = {
            {.columnName = "name", .value = &name2},
            {.columnName = "age", .value = &(int) {43}},
    };
    insert(&table, inserts2, 2);
    char *name3 = "John Smith";
    struct InsertDef inserts3[] = {
            {.columnName = "name", .value = &name3},
            {.columnName = "age", .value = &(int) {44}},
    };
    insert(&table, inserts3, 2);
    char *name4 = "Jane Smith";
    struct InsertDef inserts4[] = {
            {.columnName = "name", .value = &name4},
            {.columnName = "age", .value = &(int) {45}},
    };
    insert(&table, inserts4, 2);
    char *name5 = "John Doe";
    struct InsertDef inserts5[] = {
            {.columnName = "name", .value = &name5},
            {.columnName = "age", .value = &(int) {46}},
    };
    insert(&table, inserts5, 2);

    printf("All entries:\n");
    struct Table sel1 = select(&table, (char *[]) {"id", "name", "age"}, 3);
    printTable(&sel1);

    printf("\nID = 3:\n");
    struct Table sel2 = selectByID(&table, (char *[]) {"id", "name", "age"}, 3, 3);
    printTable(&sel2);

    printf("\nname = John Doe\n");
    struct Table sel3 = selectWhere(&table, (char *[]) {"id", "name", "age"}, 3, "name", "John Doe");
    printTable(&sel3);
}
