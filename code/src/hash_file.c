#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hash_file.h"
#define MAX_OPEN_FILES 20

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HT_ERROR;        \
  }                         \
}

int hash_func(int x) {
    x = ((x >> (sizeof(int)/2)) ^ x) * 0x45d9f3b;
    x = ((x >> (sizeof(int)/2)) ^ x) * 0x45d9f3b;
    x = (x >> (sizeof(int)/2)) ^ x;
    return x;
}

typedef struct IndexBlock {
  int globalDepth;
  int nextBlock;
  int index[(BF_BLOCK_SIZE-2*sizeof(int))/sizeof(int)];
} IndexBlock;

typedef struct DataBlock {
  int localDepth;
  int lastEmpty;
  Record index[(BF_BLOCK_SIZE-2*sizeof(int))/sizeof(Record)];
} DataBlock;

int open_files[MAX_OPEN_FILES];

HT_ErrorCode HT_Init() {
  //insert code here
  return HT_OK;
}

HT_ErrorCode HT_CreateIndex(const char *filename, int depth) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){
  //insert code here
  return HT_OK;
}

HT_ErrorCode HT_CloseFile(int indexDesc) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
  //insert code here
  return HT_OK;
}

