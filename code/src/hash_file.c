#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hash_file.h"
#define MAX_OPEN_FILES 20

#define INDEX_ARRAY_SIZE (BF_BLOCK_SIZE-2*sizeof(int))/sizeof(int)
#define DATA_ARRAY_SIZE (BF_BLOCK_SIZE-3*sizeof(int))/sizeof(Record)

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
  int index[INDEX_ARRAY_SIZE];
} IndexBlock;

typedef struct DataBlock {
  int localDepth;
  int lastEmpty;
  int nextBlock;
  Record index[DATA_ARRAY_SIZE];
} DataBlock;

int open_files[MAX_OPEN_FILES];

HT_ErrorCode HT_Init() {
  //insert code here
  for (int i = 0 ; i < MAX_OPEN_FILES ; i++) {
    open_files[i] = -1;
  }
  return HT_OK;
}

HT_ErrorCode HT_CreateIndex(const char *filename, int depth) {
  //insert code here
  CALL_BF(BF_CreateFile(filename));
  int fileDesc;
  CALL_BF(BF_OpenFile(filename, &fileDesc));
  int arraySize = 1;
  for (int i = 0; i < depth; i++) {
    arraySize *= 2;
  }

  int indexBlockAmount = (arraySize - 1) / INDEX_ARRAY_SIZE + 1;
  BF_Block* block[indexBlockAmount];
  for (int i = 0; i < indexBlockAmount; i++){
    CALL_BF(BF_AllocateBlock(fileDesc, block[i]));
    IndexBlock* data = (IndexBlock*) BF_Block_GetData(block[i]);
    data->globalDepth = depth;
    if (i+1 < indexBlockAmount){
      data->nextBlock = i+1;
    }
  }

  for (int i; i < arraySize; i++) {
    BF_Block* dataBlock;
    CALL_BF(BF_AllocateBlock(fileDesc, dataBlock));
    DataBlock* dataBlockData = (DataBlock*) BF_Block_GetData(dataBlock);
    dataBlockData->localDepth = depth;
    dataBlockData->lastEmpty = 0;
    dataBlockData->nextBlock = -1;
    BF_Block_SetDirty(dataBlock);
    CALL_BF(BF_UnpinBlock(dataBlock));
  }

  int dataBlockCounter = indexBlockAmount;
  for (int i = 0; i < indexBlockAmount; i++){
    IndexBlock* data = (IndexBlock*) BF_Block_GetData(block[i]);
    for (int j = 0; i < INDEX_ARRAY_SIZE; j++){
      if (dataBlockCounter < indexBlockAmount + arraySize - 1) data->index[j] = dataBlockCounter;
      else data->index[j] = -1;
      dataBlockCounter++;      
    }

    BF_Block_SetDirty(block[i]); 
    CALL_BF(BF_UnpinBlock(block[i]));   
  }

  return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){
  //insert code here
  int fd;
  CALL_BF(BF_OpenFile(fileName, &fd));
  int i;
  for (i = 0 ; i < MAX_OPEN_FILES ; i++) {
    if(open_files[i] == -1) {
      open_files[i] = fd;
    }
  }
  if (i == MAX_OPEN_FILES) {
    return HT_ERROR;
  }
  *indexDesc = i;
  return HT_OK;
}

HT_ErrorCode HT_CloseFile(int indexDesc) {
  //insert code here
  CALL_BF(BF_CloseFile(open_files[indexDesc]));
  open_files[indexDesc] = -1;
  return HT_OK;
}

//compares the hash value of first entry to all others
//if all are the same returns true
//else false
int sameHash(Record *records){
  int hash=hash_func(records[0].id);
  for (int i=1;i<(sizeof(records)/sizeof(Record)); i++){
    if (hash != hash_func(records[i].id)){
      return 0;
    }
  }
  return 1;
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {
  printf("Inserting {%i,%s,%s,%s}", record.id, record.name, record.surname, record.city);
  IndexBlock *index = open_files[indexDesc];
  int hashID = (hash_func(record.id)%(2^index->globalDepth));
  int count=1;
  while (index->nextBlock){
    if(count*INDEX_ARRAY_SIZE<hashID){
      index = index->nextBlock;
      count++;
    }
    else{
      BF_Block *targetBlock;
      CALL_BF(BF_GetBlock(open_files[indexDesc],index->index[hashID-(count*INDEX_ARRAY_SIZE)],targetBlock));
      DataBlock *targetData = (DataBlock *)BF_Block_GetData(targetBlock);

      if(targetData->nextBlock==-1){  //only one block

        //making an array with all the entries of this block
        Record *entryArray=malloc((1+targetData->lastEmpty)*sizeof(Record));  //1 for the new entry and all the entries of the block
        entryArray[0] = record;
        for (int i = 0; i < targetData->lastEmpty; i++){
          entryArray[i+1]=targetData->index[i];
        }

        if (targetData->lastEmpty<DATA_ARRAY_SIZE){
          //insert
          targetData->index[targetData->lastEmpty] = record;
          targetData->lastEmpty++;
          BF_Block_SetDirty(targetBlock);
          CALL_BF(BF_UnpinBlock(targetBlock));
          return HT_OK;
        }
        else if(sameHash(entryArray)){
          //make next block
        }
        else if(targetData->localDepth==32){    //reached MAX depth
          //make next block
        }
        else{
          //split
        }
      }
      else{                           //has overflow

        //making an array with all the entries of this bin
        Record *entryArray=malloc((1+targetData->lastEmpty)*sizeof(Record));  //1 for the new entry and all the entries of the block
        entryArray[0] = record;
        for (int i = 0; i < targetData->lastEmpty; i++){
          entryArray[i+1]=targetData->index[i];
        }
        int blockCount=1;
        while(targetData->nextBlock!=-1){
          CALL_BF(BF_UnpinBlock(targetBlock));  //(no SetDirty because we only read)
          CALL_BF(BF_GetBlock(open_files[indexDesc],targetData->nextBlock,targetBlock));
          targetData = (DataBlock *)BF_Block_GetData(targetBlock);
          entryArray=realloc(entryArray,(1+(blockCount*DATA_ARRAY_SIZE)+targetData->lastEmpty)*sizeof(Record)); //all previous block are full, they have blockCount*DATA_ARRAY_SIZE entries
          for (int i = 0; i < targetData->lastEmpty; i++){
            entryArray[i+1+(blockCount*DATA_ARRAY_SIZE)]=targetData->index[i];
          }
          blockCount++;
        }
        //the last block is still pined

        if (targetData->lastEmpty<DATA_ARRAY_SIZE){ //last block has space
          if(sameHash(entryArray)){
            //insert
            targetData->index[targetData->lastEmpty] = record;
            targetData->lastEmpty++;
            BF_Block_SetDirty(targetBlock);
            CALL_BF(BF_UnpinBlock(targetBlock));
            return HT_OK;
          }
          else if(targetData->localDepth==32){    //reached MAX depth
            //insert
            targetData->index[targetData->lastEmpty] = record;
            targetData->lastEmpty++;
            BF_Block_SetDirty(targetBlock);
            CALL_BF(BF_UnpinBlock(targetBlock));
            return HT_OK;
          }
          else{
            //split
          }
        }
        else{                                       //last block is full
          if(sameHash(entryArray)){
            //new block
          }
          else if(targetData->localDepth==32){    //reached MAX depth
            //new block
          }
          else{
            //split
          }
        }
      }
    }
  }
  return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
  //insert code here
  return HT_OK;
}

