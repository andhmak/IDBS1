#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

#include "bf.h"
#include "hash_file.h"
#define MAX_OPEN_FILES 20
#define NAME_BUF 100

#define INDEX_ARRAY_SIZE (BF_BLOCK_SIZE-sizeof(int))/sizeof(int)
#define DATA_ARRAY_SIZE (BF_BLOCK_SIZE-3*sizeof(int))/sizeof(Record)
#define MAX_DEPTH 1

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    printf("%d",code);    \
    return HT_ERROR;        \
  }                         \
}

int hash_func(int x) {
    x = ((x >> (sizeof(int)/2)) ^ x) * 0x45d9f3b;
    x = ((x >> (sizeof(int)/2)) ^ x) * 0x45d9f3b;
    x = (x >> (sizeof(int)/2)) ^ x;
    return x;
}

typedef struct StatBlock {
  int min_rec_per_bucket;
  int max_rec_per_bucket;
  int total_recs;
  int total_buckets;
  int globalDepth;
} StatBlock;

typedef struct IndexBlock {
  int nextBlock;
  int index[INDEX_ARRAY_SIZE];
} IndexBlock;

typedef struct DataBlock {
  int localDepth;
  int lastEmpty;
  int nextBlock;
  Record index[DATA_ARRAY_SIZE];
} DataBlock;

typedef struct OpenFileData {
  int mainPos;
  char filename[NAME_BUF];
  int fileDesc;
  int globalDepth;
  int *index;
} OpenFileData;

OpenFileData open_files[MAX_OPEN_FILES];

HT_ErrorCode HT_Init() {
  //insert code here
  for (int i = 0 ; i < MAX_OPEN_FILES ; i++) {
    open_files[i].fileDesc = -1;
    strcpy(open_files[i].filename, "");
  }
  printf("HT_Init ended OK\n");
  fflush(stdout);
  return HT_OK;
}

HT_ErrorCode HT_CreateIndex(const char *filename, int depth) {
  // Create block file
  CALL_BF(BF_CreateFile(filename));
  int fileDesc;
  // Open file
  CALL_BF(BF_OpenFile(filename, &fileDesc));

  printf("BF create and open in HT_Create is OK\n");
  fflush(stdout);

  // Initialise statistics block
  int arraySize = 1;
  for (int i = 0; i < depth; i++) {
    arraySize *= 2;
  }

  printf("HT_Create: Array size calculated OK\n");
  fflush(stdout);

  BF_Block* block;
  BF_Block_Init(&block);

  CALL_BF(BF_AllocateBlock(fileDesc, block));

  printf("HT_Create: Stat block allocated OK\n");
  fflush(stdout);

  StatBlock* stat = (StatBlock*) BF_Block_GetData(block);

  printf("HT_Create: Stat block data taken OK\n");
  fflush(stdout);


  stat->globalDepth = depth;
  stat->max_rec_per_bucket = 0;
  stat->min_rec_per_bucket = 0;
  stat->total_buckets = arraySize;
  stat->total_recs = 0;

  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));

  printf("HT_Create: Stat bucket created OK\n");
  fflush(stdout);

  // Initialise index blocks
  int indexBlockAmount = (arraySize - 1) / INDEX_ARRAY_SIZE + 1;
  for (int i = 0; i < indexBlockAmount; i++){
    CALL_BF(BF_AllocateBlock(fileDesc, block));
    IndexBlock* data = (IndexBlock*) BF_Block_GetData(block);
    if (i+1 < indexBlockAmount) {
      data->nextBlock = i+2;
    }
    else {
      data->nextBlock = -1;
    }
    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
  }

  printf("HT_Create: Index blocks initialised OK\n");
  fflush(stdout);

  // Initialise buckets
  for (int i = 0; i < arraySize; i++) {
    CALL_BF(BF_AllocateBlock(fileDesc, block));
    DataBlock* dataBlockData = (DataBlock*) BF_Block_GetData(block);
    dataBlockData->localDepth = depth;
    dataBlockData->lastEmpty = 0;
    dataBlockData->nextBlock = -1;
    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
  }

  printf("HT_Create: Buckets initialised OK\n");
  fflush(stdout);

  // Map index to buckets
  int dataBlockCounter = indexBlockAmount + 1;
  for (int i = 1; i < indexBlockAmount + 1; i++){
    CALL_BF(BF_GetBlock(fileDesc, i, block));
    IndexBlock* data = (IndexBlock*) BF_Block_GetData(block);
    for (int j = 0; j < INDEX_ARRAY_SIZE; j++){
      if (dataBlockCounter < indexBlockAmount + arraySize + 1) data->index[j] = dataBlockCounter;
      else data->index[j] = -1;
      dataBlockCounter++;      
    }
    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
  }

  printf("HT_Create: Buckets mapped to index OK\n");
  fflush(stdout);

  BF_Block_Destroy(&block);

  // Close file
  CALL_BF(BF_CloseFile(fileDesc));
  printf("HT_Create ended OK\n");
  fflush(stdout);
  return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){
  printf("HT_Open started OK\n");
  fflush(stdout);
  // Check for empty position in open files array
  int i;
  for (i = 0 ; i < MAX_OPEN_FILES ; i++) {
    if(open_files[i].fileDesc == -1) {
      break;
    }
  }
  if (i == MAX_OPEN_FILES) {
    return HT_ERROR;
  }

  *indexDesc = i;

  // Check if file is already open
  for (int j = 0 ; j < MAX_OPEN_FILES ; j++) {
    if((strcmp(open_files[j].filename, fileName) == 0) && (open_files[j].mainPos == -1)) {
      strcpy(open_files[i].filename, fileName);
      open_files[i].mainPos = j;
      open_files[i].fileDesc = open_files[j].fileDesc;
      open_files[i].index = open_files[j].index;
      printf("HT_Open ended OK\n");
      fflush(stdout);
      return HT_OK;
    }
  }
  
  // Else open it bring everything necessary to memory
  strcpy(open_files[i].filename, fileName);
  open_files[i].mainPos = -1;
  int fd;
  CALL_BF(BF_OpenFile(fileName, &fd));
  open_files[i].fileDesc = fd;
  
  BF_Block* block;
  BF_Block_Init(&block);
  CALL_BF(BF_GetBlock(fd, 0, block));
  StatBlock* stat = (StatBlock*) BF_Block_GetData(block);
  open_files[i].globalDepth = stat->globalDepth;
  CALL_BF(BF_UnpinBlock(block));
  int indexSize = 1;
  for (int j = 0; j < open_files[i].globalDepth; j++) {
    indexSize *= 2;
  }
  if ((open_files[i].index = malloc(indexSize*sizeof(int))) == NULL) {
    CALL_BF(BF_Close(fd));
    return HT_ERROR;
  }
  CALL_BF(BF_GetBlock(fd, 1, block));
  IndexBlock* data = (IndexBlock*) BF_Block_GetData(block);
  int nextBlock;
  for (int j = 0 ; nextBlock != -1 ; ) {
    for (int k = 0 ; (k < INDEX_ARRAY_SIZE) && (j < indexSize); k++, j++) {
      open_files[i].index[j] = data->index[k];
    }
    nextBlock = data->nextBlock;
    CALL_BF(BF_UnpinBlock(block));
    if (nextBlock != -1) {
      CALL_BF(BF_GetBlock(fd, nextBlock, block));
      data = (IndexBlock*) BF_Block_GetData(block);
    }
  }
  BF_Block_Destroy(&block);
  printf("HT_Open ended OK\n");
  fflush(stdout);
  return HT_OK;
}

HT_ErrorCode HT_CloseFile(int indexDesc) {
  printf("HT_Close started OK\n");
  fflush(stdout);
  // Check if indexDesc valid
  if ((indexDesc < 0) || (indexDesc >= MAX_OPEN_FILES) || (open_files[indexDesc].fileDesc == -1)) {
    return HT_ERROR;
  }

  // Check if secondary entry
  if (open_files[indexDesc].mainPos != -1) {
    open_files[indexDesc].fileDesc = -1;
    strcpy(open_files[indexDesc].filename, "");
    printf("HT_Close was secondary and ended OK\n");
    fflush(stdout);
    return HT_OK;
  }

  // Check if file is already open elsewhere as secondary entry
  for (int j = 0 ; j < MAX_OPEN_FILES ; j++) {
    if((strcmp(open_files[j].filename, open_files[indexDesc].filename) == 0) && (j != indexDesc)) {
      open_files[indexDesc].fileDesc = -1;
      strcpy(open_files[indexDesc].filename, "");
      open_files[j].mainPos = -1;
      open_files[j].globalDepth = open_files[indexDesc].globalDepth;
      printf("HT_Close was main and ended OK\n");
      return HT_OK;
    }
  }

  // Else write new information to disk close it completely
  int fd = open_files[indexDesc].fileDesc;
  BF_Block* block;
  BF_Block_Init(&block);

  CALL_BF(BF_GetBlock(fd, 0, block));
  StatBlock* stat = (StatBlock*) BF_Block_GetData(block);
  stat->globalDepth = open_files[indexDesc].globalDepth;
  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));

  CALL_BF(BF_GetBlock(fd, 1, block));
  IndexBlock* data = (IndexBlock*) BF_Block_GetData(block);
  int indexSize = 1;
  for (int j = 0; j < open_files[indexDesc].globalDepth; j++) {
    indexSize *= 2;
  }
  int blockAmount;
  CALL_BF(BF_GetBlockCounter(fd, &blockAmount));
  int nextBlock;
  for (int j = 0 ; j < indexSize ; ) {
    for (int k = 0 ; (k < INDEX_ARRAY_SIZE) && (j < indexSize); k++, j++) {
      data->index[k] = open_files[indexDesc].index[j];
    }
    nextBlock = data->nextBlock;
    if (nextBlock == -1) {
      data->nextBlock = blockAmount++;
      BF_Block_SetDirty(block);
      CALL_BF(BF_UnpinBlock(block));
      if (j < indexSize - 1) {
        CALL_BF(BF_AllocateBlock(fd, block));
        data = (IndexBlock*) BF_Block_GetData(block);
        data->nextBlock = -1;
      }
    }
    else {
      BF_Block_SetDirty(block);
      CALL_BF(BF_UnpinBlock(block));
      if (j < indexSize - 1) {
        CALL_BF(BF_GetBlock(fd, nextBlock, block));
        data = (IndexBlock*) BF_Block_GetData(block);
      }
    }
  }
  BF_Block_Destroy(&block);
  printf("HT_Close memory moved to disk OK\n");
  fflush(stdout);

  CALL_BF(BF_CloseFile(open_files[indexDesc].fileDesc));
  free(open_files[indexDesc].index);
  open_files[indexDesc].fileDesc = -1;
  strcpy(open_files[indexDesc].filename, "");
  printf("HT_Close ended OK\n");
  fflush(stdout);
  return HT_OK;
}

//compares the ID value of first entry to all others
//if all are the same returns true
//else false
int sameID(Record *records){
  int hash=records[0].id;
  for (int i=1;i<(sizeof(records)/sizeof(Record)); i++){
    if (hash != records[i].id){
      return 0;
    }
  }
  return 1;
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {
  printf("Inserting {%i,%s,%s,%s}\n", record.id, record.name, record.surname, record.city);
  
  int hashID = (hash_func(record.id) >> (8*sizeof(int) - open_files[indexDesc].globalDepth));
  BF_Block *targetBlock;
  BF_Block_Init(&targetBlock);
  CALL_BF(BF_GetBlock(open_files[indexDesc].fileDesc,open_files[indexDesc].index[hashID],targetBlock));
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
      targetData->index[targetData->lastEmpty].id = record.id;
      strcpy(targetData->index[targetData->lastEmpty].name,record.name);
      strcpy(targetData->index[targetData->lastEmpty].surname,record.surname);
      strcpy(targetData->index[targetData->lastEmpty].city,record.city);
      targetData->lastEmpty++;
      BF_Block_SetDirty(targetBlock);
      CALL_BF(BF_UnpinBlock(targetBlock));
      BF_Block_Destroy(&targetBlock);
      return HT_OK;
    }
    else if(sameID(entryArray)){
      //make next block
      BF_Block *newBlock;
      BF_Block_Init(&newBlock);
      DataBlock *newBlockData;
      CALL_BF(BF_AllocateBlock(open_files[indexDesc].fileDesc,newBlock));
      newBlockData = (DataBlock *)BF_Block_GetData(newBlock);

      CALL_BF(BF_GetBlockCounter(open_files[indexDesc].fileDesc,&(targetData->nextBlock)));
      newBlockData->localDepth = targetData->localDepth;
      newBlockData->index[0].id = record.id;
      strcpy(newBlockData->index[0].name,record.name);
      strcpy(newBlockData->index[0].surname,record.surname);
      strcpy(newBlockData->index[0].city,record.city);
      newBlockData->lastEmpty = 1;
      newBlockData->nextBlock = -1;

      BF_Block_SetDirty(targetBlock);
      BF_Block_SetDirty(newBlock);
      CALL_BF(BF_UnpinBlock(targetBlock));
      CALL_BF(BF_UnpinBlock(newBlock));
      BF_Block_Destroy(&targetBlock);
      BF_Block_Destroy(&newBlock);
      return HT_OK;
    }
    else if(targetData->localDepth==MAX_DEPTH){    //reached MAX depth
      //make next block
      BF_Block *newBlock;
      BF_Block_Init(&newBlock);
      DataBlock *newBlockData;
      CALL_BF(BF_AllocateBlock(open_files[indexDesc].fileDesc,newBlock));
      newBlockData = (DataBlock *)BF_Block_GetData(newBlock);

      CALL_BF(BF_GetBlockCounter(open_files[indexDesc].fileDesc,&(targetData->nextBlock)));
      newBlockData->localDepth = targetData->localDepth;
      newBlockData->index[0].id = record.id;
      strcpy(newBlockData->index[0].name,record.name);
      strcpy(newBlockData->index[0].surname,record.surname);
      strcpy(newBlockData->index[0].city,record.city);
      newBlockData->lastEmpty = 1;
      newBlockData->nextBlock = -1;

      BF_Block_SetDirty(targetBlock);
      BF_Block_SetDirty(newBlock);
      CALL_BF(BF_UnpinBlock(targetBlock));
      CALL_BF(BF_UnpinBlock(newBlock));
      BF_Block_Destroy(&targetBlock);
      BF_Block_Destroy(&newBlock);
      return HT_OK;
    }
    else{
      //split
      if(open_files[indexDesc].globalDepth==targetData->localDepth){
        open_files[indexDesc].globalDepth++;

        int *newIndex = malloc((2<<open_files[indexDesc].globalDepth)*sizeof(int));
        for (int i=0,j=0;i<(2<<(open_files[indexDesc].globalDepth-1)) && j<(2<<(open_files[indexDesc].globalDepth));i++,j+=2){
          newIndex[j]=open_files[indexDesc].index[i];
          newIndex[j+1]=open_files[indexDesc].index[i];
        }
        
        //open_files[indexDesc].index[(2*hashID)+1] will have the same block as in the last index but it will be empty
        targetData->localDepth = open_files[indexDesc].globalDepth;
        targetData->lastEmpty = 0;
        targetData->nextBlock = -1;

        BF_Block_SetDirty(targetBlock);
        CALL_BF(BF_UnpinBlock(targetBlock));

        BF_Block *newBlock;
        BF_Block_Init(newBlock);
        DataBlock *newBlockData;
        CALL_BF(BF_AllocateBlock(open_files[indexDesc].fileDesc,newBlock));
        newBlockData = (DataBlock *)BF_Block_GetData(newBlock);

        CALL_BF(BF_GetBlockCounter(open_files[indexDesc].fileDesc,&open_files[indexDesc].index[(2*hashID)+1]));
        newBlockData->localDepth = open_files[indexDesc].globalDepth;
        newBlockData->lastEmpty = 0;
        newBlockData->nextBlock = -1;

        BF_Block_SetDirty(newBlock);
        CALL_BF(BF_UnpinBlock(newBlock));
        BF_Block_Destroy(&newBlock);

        free(open_files[indexDesc].index);
        open_files[indexDesc].index=newIndex;
        for (int i=0;i<sizeof(entryArray)/sizeof(record);i++){
          HT_InsertEntry(open_files[indexDesc].fileDesc,entryArray[i]);
        }
        return HT_OK; 
      }
      else if(open_files[indexDesc].globalDepth>targetData->localDepth){
        int firstIDtoBlock=((hashID >> targetData->localDepth) << targetData->localDepth);
        
        int dataBlock;
        BF_Block *newBlock;
        BF_Block_Init(&newBlock);
        DataBlock *newBlockData;
        CALL_BF(BF_AllocateBlock(open_files[indexDesc].fileDesc,newBlock));
        newBlockData = (DataBlock *)BF_Block_GetData(newBlock);

        CALL_BF(BF_GetBlockCounter(open_files[indexDesc].fileDesc,&dataBlock));
        newBlockData->localDepth = targetData->localDepth+1;
        newBlockData->lastEmpty = 0;
        newBlockData->nextBlock = -1;
        
        CALL_BF(BF_UnpinBlock(targetBlock));
        for (int i=firstIDtoBlock;i<firstIDtoBlock+(2<<(targetData->localDepth-1));i++){
          open_files[indexDesc].index[i]=dataBlock;
        }

        //the second half of the hashIDs will have the same block as in the last index but it will be empty
        targetData->localDepth++;
        targetData->lastEmpty = 0;
        targetData->nextBlock = -1;

        BF_Block_SetDirty(targetBlock);
        CALL_BF(BF_UnpinBlock(targetBlock));
        BF_Block_Destroy(&targetBlock);

        BF_Block_SetDirty(newBlock);
        CALL_BF(BF_UnpinBlock(newBlock));
        BF_Block_Destroy(&newBlock);

        for (int i=0;i<sizeof(entryArray)/sizeof(record);i++){
          HT_InsertEntry(open_files[indexDesc].fileDesc,entryArray[i]);
        }
        return HT_OK; 
      }
      else{
        CALL_BF(BF_UnpinBlock(targetBlock));
        BF_Block_Destroy(&targetBlock);
        return HT_ERROR;
      }
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
      CALL_BF(BF_GetBlock(open_files[indexDesc].fileDesc,targetData->nextBlock,targetBlock));
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
        targetData->index[targetData->lastEmpty].id = record.id;
        strcpy(targetData->index[targetData->lastEmpty].name,record.name);
        strcpy(targetData->index[targetData->lastEmpty].surname,record.surname);
        strcpy(targetData->index[targetData->lastEmpty].city,record.city);
        targetData->lastEmpty++;
        BF_Block_SetDirty(targetBlock);
        CALL_BF(BF_UnpinBlock(targetBlock));
        BF_Block_Destroy(&targetBlock);
        return HT_OK;
      }
      else if(targetData->localDepth==MAX_DEPTH){    //reached MAX depth
        //insert
        targetData->index[targetData->lastEmpty].id = record.id;
        strcpy(targetData->index[targetData->lastEmpty].name,record.name);
        strcpy(targetData->index[targetData->lastEmpty].surname,record.surname);
        strcpy(targetData->index[targetData->lastEmpty].city,record.city);
        targetData->lastEmpty++;
        BF_Block_SetDirty(targetBlock);
        CALL_BF(BF_UnpinBlock(targetBlock));
        BF_Block_Destroy(&targetBlock);
        return HT_OK;
      }
      else{
        //split
        if(open_files[indexDesc].globalDepth==targetData->localDepth){
          open_files[indexDesc].globalDepth++;
          int *newIndex[2^open_files[indexDesc].globalDepth];
          for (int i=0,j=0;i<2^(open_files[indexDesc].globalDepth-1) && j<2^(open_files[indexDesc].globalDepth);i++,j++){
            newIndex[j]=open_files[indexDesc].index[i];
            newIndex[j+1]=open_files[indexDesc].index[i];
          }
          BF_Block *blockToDelete;
          DataBlock *tempData;
          CALL_BF(BF_GetBlock(open_files[indexDesc].fileDesc,open_files[indexDesc].index[hashID],blockToDelete));
          for (int i=0;i<blockCount;i++){
            tempData = (DataBlock *)BF_Block_GetData(blockToDelete);
            CALL_BF(BF_UnpinBlock(blockToDelete));  //I don't know if this is needed
            if(tempData->nextBlock!=-1) BF_GetBlock(open_files[indexDesc].fileDesc,tempData->nextBlock,blockToDelete);
          }
          CALL_BF(BF_UnpinBlock(targetBlock));

          BF_Block *newBlock;
          DataBlock *newBlockData;
          CALL_BF(BF_AllocateBlock(open_files[indexDesc].fileDesc,newBlock));
          newBlockData = (DataBlock *)BF_Block_GetData(newBlock);

          CALL_BF(BF_GetBlockCounter(open_files[indexDesc].fileDesc,open_files[indexDesc].index[2*hashID]));
          newBlockData->localDepth = open_files[indexDesc].globalDepth;
          newBlockData->lastEmpty = 0;
          newBlockData->nextBlock = -1;
          
          BF_Block_SetDirty(newBlock);
          CALL_BF(BF_UnpinBlock(newBlock));

          CALL_BF(BF_AllocateBlock(open_files[indexDesc].fileDesc,newBlock));
          newBlockData = (DataBlock *)BF_Block_GetData(newBlock);

          CALL_BF(BF_GetBlockCounter(open_files[indexDesc].fileDesc,open_files[indexDesc].index[(2*hashID)+1]));
          newBlockData->localDepth = open_files[indexDesc].globalDepth;
          newBlockData->lastEmpty = 0;
          newBlockData->nextBlock = -1;

          BF_Block_SetDirty(newBlock);
          CALL_BF(BF_UnpinBlock(newBlock));
          
          free(open_files[indexDesc].index);
          open_files[indexDesc].index=newIndex;
          for (int i=0;i<sizeof(entryArray)/sizeof(record);i++){
            HT_InsertEntry(open_files[indexDesc].fileDesc,entryArray[i]);
          }
          return HT_OK; 
        }
        else if(open_files[indexDesc].globalDepth>targetData->localDepth){
          int firstIDtoBlock=hashID-(hashID%(2^targetData->localDepth));
          
          int dataBlock1,dataBlock2;
          BF_Block *newBlock;
          DataBlock *newBlockData;
          CALL_BF(BF_AllocateBlock(open_files[indexDesc].fileDesc,newBlock));
          newBlockData = (DataBlock *)BF_Block_GetData(newBlock);

          CALL_BF(BF_GetBlockCounter(open_files[indexDesc].fileDesc,dataBlock1));
          newBlockData->localDepth = targetData->localDepth+1;
          newBlockData->lastEmpty = 0;
          newBlockData->nextBlock = -1;
          
          CALL_BF(BF_AllocateBlock(open_files[indexDesc].fileDesc,newBlock));
          newBlockData = (DataBlock *)BF_Block_GetData(newBlock);

          CALL_BF(BF_GetBlockCounter(open_files[indexDesc].fileDesc,dataBlock2));
          newBlockData->localDepth = targetData->localDepth+1;
          newBlockData->lastEmpty = 0;
          newBlockData->nextBlock = -1;
          
          CALL_BF(BF_UnpinBlock(targetBlock));
          for (int i=firstIDtoBlock;i<firstIDtoBlock+2^(targetData->localDepth-1);i++){
            open_files[indexDesc].index[i]=dataBlock1;
          }
          for (int i=firstIDtoBlock+2^(targetData->localDepth-1);i<firstIDtoBlock+2^(targetData->localDepth);i++){
            open_files[indexDesc].index[i]=dataBlock2;
          }

          BF_Block_SetDirty(dataBlock1);
          BF_Block_SetDirty(dataBlock2);
          CALL_BF(BF_UnpinBlock(dataBlock1));
          CALL_BF(BF_UnpinBlock(dataBlock2));

          for (int i=0;i<sizeof(entryArray)/sizeof(record);i++){
            HT_InsertEntry(open_files[indexDesc].fileDesc,entryArray[i]);
          }
          return HT_OK; 
        }
        else{
          CALL_BF(BF_UnpinBlock(targetBlock));
          return HT_ERROR;
        }
      }
    }
    else{                                       //last block is full
      if(sameHash(entryArray)){
        //new block
        BF_Block *newBlock;
        BF_Block_Init(&newBlock);
        DataBlock *newBlockData;
        CALL_BF(BF_AllocateBlock(open_files[indexDesc].fileDesc,newBlock));
        newBlockData = (DataBlock *)BF_Block_GetData(newBlock);

        CALL_BF(BF_GetBlockCounter(open_files[indexDesc].fileDesc,&(targetData->nextBlock)));
        newBlockData->localDepth = targetData->localDepth;
        newBlockData->index[0].id = record.id;
        strcpy(newBlockData->index[0].name,record.name);
        strcpy(newBlockData->index[0].surname,record.surname);
        strcpy(newBlockData->index[0].city,record.city);
        newBlockData->lastEmpty = 1;
        newBlockData->nextBlock = -1;

        BF_Block_SetDirty(targetBlock);
        BF_Block_SetDirty(newBlock);
        CALL_BF(BF_UnpinBlock(targetBlock));
        CALL_BF(BF_UnpinBlock(newBlock));
        BF_Block_Destroy(&targetBlock);
        BF_Block_Destroy(&newBlock);
        return HT_OK;
      }
      else if(targetData->localDepth==MAX_DEPTH){    //reached MAX depth
        //new block
        BF_Block *newBlock;
        BF_Block_Init(&newBlock);
        DataBlock *newBlockData;
        CALL_BF(BF_AllocateBlock(open_files[indexDesc].fileDesc,newBlock));
        newBlockData = (DataBlock *)BF_Block_GetData(newBlock);

        CALL_BF(BF_GetBlockCounter(open_files[indexDesc].fileDesc,&(targetData->nextBlock)));
        newBlockData->localDepth = targetData->localDepth;
        newBlockData->index[0].id = record.id;
        strcpy(newBlockData->index[0].name,record.name);
        strcpy(newBlockData->index[0].surname,record.surname);
        strcpy(newBlockData->index[0].city,record.city);
        newBlockData->lastEmpty = 1;
        newBlockData->nextBlock = -1;

        BF_Block_SetDirty(targetBlock);
        BF_Block_SetDirty(newBlock);
        CALL_BF(BF_UnpinBlock(targetBlock));
        CALL_BF(BF_UnpinBlock(newBlock));
        BF_Block_Destroy(&targetBlock);
        BF_Block_Destroy(&newBlock);
        return HT_OK;
      }
      else{
        //split
        if(open_files[indexDesc].globalDepth==targetData->localDepth){
          open_files[indexDesc].globalDepth++;
          int *newIndex[2^open_files[indexDesc].globalDepth];
          for (int i=0,j=0;i<2^(open_files[indexDesc].globalDepth-1) && j<2^(open_files[indexDesc].globalDepth);i++,j++){
            newIndex[j]=open_files[indexDesc].index[i];
            newIndex[j+1]=open_files[indexDesc].index[i];
          }
          BF_Block *blockToDelete;
          DataBlock *tempData;
          CALL_BF(BF_GetBlock(open_files[indexDesc].fileDesc,open_files[indexDesc].index[hashID],blockToDelete));
          for (int i=0;i<blockCount;i++){
            tempData = (DataBlock *)BF_Block_GetData(blockToDelete);
            CALL_BF(BF_UnpinBlock(blockToDelete));  //I don't know if this is needed
            if(tempData->nextBlock!=-1) BF_GetBlock(open_files[indexDesc].fileDesc,tempData->nextBlock,blockToDelete);
          }
          CALL_BF(BF_UnpinBlock(targetBlock));

          BF_Block *newBlock;
          DataBlock *newBlockData;
          CALL_BF(BF_AllocateBlock(open_files[indexDesc].fileDesc,newBlock));
          newBlockData = (DataBlock *)BF_Block_GetData(newBlock);

          CALL_BF(BF_GetBlockCounter(open_files[indexDesc].fileDesc,open_files[indexDesc].index[2*hashID]));
          newBlockData->localDepth = open_files[indexDesc].globalDepth;
          newBlockData->lastEmpty = 0;
          newBlockData->nextBlock = -1;

          BF_Block_SetDirty(newBlock);
          CALL_BF(BF_UnpinBlock(newBlock));
          
          CALL_BF(BF_AllocateBlock(open_files[indexDesc].fileDesc,newBlock));
          newBlockData = (DataBlock *)BF_Block_GetData(newBlock);

          CALL_BF(BF_GetBlockCounter(open_files[indexDesc].fileDesc,open_files[indexDesc].index[(2*hashID)+1]));
          newBlockData->localDepth = open_files[indexDesc].globalDepth;
          newBlockData->lastEmpty = 0;
          newBlockData->nextBlock = -1;

          BF_Block_SetDirty(newBlock);
          CALL_BF(BF_UnpinBlock(newBlock));

          free(open_files[indexDesc].index);
          open_files[indexDesc].index=newIndex;
          for (int i=0;i<sizeof(entryArray)/sizeof(record);i++){
            HT_InsertEntry(open_files[indexDesc].fileDesc,entryArray[i]);
          }
          return HT_OK; 
        }
        else if(open_files[indexDesc].globalDepth>targetData->localDepth){
          int firstIDtoBlock=hashID-(hashID%(2^targetData->localDepth));
          
          int dataBlock1,dataBlock2;
          BF_Block *newBlock;
          DataBlock *newBlockData;
          CALL_BF(BF_AllocateBlock(open_files[indexDesc].fileDesc,newBlock));
          newBlockData = (DataBlock *)BF_Block_GetData(newBlock);

          CALL_BF(BF_GetBlockCounter(open_files[indexDesc].fileDesc,dataBlock1));
          newBlockData->localDepth = targetData->localDepth+1;
          newBlockData->lastEmpty = 0;
          newBlockData->nextBlock = -1;
          
          CALL_BF(BF_AllocateBlock(open_files[indexDesc].fileDesc,newBlock));
          newBlockData = (DataBlock *)BF_Block_GetData(newBlock);

          CALL_BF(BF_GetBlockCounter(open_files[indexDesc].fileDesc,dataBlock2));
          newBlockData->localDepth = targetData->localDepth+1;
          newBlockData->lastEmpty = 0;
          newBlockData->nextBlock = -1;
          
          CALL_BF(BF_UnpinBlock(targetBlock));
          for (int i=firstIDtoBlock;i<firstIDtoBlock+2^(targetData->localDepth-1);i++){
            open_files[indexDesc].index[i]=dataBlock1;
          }
          for (int i=firstIDtoBlock+2^(targetData->localDepth-1);i<firstIDtoBlock+2^(targetData->localDepth);i++){
            open_files[indexDesc].index[i]=dataBlock2;
          }

          BF_Block_SetDirty(dataBlock1);
          BF_Block_SetDirty(dataBlock2);
          CALL_BF(BF_UnpinBlock(dataBlock1));
          CALL_BF(BF_UnpinBlock(dataBlock2));

          for (int i=0;i<sizeof(entryArray)/sizeof(record);i++){
            HT_InsertEntry(open_files[indexDesc].fileDesc,entryArray[i]);
          }
          return HT_OK; 
        }
        else{
          CALL_BF(BF_UnpinBlock(targetBlock));
          return HT_ERROR;
        }
      }
    }
  }
  return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
  if (id==NULL){
    printf("Printing entries with ID: %i\n", *id);
    
    for (int i=0;i<INDEX_ARRAY_SIZE;i++){
      BF_Block *targetBlock;
      CALL_BF(BF_GetBlock(open_files[indexDesc].fileDesc,open_files[indexDesc].index[i],targetBlock));
      DataBlock *targetData = (DataBlock *)BF_Block_GetData(targetBlock);

      for (int i = 0; i < targetData->lastEmpty; i++){
        if (*id==targetData->index[i].id){
          printf("{%i,%s,%s,%s}\n", targetData->index[i].id, targetData->index[i].name, targetData->index[i].surname, targetData->index[i].city);
        }
      }
      while(targetData->nextBlock!=-1){
        CALL_BF(BF_UnpinBlock(targetBlock));  //(no SetDirty because we only read)
        CALL_BF(BF_GetBlock(open_files[indexDesc].fileDesc,targetData->nextBlock,targetBlock));
        targetData = (DataBlock *)BF_Block_GetData(targetBlock);
        for (int i = 0; i < targetData->lastEmpty; i++){
          if (*id==targetData->index[i].id){
            printf("{%i,%s,%s,%s}\n", targetData->index[i].id, targetData->index[i].name, targetData->index[i].surname, targetData->index[i].city);
          }
        }
      }
      CALL_BF(BF_UnpinBlock(targetBlock));
    }
  }
  else{
    printf("Printing entries with ID: %i\n", *id);

    int hashID = (hash_func(id)%(2^open_files[indexDesc].globalDepth));
    BF_Block *targetBlock;
    CALL_BF(BF_GetBlock(open_files[indexDesc].fileDesc,open_files[indexDesc].index[hashID],targetBlock));
    DataBlock *targetData = (DataBlock *)BF_Block_GetData(targetBlock);

    for (int i = 0; i < targetData->lastEmpty; i++){
      if (*id==targetData->index[i].id){
        printf("{%i,%s,%s,%s}\n", targetData->index[i].id, targetData->index[i].name, targetData->index[i].surname, targetData->index[i].city);
      }
    }
    while(targetData->nextBlock!=-1){
      CALL_BF(BF_UnpinBlock(targetBlock));  //(no SetDirty because we only read)
      CALL_BF(BF_GetBlock(open_files[indexDesc].fileDesc,targetData->nextBlock,targetBlock));
      targetData = (DataBlock *)BF_Block_GetData(targetBlock);
      for (int i = 0; i < targetData->lastEmpty; i++){
        if (*id==targetData->index[i].id){
          printf("{%i,%s,%s,%s}\n", targetData->index[i].id, targetData->index[i].name, targetData->index[i].surname, targetData->index[i].city);
        }
      }
    }
    CALL_BF(BF_UnpinBlock(targetBlock));
  }
  return HT_OK;
}

HT_ErrorCode HashStatistics(char* filename) {
  // Open file
  int fd;
  CALL_BF(BF_OpenFile(filename, &fd));
  // Print statistics stored on first block
  int blockAmount;
  CALL_BF(BF_GetBlockCounter(fd, &blockAmount));
  printf("Block amount: %d\n", blockAmount);
  BF_Block* block;
  BF_Block_Init(&block);
  CALL_BF(BF_GetBlock(fd, 0, block));
  StatBlock* stat = (StatBlock*) BF_Block_GetData(block);
  printf("Mimimum records per bucket: %d\n", stat->min_rec_per_bucket);
  printf("Average records per bucket: %d\n", stat->total_recs/stat->total_buckets);
  printf("Maximum records per bucket: %d\n", stat->max_rec_per_bucket);
  CALL_BF(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);
  // Close file
  CALL_BF(BF_CloseFile(fd));
  return HT_OK;
}