#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "hash_file.h"
#define DATA_ARRAY_SIZE ((BF_BLOCK_SIZE-3*sizeof(int))/sizeof(Record))

#define RECORDS_NUM 256 // you can change it if you want
#define GLOBAL_DEPT_1 2 // you can change it if you want
#define GLOBAL_DEPT_2 10 // you can change it if you want
#define FILE_NAME_1 "data_1.db"
#define FILE_NAME_2 "data_2.db"

const char* names[] = {
  "Yannis",
  "Christofos",
  "Sofia",
  "Marianna",
  "Vagelis",
  "Maria",
  "Iosif",
  "Dionisis",
  "Konstantina",
  "Theofilos",
  "Giorgos",
  "Dimitris"
};

const char* surnames[] = {
  "Ioannidis",
  "Svingos",
  "Karvounari",
  "Rezkalla",
  "Nikolopoulos",
  "Berreta",
  "Koronis",
  "Gaitanis",
  "Oikonomou",
  "Mailis",
  "Michas",
  "Halatsis"
};

const char* cities[] = {
  "Athens",
  "San Francisco",
  "Los Angeles",
  "Amsterdam",
  "London",
  "New York",
  "Tokyo",
  "Hong Kong",
  "Munich",
  "Miami"
};

#define CALL_OR_DIE(call)     \
  {                           \
    HT_ErrorCode code = call; \
    if (code != HT_OK) {      \
      printf("Error\n");      \
      exit(code);             \
    }                         \
  }

int main() {
  BF_Init(LRU);
  CALL_OR_DIE(HT_Init());

  printf("Create the first file with a small initial depth and open it once\n");
  int indexDesc1;
  CALL_OR_DIE(HT_CreateIndex(FILE_NAME_1, GLOBAL_DEPT_1));
  CALL_OR_DIE(HT_OpenIndex(FILE_NAME_1, &indexDesc1));
  printf("Create the second file with a large initial depth and open it twice\n");
  int indexDesc2;
  CALL_OR_DIE(HT_CreateIndex(FILE_NAME_2, GLOBAL_DEPT_2));
  CALL_OR_DIE(HT_OpenIndex(FILE_NAME_2, &indexDesc2)); 
  int indexDesc3;
  CALL_OR_DIE(HT_OpenIndex(FILE_NAME_2, &indexDesc3)); 
  Record record;
  srand(12569874);
  int r;
  printf("Insert Entries with different IDs on the first file\n");
  for (int id = 0; id < RECORDS_NUM; ++id) {
    // create a record
    record.id = id;
    r = rand() % 12;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);

    CALL_OR_DIE(HT_InsertEntry(indexDesc1, record));
  }
  
  printf("RUN PrintAllEntries with specific ID on the first file\n");
  int id = rand() % RECORDS_NUM;
  CALL_OR_DIE(HT_PrintAllEntries(indexDesc1, &id));
  for (int i = 0; i < RECORDS_NUM; i++) {
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc1, &i));
  }
  printf("RUN PrintAllEntries without ID on the first file\n");
  CALL_OR_DIE(HT_PrintAllEntries(indexDesc1, NULL));


   printf("Insert enough Entries with same ID on the second file to create overflow lists\n");
   for (int i = 0; i < DATA_ARRAY_SIZE + 1; ++i) {
     // create a record
     record.id = 5;
     r = rand() % 12;
     memcpy(record.name, names[r], strlen(names[r]) + 1);
     r = rand() % 12;
     memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
     r = rand() % 10;
     memcpy(record.city, cities[r], strlen(cities[r]) + 1);

     CALL_OR_DIE(HT_InsertEntry(indexDesc2, record));
   }

  printf("RUN PrintAllEntries with ID on the second file\n");
  id = 5;
  CALL_OR_DIE(HT_PrintAllEntries(indexDesc2, &id));
  printf("RUN PrintAllEntries without ID on the second file\n");
  CALL_OR_DIE(HT_PrintAllEntries(indexDesc2, NULL));
  printf("RUN HashStatistics on the first open file\n");
  CALL_OR_DIE(HashStatistics(FILE_NAME_1));
  printf("RUN HashStatistics on the second open file (this could take a few seconds)\n");
  CALL_OR_DIE(HashStatistics(FILE_NAME_2));


  printf("Close the first file\n");
  CALL_OR_DIE(HT_CloseFile(indexDesc1));
  printf("Close the second file once\n");
  CALL_OR_DIE(HT_CloseFile(indexDesc2));
  printf("Close the second file twice (this could take a few seconds)\n");
  CALL_OR_DIE(HT_CloseFile(indexDesc3));
  printf("RUN HashStatistics on the first closed file\n");
  CALL_OR_DIE(HashStatistics(FILE_NAME_1));
  printf("RUN HashStatistics on the second closed file (this could take a few seconds)\n");
  CALL_OR_DIE(HashStatistics(FILE_NAME_2));
  BF_Close();
}
