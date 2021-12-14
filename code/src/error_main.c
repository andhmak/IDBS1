#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "hash_file.h"
#define GLOBAL_DEPT_1 2 // you can change it if you want
#define FILE_NAME_1 "data_3.db"
#define MAX_OPEN_FILES 20

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
  
  Record record;
  srand(12569874);
  int r;
  // create a record
  record.id = 0;
  r = rand() % 12;
  memcpy(record.name, names[r], strlen(names[r]) + 1);
  r = rand() % 12;
  memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
  r = rand() % 10;
  memcpy(record.city, cities[r], strlen(cities[r]) + 1);

  printf("Choose a function to run with a wrong indexDesc:\n");
  printf("HT_CloseFile (1)\n");
  printf("HT_InsertEntry (2)\n");
  printf("HT_PrintAllEntries (3)\n");
  printf("None (open too many files instead) (4)\n");
  if (scanf("%d", &r) == EOF) {
    printf("scanf failed");
    exit(1);
  }
  int indexDesc1;
  switch(r) {
    case 1:
      CALL_OR_DIE(HT_CloseFile(0));
      break;
    case 2:
      CALL_OR_DIE(HT_InsertEntry(0, record));
      break;
    case 3:
      CALL_OR_DIE(HT_PrintAllEntries(0, NULL));
      break;
    case 4:
      CALL_OR_DIE(HT_CreateIndex(FILE_NAME_1, GLOBAL_DEPT_1));
      for (int i = 0 ; i <= MAX_OPEN_FILES ; i++) {
        CALL_OR_DIE(HT_OpenIndex(FILE_NAME_1, &indexDesc1));
      }
      break;
    default:
      printf("Not an option\n");
      break;
  }

  BF_Close();
}