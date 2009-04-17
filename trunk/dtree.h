#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "bool.h"
#include "list.h"

//#include <ctype.h>
//#include <sys/fcntl.h>
//#include <unistd.h>
//#include <sys/types.h>
//#include <sys/stat.h>
//#include <sys/io.h>


// ### MACROS ###

#define DEBUG(x) { \
	time_t time; \
	printf("%s - %s\n", ctime(&time), x); \
	}


// ### CONTANTES ###

#define C_FILENAME "dtree.db"

#define C_EXIT_FAILURE EXIT_FAILURE
#define C_EXIT_SUCCESS EXIT_SUCCESS

// Blocks

#define C_BLOCK_SIZE 1024 // 1024 B = 1 KB
#define C_MAX_BLOCKS 4096
#define C_BLOCK_HEADER_OFFSET 0

#define C_BLOCK_HEADER_SIZE 1 // 1 B = 8 bits (4 sobram)
#define C_INDEX_KEY_SIZE 1 // 1 B = 8 bits
#define C_POINTER_OFFSET (C_POINTER_BLOCK_ID_SIZE + C_POINTER_BLOCK_OFFSET_SIZE) / 8 // 3 B = 24 bits
#define C_POINTER_BLOCK_ID_SIZE 16 // 16 bits
#define C_POINTER_BLOCK_OFFSET_SIZE 8 // 8 bits
#define C_INDEX_SET_DATA_SIZE 62 // 62 bits
#define C_INDEX_SET_DATA_FULL_BYTES C_INDEX_SET_DATA_SIZE / 8 // 7,75 -> 7

#define C_LEAF_DATA_SIZE 15 // 15 bits
#define C_LEAF_MORA_DATA_FLAG 1 // 1 bit


// Masks
#define C_BLOCK_TYPE_MAIN_TAB 0
#define C_BLOCK_TYPE_INDEX 1

// ### REGISTROS ###
// Importante: Atenção para fechar corretamente o tamanho das structs!!!


// Genéricos

struct rid_st {
	unsigned int valid:1;  // 1 bit = true/false
	unsigned int more :1;  // 1 bit = true/false
	unsigned int id   :30; // 30 bits -> 2^30 = 1.073.741.824
};
typedef struct rid_st rid_t;
typedef rid_t * rid_p;

typedef unsigned long long int data_def_t;
struct data_st {
	data_def_t valid:1;  // 1 bit = true/false
	data_def_t more :1;  // 1 bit = true/false
	data_def_t content:C_INDEX_SET_DATA_SIZE;  // 62 bits
};
typedef struct data_st data_t;
typedef data_t * data_p;

struct pointer_st {
	unsigned int valid:1;    // 1 bit   = true/false
	unsigned int more:1;	 // 1 bit   = true/false
	unsigned int id:20;     // 20 bits = 1.048.576
	unsigned int offset:10; // 10 bits = 1.024
}; // 32 bits
typedef struct pointer_st pointer_t;
typedef pointer_t * pointer_p;

struct block_header_st {
	unsigned char valid:1;
	unsigned char empty:1;
	unsigned char type :3;
	unsigned char 	   :3; // 3 bits sobram
};
typedef struct block_header_st block_header_t;
typedef block_header_t *	   block_header_p;

/*
// Indexes

struct index_data_st {
	unsigned char content; // 8 bits
};
typedef struct index_data_st index_data_t;


// Main index

struct index_entry_st {
	index_data_t data; // 8 bits
	pointer_t    pointer; // 24 bits
}; // 32 bits
typedef struct index_entry_st index_entry_t;

struct index_block_header_st {
	block_header_t header;
	index_entry_t  entry;
};
typedef struct index_block_header_st index_block_header_t;
typedef index_block_header_t *	     index_block_header_p;


// Index leaf

struct index_leaf_entry_st {
	unsigned short size:C_LEAF_DATA_SIZE; // 2^15 = 32768
	unsigned short more:C_LEAF_MORA_DATA_FLAG;
};
typedef struct index_leaf_entry_st index_leaf_entry_t;

struct leaf_block_header_st {
	block_header_t     header; // 8 bits
	index_leaf_entry_t entry; // 32 bits
}; // 40 bits
typedef struct leaf_block_header_st leaf_block_header_t;
typedef leaf_block_header_t *	    leaf_block_header_p;
*/

// ### TIPOS ###
typedef unsigned char * generic_pointer_p;

typedef long int block_offset_t;
typedef block_offset_t * block_offset_p;


// ### ROTINAS INTERNAS ###

// Inicialização
int initialize();
int bufferize();
void destroy();
int createStructure(int,generic_pointer_p);
int createDataDictionary();

// Controle
int insertData();

// Auxilares
generic_pointer_p blankBuffer();
void freeBuffer(generic_pointer_p);
int lockFreeBlock(block_offset_p);
int unlockBlock(block_offset_t);
int writeOnEmptyBlock(generic_pointer_p,block_offset_p);
int writeOnBlock(generic_pointer_p,block_offset_t);
int writeIt(generic_pointer_p);

