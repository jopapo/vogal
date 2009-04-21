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

#define C_LEAF_DATA_SIZE 15 // 15 bits
#define C_LEAF_MORA_DATA_FLAG 1 // 1 bit

#define C_MAX_SIZE_PER_DATA_UNIT 127 // 7 bits = 128

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

// Aqui foi pensado em definir o número 255 (1111 1111) como identificador
// que o tamanho é maior, porém, essa metodologia ia trazer uma malefício mto 
// grande que seria. Se, por acaso, o tamanh fosse maior que 255, digamos
// 1000: Precisaríamos de, no mínimo 4 bytes para identificá-lo, pois ficaria algo
// semelhante a:
//   variable_size_st a = {1111 1111} -- 255
//   variable_size_st b = {1111 1111} -- 255
//   variable_size_st c = {1111 1111} -- 255
//   variable_size_st d = {0001 0100} -- 20 = total 1000B
// Mas com o indicador "more", podemos tratar a informação como binária, ou seja,
// poderíamos identificar até 14 bits de tamanho em apenas 2 bytes = 16384. Portanto
// o tamanho acima poderia ser identificado da forma que seja, tendo uma progressão
// geométrica ao invés de meramente aritmética:
//   variable_size_st a = {0000111, 1} 
//   variable_size_st b = {1101000, 0} -- 00001111101000 = 1000
// Enquanto a primeira forma gera uma maior aproveitamento do primeiro byte,
// a segunda forma gera uma desperdício extremamente menor em caso de tamanhos
// maiores. Resumindo, para representar o que a segunda forma pd com 2 bytes,
// a primeira forma necessitaria de 455 bytes.
struct variable_size_st {
	unsigned char size:7; // 0 - 127
	unsigned char more:1; // true/false
};

typedef unsigned char variable_size_data_st; // Normalmente será um array deste

/*struct data_st {
	unsigned char     valid:1;  // 1 bit = true/false
	unsigned char     more :1;  // 1 bit = true/false
	unsigned char     usedB:2;  // 2 bits = max 4 => Bytes usados no campo content
	unsigned char 	       :4;  // 4 bits livres
	unsigned long int content; // 32 bits
}; // 48 bits / 5 bytes

struct pointer_st {
	unsigned long int valid :1;    // 1 bit   = true/false
	unsigned long int more  :1;	 // 1 bit   = true/false
	unsigned long int id    :20;     // 20 bits = 1.048.576
	unsigned long int offset:8; // 10 bits = 1.024
}; // 32 bits / 4 bytes
*/

struct block_header_st {
	unsigned char valid:1;
	unsigned char empty:1;
	unsigned char type :3;
	unsigned char 	   :3; // 3 bits sobram
};

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
int writeVariableSizeData(generic_pointer_p*, generic_pointer_p, int);
int writeOnEmptyBlock(generic_pointer_p,block_offset_p);
int writeOnBlock(generic_pointer_p,block_offset_t);
int writeIt(generic_pointer_p);
int writeInt(int, generic_pointer_p*);
int writeString(char *, generic_pointer_p*);
int writeData(generic_pointer_p*, generic_pointer_p, int);
int writeRidPointer(block_offset_t, generic_pointer_p*);
int writeBlockPointer(block_offset_t, block_offset_t, generic_pointer_p*);
