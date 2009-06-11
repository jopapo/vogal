#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "bool.h"
#include "list.h"


// ### CONTANTES ###

#define C_FILENAME "dtree.db"

#define C_EXIT_FAILURE EXIT_FAILURE
#define C_EXIT_SUCCESS EXIT_SUCCESS

// Blocks

// Observação: Aumentado para possibilitar a gravação do dicionário de dados sem dividir em blocos
#define C_BLOCK_SIZE 4096 // 4096 B = 4 KB
#define C_MAX_BLOCKS 5000

// Masks
#define C_BLOCK_TYPE_MAIN_TAB 0

#define C_OBJECTS_BLOCK 0
#define C_COLUMNS_BLOCK 1

#define C_OBJECTS "OBJS"
#define C_COLUMNS "COLS"
#define C_NAME_KEY "NAME"
#define C_TYPE_KEY "TYPE"
#define C_TABLE_RID_KEY "TABLE_RID"
#define C_COLUMN_SID_KEY "COLUMN_SID"
#define C_LOCATION_KEY "LOCATION"
#define C_LAST_BLOCK_KEY "LAST_BLOCK"
#define C_COLUMN_COUNT_KEY "COLUMN_COUNT"
#define C_RECORD_COUNT_KEY "RECORD_COUNT"

#define C_OBJECTS_COLS_COUNT 6
#define C_COLUMNS_COLS_COUNT 4

#define C_TYPE_VARCHAR "VARCHAR"
#define C_TYPE_NUMBER "NUMBER"
#define C_TYPE_DATE "DATE"
#define C_TYPE_FLOAT "FLOAT"

#define C_OBJECT_TYPE_TABLE "TABLE"

#define C_COLUMNS_NAMES {C_NAME_KEY,C_TYPE_KEY,C_LOCATION_KEY,C_LAST_BLOCK_KEY,C_COLUMN_COUNT_KEY,C_RECORD_COUNT_KEY,C_TABLE_RID_KEY, C_COLUMN_SID_KEY, C_NAME_KEY, C_TYPE_KEY};
#define C_COLUMNS_TYPES {C_TYPE_VARCHAR,C_TYPE_VARCHAR,C_TYPE_NUMBER,C_TYPE_NUMBER,C_TYPE_NUMBER,C_TYPE_NUMBER,C_TYPE_NUMBER,C_TYPE_NUMBER,C_TYPE_VARCHAR,C_TYPE_VARCHAR};

// ### SUBTIPOS ###
typedef enum {NUMBER,FLOAT,DATE,VARCHAR,UNKNOWN} DataTypes;

typedef unsigned char * GenericPointer;
typedef unsigned long long int BigNumber;
typedef BigNumber BlockOffset;


// ### REGISTROS ###
// Importante: Atenção para fechar corretamente o tamanho das structs!!!

// Genéricos

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
typedef struct {
	unsigned char size:7; // 0 - 127
	unsigned char more:1; // true/false
} VariableSizeType;

typedef struct {
	unsigned char valid:1;
	unsigned char type :3;
	unsigned char 	   :4; // 3 bits sobram
} BlockHeaderType;


// Internal

typedef struct {
	BlockOffset block;
	union {
		GenericPointer    buffer;
		BlockHeaderType * header; // Apenas um ponteiro dentro do buffer
	};
} BlockCursorType;
 

typedef struct {
	BigNumber	   id;
	char *         name;
	DataTypes	   type;
} ColumnCursorType;

typedef struct {
	char *            name;
	BlockOffset       block;
	StringTreeRoot  * colsList; // <ColumnCursorType>
} ObjectCursorType;

typedef struct {
	 ColumnCursorType * column;
	 BigNumber 		allocSize;
	 GenericPointer content;
} DataCursorType;

typedef struct {
	BigNumber 		  rid;
	StringTreeRoot  * dataList; // <DataCursorType> 
} RidCursorType;

typedef struct {
	ObjectCursorType * table;
	RidCursorType *    fetch;
	StringTreeRoot *   filter; 
	BigNumber		   rowCount;
	BlockCursorType	*  block;
	GenericPointer	   nextBlockOffset;
	BlockOffset  	   nextBlock;
	GenericPointer	   blockRegCountOffset;
	BigNumber		   blockRegCount;
	GenericPointer	   offset;
	int				   hasMore;
} CursorType;

typedef struct {
	BlockOffset id;
	BlockOffset offset;
} BlockPointerType;

// ### ROTINAS INTERNAS ###

// Inicialização
int openDatabase();
void closeDatabase();
int initialize();
int bufferize();
int createStructure(int, GenericPointer);
int createDataDictionary();

// Controle
int createTables();
int newTable(char*, StringTreeRoot*);
int createTableStructure(char*, StringTreeRoot*, BlockOffset*);
BlockCursorType * openBlock(BlockOffset);
int closeBlock(BlockCursorType *);
ObjectCursorType * openTable(char *);
int closeTable(ObjectCursorType *);
RidCursorType * insertData(ObjectCursorType*, StringTreeRoot*);
int findRidFromColumn(ObjectCursorType*, char *, void *, RidCursorType*);
CursorType * openCursor(ObjectCursorType *, StringTreeRoot *);
int closeCursor(CursorType *);
int fetch(CursorType *);
StringTreeRoot * createObjectData(char*, char*, BlockOffset, BlockOffset, BigNumber, BigNumber);
StringTreeRoot * createColumnData(BigNumber, BigNumber, char*, char*);	

// Util	
DataTypes dataType(char *);
char * cloneString(char *);
	
// Buffer
GenericPointer blankBuffer();
void freeBuffer(GenericPointer);

// Blocks
int lockFreeBlock(BlockOffset*);
int unlockBlock(BlockOffset);
int writeOnEmptyBlock(GenericPointer,BlockOffset*);
int writeOnBlock(GenericPointer,BlockOffset);
int writeIt(GenericPointer);
int writeBlock(BlockCursorType*);

// Read
int readDataSize(GenericPointer*, BigNumber*);
int readData(GenericPointer*, GenericPointer, BigNumber, BigNumber, DataTypes);
int readSizedNumber(GenericPointer*, BigNumber*);
int readNumber(GenericPointer*, BigNumber*, BigNumber);
int skipData(GenericPointer*);
int skipAllData(GenericPointer*, BigNumber);
int readRid(CursorType *, RidCursorType *);

// Write
int writeDataSize(GenericPointer*, BigNumber);
int writeData(GenericPointer*, GenericPointer, BigNumber, DataTypes);
int writeAnyData(GenericPointer*, GenericPointer, DataTypes);
int writeInt(int, GenericPointer*);
int writeNumber(BigNumber, GenericPointer*);
int writeString(char *, GenericPointer*);
int writeBlockPointer(BlockOffset, BlockOffset, GenericPointer*);
int writeRid(CursorType *, RidCursorType *);
