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

#define C_OBJECTS_COLS_COUNT 5
#define C_COLUMNS_COLS_COUNT 4

#define C_COLUMNS_NAMES {C_NAME_KEY,"TYPE","COLUMN_COUNT","RECORD_COUNT","LAST_BLOCK", "TABLE_RID", "COLUMN_SID", "NAME", "TYPE"};
#define C_COLUMNS_TYPES {"VARCHAR","VARCHAR","NUMBER","NUMBER","NUMBER","NUMBER","NUMBER","VARCHAR","VARCHAR"};

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
	//GenericPointer    buffer;
	StringTreeRoot  * colsList; // <ColumnCursorType>
} ObjectCursorType;

typedef struct {
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
	BlockOffset  	   nextBlock;
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
int newTable(char*, PairListRoot*);
int createTableStructure(char*, PairListRoot*, BlockOffset*);
int openBlock(BlockOffset, BlockCursorType *);
int closeBlock(BlockCursorType *);
int openTable(char *, ObjectCursorType **);
int closeTable(ObjectCursorType *);
//RidCursorType * findRidInfo(BlockOffset, BigNumber);
int insertRawData(BlockOffset, PairListRoot*, BigNumber*);
//int insertRawColumn(BlockOffset, GenericPointer, DataTypes, BlockOffset, BlockPointerType *);
int findRidFromColumn(ObjectCursorType*, char *, void *, RidCursorType*);
int openCursor(ObjectCursorType *, StringTreeRoot *, CursorType *);
int fetch(CursorType *);

// Util	
DataTypes dataType(char *);
	
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

// Find
//BigNumber * findData(BlockOffset, GenericPointer, BigNumber, DataTypes);
//BigNumber * findString(BlockOffset, char *);
