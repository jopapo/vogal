
#ifndef VOGAL_UTILS
#define VOGAL_UTILS

#include <my_global.h> // DBUG
#include <mysql_com.h> // Field types
#include "list.h"

#define VOGAL "vogal"
#define VOGAL_EXT ".VDB"

#define C_BLOCK_SIZE 1024

#define C_BLOCK_TYPE_MAIN_TAB 0
#define C_BLOCK_TYPE_MAIN_COL 1

#define C_OBJECTS_BLOCK 0
#define C_COLUMNS_BLOCK 1

#define C_OBJECTS "OBJS"
#define C_COLUMNS "COLS"
#define C_NAME_KEY "NAME"
#define C_TYPE_KEY "TYPE"
#define C_TABLE_RID_KEY "TABLE_RID"
#define C_LOCATION_KEY "LOCATION"

#define C_OBJECTS_COLS_COUNT 3
#define C_COLUMNS_COLS_COUNT 4

#define C_TYPE_VARCHAR "VARCHAR"
#define C_TYPE_NUMBER "NUMBER"
#define C_TYPE_DATE "DATE"
#define C_TYPE_FLOAT "FLOAT"
#define C_TYPE_UNKNOWN "UNKNOWN"

#define C_OBJECT_TYPE_TABLE "TABLE"

#define C_COLUMNS_NAMES {C_NAME_KEY,C_TYPE_KEY,C_LOCATION_KEY,C_TABLE_RID_KEY,C_NAME_KEY,C_TYPE_KEY,C_LOCATION_KEY};
#define C_COLUMNS_TYPES {C_TYPE_VARCHAR,C_TYPE_VARCHAR,C_TYPE_NUMBER,C_TYPE_NUMBER,C_TYPE_VARCHAR,C_TYPE_VARCHAR,C_TYPE_NUMBER};
	
// ### MACROS ###
#define MIN(x,y) x<y?x:y

// TODO: Melhorar rotina de comparação por causa da performance, aqui a comparação pode ser executada duas vezes.
//		 Por enquanto é necessário pois o dado pode ser grande demais para caber num inteiro
#define DIFF(a,b) a>b?1:a<b?-1:0

#define ERROR(msg) fprintf(stderr, msg)

// ### SUBTIPOS ###
typedef enum {NUMBER,FLOAT,DATE,VARCHAR,UNKNOWN} DataTypes;
typedef enum {EQ,BG,BGE,SM,SME,NONE} FilterTypes;

typedef unsigned long long int BigNumber;
typedef unsigned char * GenericPointer;
typedef BigNumber BlockOffset;

typedef struct {
	unsigned char valid:1;
	unsigned char type :3;
	unsigned char 	   :4; // 4 bits sobram
} BlockHeaderType;

// Declaração da classe
class vogal_utils
{

public:
	vogal_utils();
	virtual ~vogal_utils();

	static DataTypes str2type(char * str);
	static char* type2str(DataTypes type);
	static DataTypes myType2VoType(enum enum_field_types type);

	static char * cloneString(char * from);

};

#endif
