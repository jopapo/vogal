
#ifndef VOGAL_DEFINITION
#define VOGAL_DEFINITION

class vogal_definition; // Interrelacionamento com a vogal_handler

#include "vogal_utils.h"
#include "CursorType.h"
#include "vogal_handler.h"

class vogal_definition
{

public:
	vogal_definition(vogal_handler *handler);
	virtual ~vogal_definition();

	int createDataDictionary();
	CursorType * createTableStructure(char *name, PairListRoot *columns);
	int createStructure(int type, GenericPointer buf);
	
	ObjectCursorType * openTable(char * tableName);
	int parseBlock(CursorType * cursor, ColumnCursorType * column, BlockCursorType * block);

	int newTable(char* name, PairListRoot * columns);
	int dropTable(char* name);
	ColumnCursorType * findColumn(ObjectCursorType* table, char * colName);

private:
	vogal_handler *m_Handler;

};

#endif
