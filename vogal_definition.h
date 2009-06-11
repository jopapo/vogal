
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

	int newTable(char* name, PairListRoot * columns);

private:
	vogal_handler *m_Handler;

};

#endif
