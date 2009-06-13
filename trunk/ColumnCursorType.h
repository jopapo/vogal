
#ifndef VOGAL_COLUMN_CURSOR_TYPE
#define VOGAL_COLUMN_CURSOR_TYPE

class ColumnCursorType; // interrelacionamento do bloco com o nodo com a coluna
#include "BlockCursorType.h"

class ColumnCursorType {

public:
	ColumnCursorType(int id);
	virtual ~ColumnCursorType();

	char 			*name;
	DataTypes	     type;
	BlockCursorType *block;

	int getId();
	
private:
	int 			 m_Id;

};

#endif
