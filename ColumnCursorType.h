
#ifndef VOGAL_COLUMN_CURSOR_TYPE
#define VOGAL_COLUMN_CURSOR_TYPE

#include "BlockCursorType.h"
#include "vogal_utils.h"

class ColumnCursorType {

public:
	ColumnCursorType();
	virtual ~ColumnCursorType();

	char 			*name;
	DataTypes	     type;
	BlockCursorType *block;
	
};

#endif
