
#ifndef VOGAL_COLUMN_CURSOR_TYPE
#define VOGAL_COLUMN_CURSOR_TYPE

#include "vogal_utils.h"

class ColumnCursorType {

public:
	ColumnCursorType(int id);
	virtual ~ColumnCursorType();

	char 			*name;
	DataTypes	     type;
	BlockOffset  	 blockId;

	int getId();
	
private:
	int 			 m_Id;

};

#endif
