
#ifndef VOGAL_OBJECT_CURSOR_TYPE
#define VOGAL_OBJECT_CURSOR_TYPE

#include "BlockCursorType.h"

class ObjectCursorType {

public:
	ObjectCursorType();
	virtual ~ObjectCursorType();

	char 		    *name;
	ValueListRoot   *colsList; // <ColumnCursorType>
	BlockOffset      blockId;
	
};

#endif
