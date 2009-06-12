
#ifndef VOGAL_DATA_CURSOR_TYPE
#define VOGAL_DATA_CURSOR_TYPE

#include "ColumnCursorType.h"
#include "vogal_utils.h"

class DataCursorType {

public:
	DataCursorType();
	virtual ~DataCursorType();

	ColumnCursorType *column;
	BigNumber 		  allocSize;
	BigNumber 		  usedSize;
	GenericPointer    content;
	BlockOffset 	  blockId;
	BlockOffset 	  blockOffset;
	
};

#endif
