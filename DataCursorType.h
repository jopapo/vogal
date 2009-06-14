
#ifndef VOGAL_DATA_CURSOR_TYPE
#define VOGAL_DATA_CURSOR_TYPE

class DataCursorType; // interrelacionamento da column com a data com a node
#include "ColumnCursorType.h"

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
	bool			  contentOwner;
	
};

#endif
