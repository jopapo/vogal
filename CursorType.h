
#ifndef VOGAL_CURSOR_TYPE
#define VOGAL_CURSOR_TYPE

#include "vogal_utils.h"
#include "ObjectCursorType.h"

class CursorType {

public:
	CursorType();
	virtual ~CursorType();

	ObjectCursorType *table;
	BigNumber		  lastRid;
	
};

#endif
