
#ifndef VOGAL_CURSOR_TYPE
#define VOGAL_CURSOR_TYPE

#include "vogal_utils.h"
#include "ObjectCursorType.h"
#include "RidCursorType.h"

class CursorType {

public:
	CursorType();
	virtual ~CursorType();

	ObjectCursorType *table;
	RidCursorType 	 *fetch;
	PairListRoot 	 *filter; 
	BigNumber		  rowCount;
	BigNumber		  lastRid;
	int				  hasMore;
	
};

#endif
