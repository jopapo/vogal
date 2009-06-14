
#ifndef VOGAL_FILTER_CURSOR_TYPE
#define VOGAL_FILTER_CURSOR_TYPE

#include "CursorType.h"
#include "SearchInfoType.h"

class FilterCursorType {

public:
	FilterCursorType();
	virtual ~FilterCursorType();

	bool 			  opened;
	CursorType		 *cursor;
	ColumnCursorType *column;
	DataCursorType	 *data;
	RidCursorType 	 *fetch;
	SearchInfoType   *infoData;
	SearchInfoType   *infoFetch;
		
};

#endif
