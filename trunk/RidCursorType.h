
#ifndef VOGAL_RID_CURSOR_TYPE
#define VOGAL_RID_CURSOR_TYPE

#include "DataCursorType.h"

class RidCursorType {

public:
	RidCursorType();
	virtual ~RidCursorType();

	BigNumber	   id;
	ValueListRoot *dataList; // <DataCursorType> 
	
};

#endif
