
#ifndef VOGAL_RID_CURSOR_TYPE
#define VOGAL_RID_CURSOR_TYPE

#include "vogal_utils.h"

class RidCursorType {

public:
	RidCursorType();
	virtual ~RidCursorType();

	BigNumber	   id;
	ValueListRoot *dataList; // <DataCursorType>
	
};

#endif
