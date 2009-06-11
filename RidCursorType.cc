
#include "RidCursorType.h"
#include "vogal_utils.h"

RidCursorType::RidCursorType() {
	DBUG_ENTER("RidCursorType::RidCursorType");
	
	id = 0;
	dataList = NULL;
	
	DBUG_LEAVE;
}

RidCursorType::~RidCursorType() {
	DBUG_ENTER("RidCursorType::~RidCursorType");
	
	vlFree(dataList);
	
	DBUG_LEAVE;
}
