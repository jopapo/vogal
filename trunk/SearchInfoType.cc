
#include "SearchInfoType.h"

SearchInfoType::SearchInfoType() {
	DBUG_ENTER("SearchInfoType::SearchInfoType");
	
	rootBlock = NULL;
	blocksList = NULL;
	findedBlock = NULL;
	findedRid = NULL;
	offset = 0;
	comparision = 0;

	DBUG_LEAVE;
}

SearchInfoType::~SearchInfoType() {
	DBUG_ENTER("SearchInfoType::~SearchInfoType");
	
	vlFree(blocksList);
	if (findedRid)
		findedRid->~RidCursorType();

	DBUG_LEAVE;
}
