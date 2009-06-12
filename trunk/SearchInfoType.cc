
#include "SearchInfoType.h"

SearchInfoType::SearchInfoType() {
	rootBlock = NULL;
	blocksList = NULL;
	findedBlock = NULL;
	findedRid = NULL;
	offset = 0;
	comparision = 0;
}

SearchInfoType::~SearchInfoType() {
	vlFree(blocksList);
	if (findedRid)
		findedRid->~RidCursorType();
}
