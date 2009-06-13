
#include "SearchInfoType.h"

SearchInfoType::SearchInfoType() {
	DBUG_ENTER("SearchInfoType::SearchInfoType");
	
	rootBlock = NULL;
	blocksList = NULL;
	findedBlock = NULL;
	findedNode = NULL;
	offset = 0;
	comparision = 0;

	DBUG_LEAVE;
}

SearchInfoType::~SearchInfoType() {
	DBUG_ENTER("SearchInfoType::~SearchInfoType");
	
	vlFree(&blocksList);
	if (findedNode)
		findedNode->~NodeType();

	DBUG_LEAVE;
}
