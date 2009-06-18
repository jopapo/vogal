
#include "SearchInfoType.h"

SearchInfoType::SearchInfoType() {
	DBUG_ENTER("SearchInfoType::SearchInfoType");
	
	rootBlock = NULL;
	blocksList = NULL;
	findedBlock = NULL;
	findedNode = NULL;
	notFound = false;
	
	DBUG_LEAVE;
}

SearchInfoType::~SearchInfoType() {
	DBUG_ENTER("SearchInfoType::~SearchInfoType");
	
	vlFree(&blocksList);
	/*if (findedNode) // Liberado na limpeza dos blocos
		findedNode->~NodeType();*/

	DBUG_LEAVE;
}
