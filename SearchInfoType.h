
#ifndef VOGAL_SEARCH_INFO_TYPE
#define VOGAL_SEARCH_INFO_TYPE

#include "vogal_utils.h"
#include "BlockCursorType.h"
#include "RidCursorType.h"

class SearchInfoType {

public:
	SearchInfoType();
	virtual ~SearchInfoType();

	BlockCursorType *rootBlock;
	ValueListRoot   *blocksList; // <BlockCursorType>
	BlockCursorType *findedBlock;
	RidCursorType   *findedRid;
	int				 offset;
	int				 comparision;
	
};

#endif
