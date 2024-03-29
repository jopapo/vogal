
#ifndef VOGAL_NODE_TYPE
#define VOGAL_NODE_TYPE

#include "RidCursorType.h"
#include "DataCursorType.h"
#include "BlockCursorType.h"

class NodeType {

public:
	NodeType();
	virtual ~NodeType();

	RidCursorType  *rid;
	DataCursorType *data;
	bool 		 	dataOwner;
	bool			deleted;
	bool 			inserted;
	
};

#endif
