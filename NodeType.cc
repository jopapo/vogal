
#include "NodeType.h"

NodeType::NodeType() {
	DBUG_ENTER("NodeType::NodeType");

	rid = NULL;
	data = NULL;
	dataOwner = false;
	
	DBUG_LEAVE;
}

NodeType::~NodeType() {
	DBUG_ENTER("NodeType::~NodeType");

	// Nada é liberado pois não é dono de nada
	if (dataOwner && data)
		data->~DataCursorType();
	
	DBUG_LEAVE;
}
