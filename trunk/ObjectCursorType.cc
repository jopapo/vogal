
#include "ObjectCursorType.h"
#include "vogal_utils.h"

ObjectCursorType::ObjectCursorType() {
	DBUG_ENTER("ObjectCursorType::ObjectCursorType");
	
	name = NULL;
	colsList = NULL;
	block = NULL;
	
	DBUG_LEAVE;
}

ObjectCursorType::~ObjectCursorType() {
	DBUG_ENTER("ObjectCursorType::~ObjectCursorType");
	
	// O nome não pode ser liberado pois tem outro dono
	if (colsList) {
		vlFree(colsList);
		colsList = NULL;
	}
	if (block) {
		block->~BlockCursorType();
		block = NULL;
	}
	
	DBUG_LEAVE;
}
