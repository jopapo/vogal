
#include "ObjectCursorType.h"
#include "vogal_utils.h"

ObjectCursorType::ObjectCursorType() {
	DBUG_ENTER("ObjectCursorType::ObjectCursorType");
	
	name = NULL;
	colsList = NULL;
	blockId = 0;
	ridNumber = 0;
	
	DBUG_LEAVE;
}

ObjectCursorType::~ObjectCursorType() {
	DBUG_ENTER("ObjectCursorType::~ObjectCursorType");

	// O nome não pode ser liberado pois tem outro dono
	vlFree(&colsList);
	
	DBUG_LEAVE;
}
