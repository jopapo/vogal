
#include "ColumnCursorType.h"
#include "vogal_utils.h"

ColumnCursorType::ColumnCursorType() {
	DBUG_ENTER("ColumnCursorType::ColumnCursorType");
	
	name = NULL;
	type = UNKNOWN;
	block = NULL;
	
	DBUG_LEAVE;
}

ColumnCursorType::~ColumnCursorType() {
	DBUG_ENTER("ColumnCursorType::~ColumnCursorType");

	if (block) {
		block->~BlockCursorType();
		block = NULL;
	}
		
	DBUG_LEAVE;
}
