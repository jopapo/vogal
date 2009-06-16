
#include "ColumnCursorType.h"

ColumnCursorType::ColumnCursorType(int id) {
	DBUG_ENTER("ColumnCursorType::ColumnCursorType");

	m_Id = id;
	name = NULL;
	type = UNKNOWN;
	blockId = 0;
	
	DBUG_LEAVE;
}

ColumnCursorType::~ColumnCursorType() {
	DBUG_ENTER("ColumnCursorType::~ColumnCursorType");
	DBUG_LEAVE;
}

int ColumnCursorType::getId() {
	DBUG_ENTER("ColumnCursorType::getId");
	DBUG_RETURN(m_Id);
}
