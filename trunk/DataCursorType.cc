
#include "DataCursorType.h"

DataCursorType::DataCursorType() {
	DBUG_ENTER("DataCursorType::DataCursorType");
	
	column = NULL;
	allocSize = 0;
	usedSize = 0;
	content = NULL;
	blockId = 0;
	blockOffset = 0;
	
	DBUG_LEAVE;
}

DataCursorType::~DataCursorType() {
	DBUG_ENTER("DataCursorType::~DataCursorType");
	
	// A propriedade column não deve ser limpa pois tem outro owner só é associada ao dado
	if (content) {
		free(content);
		content = NULL;
		allocSize = 0;
	}
	
	DBUG_LEAVE;
}
