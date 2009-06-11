
#include "DataCursorType.h"

DataCursorType::DataCursorType() {
	DBUG_ENTER("DataCursorType::DataCursorType");
	
	column = NULL;
	allocSize = 0;
	usedSize = 0;
	content = NULL;
	location = NULL;
	
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
	if (location) {
		location->~DataLocationType();
		location = NULL;
	}
	
	DBUG_LEAVE;
}
