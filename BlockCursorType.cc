
#include "BlockCursorType.h"
#include "vogal_cache.h"

BlockCursorType::BlockCursorType() {
	DBUG_ENTER("BlockCursorType::BlockCursorType");
	
	id = 0;
	buffer = NULL;
	nodesList = NULL;
	offsetsList = NULL;
	searchOffset = 0;
	
	DBUG_LEAVE;
}

BlockCursorType::~BlockCursorType() {
	DBUG_ENTER("BlockCursorType::~BlockCursorType");
	
	vogal_cache::freeBuffer(buffer);
	buffer = NULL;
	vlFree(&nodesList);
	vlFree(&offsetsList);
	
	DBUG_LEAVE;
}
