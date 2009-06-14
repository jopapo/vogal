
#include "FilterCursorType.h"

FilterCursorType::FilterCursorType() {
	DBUG_ENTER("FilterCursorType::FilterType");
	opened = false;
	cursor = NULL;
	column = NULL;
	data = NULL;
	fetch = NULL;
	infoData = NULL;
	infoFetch = NULL;
	DBUG_LEAVE;
}

FilterCursorType::~FilterCursorType() {
	DBUG_ENTER("FilterCursorType::~FilterType");
	if (data)
		data->~DataCursorType();
	if (cursor)
		cursor->~CursorType();
	if (fetch)
		fetch->~RidCursorType();
	if (infoData)
		infoData->~SearchInfoType();
	if (infoFetch)
		infoFetch->~SearchInfoType();
	DBUG_LEAVE;
}
