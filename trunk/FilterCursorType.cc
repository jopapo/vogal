
#include "FilterCursorType.h"

FilterCursorType::FilterCursorType() {
	DBUG_ENTER("FilterCursorType::FilterType");
	opened = false;
	empty = false;
	cursor = NULL;
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
	/*if (fetch)
		fetch->~RidCursorType();*/ // Liberado abaixo, no infoFetch
	if (infoData)
		infoData->~SearchInfoType();
	if (infoFetch)
		infoFetch->~SearchInfoType();
	DBUG_LEAVE;
}
