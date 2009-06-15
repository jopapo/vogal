
#include "FilterCursorType.h"

FilterCursorType::FilterCursorType() {
	DBUG_ENTER("FilterCursorType::FilterType");
	cursor = NULL;
	data = NULL;
	infoData = NULL;
	infoFetch = NULL;
	cursorOwner = false;
	reset();
	DBUG_LEAVE;
}

FilterCursorType::~FilterCursorType() {
	DBUG_ENTER("FilterCursorType::~FilterType");
	if (data)
		data->~DataCursorType();
	if (cursorOwner)
		if (cursor)
			cursor->~CursorType();
	reset();
	DBUG_LEAVE;
}

void FilterCursorType::reset() {
	DBUG_ENTER("FilterCursorType::reset");
	
	fetch = NULL;
	/*if (fetch)
		fetch->~RidCursorType();*/ // Liberado abaixo, no infoFetch
	opened = false;
	notFound = false;
 	count = 0;
	if (infoData) {
		infoData->~SearchInfoType();
		infoData = NULL;
	}
	if (infoFetch) {
		infoFetch->~SearchInfoType();
		infoFetch = NULL;
	}

	DBUG_LEAVE;
}
