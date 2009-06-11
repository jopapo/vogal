
#include "DataLocationType.h"
#ifndef VOGAL_DATA_LOCATION_TYPE
#define VOGAL_DATA_LOCATION_TYPE

class DataLocationType {

public:
	DataLocationType() {
		DBUG_ENTER("DataLocationType::DataLocationType");
		id = 0;
		offset = 0;
		DBUG_LEAVE;
	}
	virtual ~DataLocationType() {
		DBUG_ENTER("DataLocationType::~DataLocationType");
		DBUG_LEAVE;
	}

	BlockOffset id;
	BlockOffset offset;
	
};

#endif
