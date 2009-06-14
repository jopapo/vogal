
#ifndef VOGAL_MANIPULATION
#define VOGAL_MANIPULATION

class vogal_manipulation; // Interrelacionamento com a vogal_handler

#include "vogal_utils.h"
#include "CursorType.h"
#include "SearchInfoType.h"
#include "NodeType.h"
#include "FilterCursorType.h"
#include "vogal_handler.h"

class vogal_manipulation
{

public:
	vogal_manipulation(vogal_handler *handler);
	virtual ~vogal_manipulation();
	
	CursorType * openCursor(ObjectCursorType * object);
	int fetch(FilterCursorType * filter);
	BigNumber nextRid(CursorType * cursor);
	int fillDataFromLocation(CursorType * cursor, DataCursorType * data);

	NodeType * parseRecord(CursorType * cursor, ColumnCursorType * column, BlockCursorType * block, GenericPointer * offset);
	int comparison(SearchInfoType * info, CursorType * cursor, RidCursorType * rid2find, DataCursorType * data2find, int startingOffset = 0);
	SearchInfoType * findNearest(CursorType * cursor, RidCursorType * rid2find, DataCursorType * data2find, BlockCursorType * rootBlock);
	int updateBlockBuffer(CursorType * cursor, BlockCursorType * block);
	int updateLocation(CursorType * cursor, NodeType * node, BlockOffset blockId, BlockOffset blockOffset);
	
	BigNumber insertData(CursorType* cursor, ValueListRoot* data);
	int writeData(CursorType * cursor, RidCursorType * rid, DataCursorType * data);
	int writeRid(CursorType * cursor, RidCursorType * rid);
	int writeDataCursor(GenericPointer* dest, DataCursorType * data);
	int removeFetch(FilterCursorType * filter);

	ValueListRoot * createObjectData(CursorType * cursor, char* name, char* type, BlockOffset location);
	ValueListRoot * createColumnData(CursorType * cursor, BigNumber tableRid, char* name, char* type, BlockOffset location);

private:
	vogal_handler *m_Handler;

};

#endif
