
#ifndef VOGAL_MANIPULATION
#define VOGAL_MANIPULATION

class vogal_manipulation; // Interrelacionamento com a vogal_handler

#include "vogal_utils.h"
#include "CursorType.h"
#include "SearchInfoType.h"
#include "NodeType.h"
#include "vogal_handler.h"

class vogal_manipulation
{

public:
	vogal_manipulation(vogal_handler *handler);
	virtual ~vogal_manipulation();
	
	CursorType * openCursor(ObjectCursorType * object, PairListRoot * filter);
	int fetch(CursorType * cursor);
	RidCursorType * readRid(CursorType * cursor);
	BigNumber nextRid(CursorType * cursor);

	NodeType * parseRecord(CursorType * cursor, ColumnCursorType * column, BlockCursorType * block, GenericPointer * offset);
	SearchInfoType * findNearest(CursorType * cursor, RidCursorType * rid2find, DataCursorType * data2find, BlockCursorType * rootBlock);
	int updateBlockBuffer(BlockCursorType * block);
	int updateLocation(CursorType * cursor, NodeType * node, BlockOffset blockId, BlockOffset blockOffset);
	
	BigNumber insertData(CursorType* cursor, ValueListRoot* data);
	int writeData(CursorType * cursor, RidCursorType * rid, DataCursorType * data);
	int writeRid(CursorType * cursor, RidCursorType * rid);
	int writeDataCursor(GenericPointer* dest, DataCursorType * data);

	ValueListRoot * createObjectData(CursorType * cursor, char* name, char* type, BlockOffset location);
	ValueListRoot * createColumnData(CursorType * cursor, BigNumber tableRid, char* name, char* type, BlockOffset location);
	ColumnCursorType * findColumn(CursorType * cursor, char * colName);

private:
	vogal_handler *m_Handler;

};

#endif
