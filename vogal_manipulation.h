
#ifndef VOGAL_MANIPULATION
#define VOGAL_MANIPULATION

class vogal_manipulation; // Interrelacionamento com a vogal_handler

#include "vogal_utils.h"
#include "CursorType.h"
#include "SearchInfoType.h"
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

	RidCursorType * parseRecord(CursorType * cursor, ColumnCursorType * column, BlockCursorType * block, GenericPointer * offset, bool loadData);
	int parseBlock(CursorType * cursor, ColumnCursorType * column, BlockCursorType * block, bool loadData);
	SearchInfoType * findNearest(CursorType * cursor, RidCursorType * rid2find, DataCursorType * data2find, BlockCursorType * rootBlock);
	int updateBlockBuffer(BlockCursorType * block);
	
	RidCursorType * insertData(CursorType* cursor, ValueListRoot* data);
	int writeData(CursorType * cursor, RidCursorType * rid, DataCursorType * data);
	int writeRid(CursorType * cursor, RidCursorType * rid);
	int writeDataCursor(GenericPointer* dest, DataCursorType * data);

	ValueListRoot * createObjectData(char* name, char* type, BlockOffset location);
	ValueListRoot * createColumnData(BigNumber tableRid, char* name, char* type, BlockOffset location);
	ColumnCursorType * findColumn(ObjectCursorType* table, char * colName);

private:
	vogal_handler *m_Handler;

};

#endif
