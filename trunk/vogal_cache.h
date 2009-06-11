
#ifndef VOGAL_CACHE
#define VOGAL_CACHE

class vogal_cache; // Necessário pelo interrelacionamento com a vogal_handler

#include "vogal_utils.h"
#include "ObjectCursorType.h"
#include "vogal_handler.h"


class vogal_cache
{
public:
	vogal_cache(vogal_handler *handler);
	virtual ~vogal_cache();

	int bufferize();

	static GenericPointer blankBuffer();
	static void freeBuffer(GenericPointer buf);
	
	int lockFreeBlock(BlockOffset *pos);
	int unlockBlock(BlockOffset block);
	
	ObjectCursorType *getObjects();
	ObjectCursorType *getColumns();
	
	int openDataDictionary();

private:
	vogal_handler *m_Handler;

	LinkedListRootType *m_FreeBlocks;
	LinkedListRootType *m_LockedBlocks;

	ObjectCursorType *m_Objects;
	ObjectCursorType *m_Columns;

};

#endif
