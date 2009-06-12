
#ifndef VOGAL_BLOCK_CURSOR_TYPE
#define VOGAL_BLOCK_CURSOR_TYPE

#include "vogal_utils.h"

class BlockCursorType {

public:
	BlockCursorType();
	virtual ~BlockCursorType();

	BlockOffset id;
	union {
		GenericPointer   buffer;
		BlockHeaderType *header; // Apenas um ponteiro dentro do buffer
	};

	// Disponíveis apenas após o parse
	int freeSpace;
	ValueListRoot *ridsList; // <RidCursorType>
	ValueListRoot *offsetsList; // <BlockOffset>
	
};

#endif
