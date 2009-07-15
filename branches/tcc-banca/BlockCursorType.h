
#ifndef VOGAL_BLOCK_CURSOR_TYPE
#define VOGAL_BLOCK_CURSOR_TYPE

#include "vogal_utils.h"

class BlockCursorType; // Necessário por causa do interrelacionamento NodeType->DataCursorType->ColumnCursorType
#include "NodeType.h"

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
	ValueListRoot *nodesList; // <NodeType>
	ValueListRoot *offsetsList; // <BlockOffset>
	int			   searchOffset;
	bool		   navigatedLeft;
	bool		   navigatedSelf;
	
};

#endif
