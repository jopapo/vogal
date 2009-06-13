
#include "CursorType.h"
#include "vogal_utils.h"

CursorType::CursorType() {
	DBUG_ENTER("CursorType::CursorType");
	
	table = NULL;
	fetch = NULL;
	filter = NULL;
	rowCount = 0;
	hasMore = false;
	lastRid = 0;
	
	DBUG_LEAVE;
}

// TODO (Pendência): Implementar fechamento da tabela (Finalização dos ponteiros, remoção da lista e limpeza de memória)
CursorType::~CursorType() {
	DBUG_ENTER("CursorType::~CursorType");

	if (table) {
		table->~ObjectCursorType();
		table = NULL;
	}
	if (fetch) {
		fetch->~RidCursorType();
		fetch = NULL;
	}
	plFree(&filter);

	DBUG_LEAVE;
}
