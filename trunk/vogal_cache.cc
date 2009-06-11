
#include "vogal_cache.h"

vogal_cache::vogal_cache(vogal_handler * handler){
	DBUG_ENTER("vogal_cache::vogal_cache");
	
	m_Handler = handler;
	m_Objects = NULL;
	m_Columns = NULL;
	m_FreeBlocks = llNew();
	m_LockedBlocks = llNew();

	DBUG_LEAVE;
}

vogal_cache::~vogal_cache(){
	DBUG_ENTER("vogal_cache::~vogal_cache");
	
	llFree(m_FreeBlocks);
	m_FreeBlocks = NULL;
	llFree(m_LockedBlocks);
	m_LockedBlocks = NULL;

	// Fechando tabelas
	if (m_Objects) {
		m_Objects->~ObjectCursorType();
		m_Objects = NULL;
	}
	if (m_Columns) {
		m_Columns->~ObjectCursorType();
		m_Columns = NULL;
	}

	DBUG_LEAVE;
}

GenericPointer vogal_cache::blankBuffer(){
	DBUG_ENTER("vogal_cache::blankBuffer");
	
	GenericPointer buf = (GenericPointer) malloc(C_BLOCK_SIZE);
	memset(buf, 0x00, C_BLOCK_SIZE);
	
	DBUG_RETURN(buf);
}

int vogal_cache::bufferize(){
	DBUG_ENTER("vogal_cache::bufferize");

	// Bufferizando blocos vazios de dados
	GenericPointer buf = blankBuffer();
	// Posiciona no primeiro bloco que não é de controle
	for (long int offset = C_COLUMNS_BLOCK + 1; m_Handler->getStorage()->readBlock(offset, buf); offset++) {
		BlockHeaderType * header = (BlockHeaderType *) buf;
		// Verifica se o primeiro bit está ativo, se não estive, está vazio
		if (!header->valid) {
			llPush(m_FreeBlocks, offset);
		}
	}
	
	// Lista blocos livres
	llDump(m_FreeBlocks);

	// Buffers inicializados com sucesso
	DBUG_RETURN(true);
}

void vogal_cache::freeBuffer(GenericPointer buf){
	DBUG_ENTER("vogal_cache::freeBuffer");
	
	if (buf) {
		free(buf);
	}
	
	DBUG_LEAVE;
}

int vogal_cache::lockFreeBlock(BlockOffset* pos){
	DBUG_ENTER("vogal_cache::lockFreeBlock");

	int ret = llPop(m_FreeBlocks, pos);
	if (ret) {
		ret = llPush(m_LockedBlocks, (*pos));
		if (!ret) {
			llPush(m_FreeBlocks, (*pos));
		}
	}

	DBUG_RETURN(ret);
}

int vogal_cache::unlockBlock(BlockOffset block) {
	DBUG_ENTER("vogal_cache::unlockBlock");
	
	llRemove(m_LockedBlocks, block);
	//llDump(lockedBlocks);
	
	DBUG_RETURN(true);
}

ObjectCursorType *vogal_cache::getObjects() {
	DBUG_ENTER("vogal_cache::getObjects");
	DBUG_RETURN(m_Objects);
}

ObjectCursorType *vogal_cache::getColumns() {
	DBUG_ENTER("vogal_cache::getColumns");
	DBUG_RETURN(m_Columns);
}

int vogal_cache::openDataDictionary() {
	DBUG_ENTER("vogal_cache::openDataDictionary");
	
	m_Objects = m_Handler->getDefinition()->openTable(C_OBJECTS);  
	if (!m_Objects)
		DBUG_RETURN(false);
	m_Columns = m_Handler->getDefinition()->openTable(C_COLUMNS); 
	if (!m_Columns)
		DBUG_RETURN(false);
		
	DBUG_RETURN(true);
}
