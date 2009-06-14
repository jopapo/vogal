
#include "vogal_cache.h"

vogal_cache::vogal_cache(vogal_handler * handler){
	DBUG_ENTER("vogal_cache::vogal_cache");
	
	m_Handler = handler;
	m_FreeBlocks = llNew();
	m_LockedBlocks = llNew();

	DBUG_LEAVE;
}

vogal_cache::~vogal_cache(){
	DBUG_ENTER("vogal_cache::~vogal_cache");
	
	llFree(&m_FreeBlocks);
	llFree(&m_LockedBlocks);
	
	DBUG_LEAVE;
}

// Otimizar para trabalhar na HEAP com um certo tamanho pré-alocado
GenericPointer vogal_cache::blankBuffer(int size){
	DBUG_ENTER("vogal_cache::blankBuffer");
	
	GenericPointer buf = (GenericPointer) malloc(sizeof(unsigned char) * size);
	// Não precisaria mas é interessante para visibilidade do código binário gravado no arquivão
	memset(buf, 0x00, size); 
	
	DBUG_RETURN(buf);
}

int vogal_cache::bufferize(){
	DBUG_ENTER("vogal_cache::bufferize");

	// Bufferizando blocos vazios de dados
	GenericPointer buf = blankBuffer();
	// Posiciona no primeiro bloco que não é de controle
	for (long int offset = C_OBJECTS_COLS_COUNT + C_COLUMNS_COLS_COUNT + 2; m_Handler->getStorage()->readBlock(offset, buf); offset++) {
		BlockHeaderType * header = (BlockHeaderType *) buf;
		// Verifica se o primeiro bit está ativo, se não estive, está vazio
		if (!header->valid) {
			llPush(m_FreeBlocks, offset);
		}
	}
	freeBuffer(buf);
	
	// Lista blocos livres
	llDump(m_FreeBlocks);

	// Buffers inicializados com sucesso
	DBUG_RETURN(true);
}

void vogal_cache::freeBuffer(GenericPointer buf){
	DBUG_ENTER("vogal_cache::freeBuffer");
	
	if (buf)
		free(buf);
	
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

// TODO: Melhorar mecanismo de cache dos objetos do metadados!
ObjectCursorType *vogal_cache::openObjects() {
	DBUG_ENTER("vogal_cache::openObjects");
	DBUG_RETURN( m_Handler->getDefinition()->openTable(C_OBJECTS) );
}

ObjectCursorType *vogal_cache::openColumns() {
	DBUG_ENTER("vogal_cache::openColumns");
	DBUG_RETURN( m_Handler->getDefinition()->openTable(C_COLUMNS) );
}
