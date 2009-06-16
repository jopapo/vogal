
#include "vogal_storage.h"

#define C_FILENAME "data.vdb"

//#define C_MAX_BLOCKS 5000
#define C_MAX_BLOCKS 100 // PARA FACILITAR TESTES !!!!

#define BI2BO(x) x * C_BLOCK_SIZE

// Aqui foi pensado em definir o número 255 (1111 1111) como identificador
// que o tamanho é maior, porém, essa metodologia ia trazer uma malefício mto 
// grande que seria. Se, por acaso, o tamanh fosse maior que 255, digamos
// 1000: Precisaríamos de, no mínimo 4 bytes para identificá-lo, pois ficaria algo
// semelhante a:
//   variable_size_st a = {1111 1111} -- 255
//   variable_size_st b = {1111 1111} -- 255
//   variable_size_st c = {1111 1111} -- 255
//   variable_size_st d = {0001 0100} -- 20 = total 1000B
// Mas com o indicador "more", podemos tratar a informação como binária, ou seja,
// poderíamos identificar até 14 bits de tamanho em apenas 2 bytes = 16384. Portanto
// o tamanho acima poderia ser identificado da forma que seja, tendo uma progressão
// geométrica ao invés de meramente aritmética:
//   variable_size_st a = {0000111, 1} 
//   variable_size_st b = {1101000, 0} -- 00001111101000 = 1000
// Enquanto a primeira forma gera uma maior aproveitamento do primeiro byte,
// a segunda forma gera uma desperdício extremamente menor em caso de tamanhos
// maiores. Resumindo, para representar o que a segunda forma pd com 2 bytes,
// a primeira forma necessitaria de 455 bytes.
typedef struct {
	unsigned char size:7; // 0 - 127
	unsigned char more:1; // true/false
} VariableSizeType;


vogal_storage::vogal_storage(vogal_handler * handler){
	DBUG_ENTER("vogal_storage::vogal_storage");

	m_Handler = handler;
	m_DB = NULL;

	DBUG_LEAVE;
}

vogal_storage::~vogal_storage(){
	DBUG_ENTER("vogal_storage::~vogal_storage");

	closeDatabase();

	DBUG_LEAVE;
}

int vogal_storage::openDatabase(int mode) {
	DBUG_ENTER("vogal_storage::openDatabase");
	
	// Só abre se estiver fechado
	if (!isOpen()) {
		m_DB = my_open(C_FILENAME, O_RDWR | mode, MYF(0));
		if (!isOpen())
			DBUG_RETURN(false);
	}

	DBUG_RETURN(true);
}

bool vogal_storage::isOpen() {
	DBUG_ENTER("vogal_storage::isOpen");
	
	DBUG_RETURN(m_DB > 0);
}

void vogal_storage::closeDatabase() {
	DBUG_ENTER("vogal_storage::closeDatabase");

	// Só fecha se estiver aberto
	if (!isOpen())
		DBUG_LEAVE;
	
	// Atualiza arquivo
	my_close(m_DB, MYF(MY_WME));
	m_DB = NULL;
	
	DBUG_LEAVE;
}

int vogal_storage::initialize() {
	DBUG_ENTER("vogal_storage::initialize");

	if (!openDatabase(O_CREAT)) {
	  my_error(ER_CANT_OPEN_FILE, MYF(0), "Erro ao inicializar DB");
	  DBUG_RETURN(false);
	}

	int i;
	GenericPointer buf = vogal_cache::blankBuffer();
	for (i = 0; i < C_MAX_BLOCKS; i++) {
		writeOnBlock(buf, i);
	}
	vogal_cache::freeBuffer(buf);
	
	DBUG_RETURN(true);
	
}

int vogal_storage::writeOnBlock(GenericPointer buf, BlockOffset block){
	DBUG_ENTER("vogal_storage::writeOnBlock");
	
	// Posiciona no arquivo
	if (my_seek(m_DB, BI2BO(block), SEEK_SET, MYF(MY_WME)) == BI2BO(block))
		// Grava no arquivo
		if (my_write(m_DB, buf, C_BLOCK_SIZE, MYF(MY_WME)) == C_BLOCK_SIZE)
			// Remove da pilha de pendências
			if (m_Handler->getCache()->unlockBlock(block))
				DBUG_RETURN(true);
		
	
	DBUG_RETURN(false);
}

int vogal_storage::readBlock(BlockOffset block, GenericPointer buffer) {
	DBUG_ENTER("vogal_storage::readBlock");
	
	// Posiciona no arquivo
	if (my_seek(m_DB, BI2BO(block), SEEK_SET, MYF(MY_WME)) == BI2BO(block))
		// Lê do arquivo
		if (my_read(m_DB, buffer, C_BLOCK_SIZE, MYF(MY_WME)) == C_BLOCK_SIZE)
			DBUG_RETURN(true);

	DBUG_RETURN(false);
}

int vogal_storage::writeNumber(BigNumber number, GenericPointer* where){
	DBUG_ENTER("vogal_storage::writeNumber");
	
	int ret = writeData(where, (GenericPointer)&number, sizeof(BigNumber), NUMBER);
	
	DBUG_RETURN(ret);
}

int vogal_storage::writeData(GenericPointer* dest, GenericPointer src, BigNumber dataBytes, DataTypes dataType){
	DBUG_ENTER("vogal_storage::writeData");
	
	// Se é numérico, bytes zerados à direita significam zeros à esquerda, portanto, podem ser ignorados.
	switch (dataType) {
		case NUMBER:
			while ((dataBytes>1) && (!(*(src + dataBytes - 1))))
				dataBytes--;
			break;
		default:
			break;
	}
	// Grava o tamanho do dado
	if (!writeDataSize(dest, dataBytes)) {
		ERROR("Erro ao gravar tamanho do dado!");
		DBUG_RETURN(false);
	}
	// Grava o dado
	memcpy((*dest), src, dataBytes);
	(*dest) += dataBytes;
	
	DBUG_RETURN(true);
}

int vogal_storage::writeBlock(BlockCursorType* block){
	DBUG_ENTER("vogal_storage::writeBlock");

	int ret = writeOnBlock(block->buffer, block->id);

	DBUG_RETURN(ret);
}

BlockCursorType * vogal_storage::openBlock(BlockOffset id){
	DBUG_ENTER("vogal_storage::openBlock");

	BlockCursorType * block = NULL;

	GenericPointer buffer = vogal_cache::blankBuffer(); 
	if (!readBlock(id, buffer)) {
		ERROR("Erro ao ler bloco!");
		goto free;
	}
	
	block = new BlockCursorType(); 
	block->id = id;
	block->buffer = buffer;
	
	buffer = NULL;
	
free:
	vogal_cache::freeBuffer(buffer);
	
	DBUG_RETURN(block);
}

int vogal_storage::readSizedNumber(GenericPointer* src, BigNumber* number){
	DBUG_ENTER("vogal_storage::readSizedNumber");
	
	BigNumber dataBytes;
	if (!readDataSize(src, &dataBytes)) {
		ERROR("Erro ao ler o tamanho do número!");
		DBUG_RETURN(false);
	}
	
	DBUG_RETURN( readNumber(src, number, dataBytes) );
}

int vogal_storage::writeDataSize(GenericPointer* dest, BigNumber size){
	DBUG_ENTER("vogal_storage::writeDataSize");
	do {
		VariableSizeType * var_p = (VariableSizeType *)(*dest);
		var_p->size = size & (unsigned char)127; // Tamanho máximo
		size >>= 7; // Desloca 7 bits pra direita
		var_p->more = size > 0;
		(*dest)+=sizeof(VariableSizeType);
	} while (size > 0);
	DBUG_RETURN(true);
}

// TODO: Corrigir leitura sequenciais
int vogal_storage::readDataSize(GenericPointer* src, BigNumber* dest){
	DBUG_ENTER("vogal_storage::readDataSize");

	(*dest) = 0; // Zera geral para não interferir na operação lógica
	bool more = false;
	do {
		VariableSizeType * var_p = (VariableSizeType *)(*src);
		(*dest) |= var_p->size;
		more = var_p->more;
		(*src)+=sizeof(VariableSizeType);
		if (more)
			(*dest) <<= 7; // Desloca os bits 7 para esquerda
	} while (more);

	DBUG_RETURN(true);
}

int vogal_storage::readNumber(GenericPointer* src, BigNumber* number, BigNumber dataBytes){
	DBUG_ENTER("vogal_storage::readNumber");

	(*number) = 0;
	int ret = true;
	if (dataBytes)
		ret = readData(src, (GenericPointer) number, dataBytes, sizeof(BigNumber), NUMBER);

	DBUG_RETURN(ret);
}

int vogal_storage::readData(GenericPointer* src, GenericPointer dest, BigNumber dataBytes, BigNumber destSize, DataTypes dataType){
	DBUG_ENTER("vogal_storage::readData");
	
	// dataType será relevante por causa da redução que se dá ao gravar (O retorno pode variar)
	if (destSize < dataBytes) {
		ERROR("Espaço disponível para alocação insuficiente para o campo!");
		DBUG_RETURN(false);
	}

	memcpy(dest, (*src), dataBytes);
	(*src)+=dataBytes;

	DBUG_RETURN(true);
}

int vogal_storage::skipData(GenericPointer* src){
	DBUG_ENTER("vogal_storage::skipData");

	BigNumber dataBytes;
	if (!readDataSize(src, &dataBytes)) {
		ERROR("Erro ao ler o tamanho do número!");
		DBUG_RETURN(false);
	}
	(*src)+=dataBytes;

	DBUG_RETURN(true);
}

int vogal_storage::skipAllData(GenericPointer* src, BigNumber count){
	DBUG_ENTER("vogal_storage::skipAllData");

	for (; count; count--)
		if (!skipData(src))
			DBUG_RETURN(false);

	DBUG_RETURN(true);
}

int vogal_storage::skipDataSize(GenericPointer* src) {
	DBUG_ENTER("vogal_storage::skipDataSize");

	VariableSizeType * var_p;
	do {
		var_p = (VariableSizeType *)(*src);
		(*src)+=sizeof(VariableSizeType);
	} while (var_p->more);

	DBUG_RETURN(true);
}

int vogal_storage::writeAnyData(GenericPointer* dest, GenericPointer src, DataTypes dataType){
	DBUG_ENTER("vogal_storage::writeAnyData");

	switch (dataType) {
		case VARCHAR:
			DBUG_RETURN( writeString((char *)src, dest) );
		case NUMBER:
			DBUG_RETURN( writeNumber((*(BigNumber*)src), dest) );
	}

	ERROR("Tipo ainda não implementado");
	DBUG_RETURN(false);
}

int vogal_storage::writeString(char * strValue, GenericPointer* where){
	DBUG_ENTER("vogal_storage::writeString");

	int ret = writeData(where, (GenericPointer)strValue, strlen(strValue), VARCHAR);

	DBUG_RETURN(ret);
}

/*

int vogal_storage::writeBlockPointer(BlockOffset where, BlockOffset offset, GenericPointer* buf){
	DBUG_ENTER("vogal_storage::writeBlockPointer");
	DBUG_RETURN(0);
}

int vogal_storage::writeInt(int value, GenericPointer* dest){
	DBUG_ENTER("vogal_storage::writeInt");
	DBUG_RETURN(0);
}

int vogal_storage::writeIt(GenericPointer buf){
	DBUG_ENTER("vogal_storage::writeIt");
	DBUG_RETURN(0);
}

int vogal_storage::writeOnEmptyBlock(GenericPointer buf, BlockOffset* block){
	DBUG_ENTER("vogal_storage::writeOnEmptyBlock");
	DBUG_RETURN(0);
}

*/
