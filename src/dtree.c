/*
 *      dtree.c
 *      
 *      Copyright 2009 João Paulo <jopapo@jopapo-nb-ubuntu>
 *      
 *      This program is free software; you can redistribute it and/or modify
 *      it under the terms of the GNU General Public License as published by
 *      the Free Software Foundation; either version 2 of the License, or
 *      (at your option) any later version.
 *      
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU General Public License for more details.
 *      
 *      You should have received a copy of the GNU General Public License
 *      along with this program; if not, write to the Free Software
 *      Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 *      MA 02110-1301, USA.
 */

#include "dtree.h"

// ### MACROS ###
#define BI2BO(x) x * C_BLOCK_SIZE
#define DEBUG(x) {      \
	time_t time;        \
	printf("%10s - %s\n", \
	  ctime(&time), x); \
	}
#define MIN(x,y) x < y?x:y


// ### Variáveis globais ###
FILE *pDb;

LinkedListRootType *freeBlocks = NULL, *lockedBlocks = NULL;
ObjectCursorType *objs = NULL, *cols = NULL;

// ### FUNÇÃO PRINCIPAL ###

int main(int argc, char** argv)
{
	
	/*if (argc <= 1) {
		printf("Argumentos:\n");
		printf("\t-r RESET : Recria banco;\n");
		printf("\t-t TEST : Teste geral;\n");
		printf("\n");
		printf("Temporários:\n");
		printf("\t-asd [data] : Add Short Data - Adicionar ao índice o dado [data];\n");
		printf("\t-rsd [data] : Add Short Data - Adicionar ao índice fixo no bloco 0 o dado [data];\n");
			   
		return C_EXIT_FAILURE;
	}
	
	for (int i = 1; i < argc; i++) {
		if (strcmp(argv[i],"-r")) {
			initialize();
		}
	}*/
	
	// Executa testes programados
	//tests();
	
	// Se o arquivo não existir
	if (!openDatabase()) {
		if (initialize())  {
			if (!openDatabase()) {
				perror("Erro ao abrir base!");
				return C_EXIT_FAILURE;
			}
			if (!createDataDictionary()) {
				perror("Erro ao criar dicionário de dados!");
				return C_EXIT_FAILURE;
			}
		}
		else {
			perror("Impossível inicializar base!");
			return C_EXIT_FAILURE;
		}
	}
	
	createTables();
	
	DEBUG("Finalizando aplicação!");
	closeDatabase();
	
	return C_EXIT_SUCCESS;
}

int openDatabase() {
	pDb = fopen(C_FILENAME, "r+");
	if (!pDb) {
		perror("Erro durante abertura da base!");
		return FALSE;
	}
	
	if (!bufferize()) {
		perror("Erro durante bufferização!");
		return FALSE;
	}
	
	// Verifica se os cursores do metadados estão carregados na memória
	DEBUG("Abrindo tabelas do metadados!");
	objs = openTable(C_OBJECTS);  
	if (!objs)
		return FALSE;
	cols = openTable(C_COLUMNS); 
	if (!cols)
		return FALSE;
	
	return TRUE;
}

void closeDatabase() {
	
	// Fechando tabelas
	closeTable(objs);
	closeTable(cols);

	// Limpando buffer de blocos livres
	DEBUG("Limpando buffers de blocos livres!");
	llFree(freeBlocks);
	llFree(lockedBlocks);
	
	DEBUG("Fechando base!");
	// Atualiza arquivo
	fclose(pDb);
	
}

int createTables() {
	int ret = FALSE;
	
	// Teste de criação de objetos
	StringTreeRoot* columns = stNew(FALSE, FALSE);
	stPut(columns, "CODIGO", C_TYPE_NUMBER);
	stPut(columns, "DESCRICAO", C_TYPE_VARCHAR);
	if (!newTable("TESTE", columns)) {
		perror("Impossível criar tabela, tabela já exite!");
		goto free;
	}
	
	ret = TRUE;
		
free:
	stFree(columns);
	DEBUG("Tabela criada!");

	return ret;
}

int readBlock(BlockOffset block, GenericPointer buffer) {
	if (!fseek(pDb, BI2BO(block), SEEK_SET)) // Zero é sucesso
		if (fread(buffer, C_BLOCK_SIZE, 1, pDb)) // Zero é erro
			return TRUE;
	return FALSE;
}


BlockCursorType * openBlock(BlockOffset block) {
	BlockCursorType * cursor = NULL;

	GenericPointer buffer = blankBuffer(); 
	if (!readBlock(block, buffer)) {
		perror("Erro ao ler bloco!");
		goto free;
	}
	
	cursor = malloc(sizeof(BlockCursorType)); 
	cursor->block = block;
	cursor->buffer = buffer;
	
	buffer = NULL;
	
free:
	freeBuffer(buffer);
	
	return cursor;
}

int closeBlock(BlockCursorType * cursor) {
	if (!cursor)
		return TRUE;
	freeBuffer(cursor->buffer);
	free(cursor);
	return TRUE;
}

int initialize() {

	FILE *pFile;

	DEBUG("Inicializando arquivo de dados!");
	pFile = fopen(C_FILENAME, "w");
	if (!pFile) {
		perror("Impossível inicializar arquivo de dados!");
		return FALSE;
	}

	DEBUG("Zerando dados do base!");
	int i;
	GenericPointer buf = blankBuffer();
	for (i = 0; i < C_MAX_BLOCKS; i++) {
		fwrite(buf, C_BLOCK_SIZE, 1, pFile);
	}
	freeBuffer(buf);

	DEBUG("Criou arquivo de 1mb vazio!");
	fclose(pFile);
	
	return TRUE;
	
}

int bufferize() {
	
	DEBUG("Bufferizando!");

	// Bufferizando blocos vazios de dados
	freeBlocks = llNew();
	GenericPointer buf = blankBuffer();
	long int offset = C_COLUMNS_BLOCK + 1; // Posiciona no primeiro bloco que não é de controle
	fseek(pDb, BI2BO(offset), SEEK_SET); 
	while (fread(buf, C_BLOCK_SIZE, 1, pDb)) {
		BlockHeaderType * header = (BlockHeaderType *) buf;
		// Verifica se o primeiro bit está ativo, se não estive, está vazio
		if (!header->valid) {
			llPush(freeBlocks, offset);
		}
		offset++;
	}
	lockedBlocks = llNew();
	
	// Lista blocos livres
	llDump(freeBlocks);

	// Buffers inicializados com sucesso
	DEBUG("Buffers inicializados com sucesso!");
	return TRUE;
}

int createStructure(int type, GenericPointer buf) {
	BlockHeaderType * index = (BlockHeaderType *) buf;
	
	index->valid = TRUE;
	index->type  = type;

	return TRUE;
}

int writeOnEmptyBlock(GenericPointer buf, BlockOffset* pBlock) {
	if (lockFreeBlock(pBlock))
		return writeOnBlock(buf, (*pBlock));
	return FALSE;
}

int writeOnBlock(GenericPointer buf, BlockOffset block) {
	// Grava no arquivo
	if (fseek(pDb, BI2BO(block), SEEK_SET)) // Zero é sucesso
		return FALSE;
	if (!fwrite(buf, C_BLOCK_SIZE, 1, pDb))
		return FALSE;
		
	// Remove da pilha de pendências
	llRemove(lockedBlocks, block);
	//llDump(lockedBlocks);
	
	return TRUE;
}

int writeIt(GenericPointer buf) {
	BlockOffset* pBlock;
	return writeOnEmptyBlock(buf, pBlock);
}

void freeBuffer(GenericPointer buf) {
	if (buf) {
		free(buf);
	}
}

int createDataDictionary() {
	int ret = FALSE;
	CursorType * objsCursor = NULL;
	CursorType * colsCursor = NULL;
	
	// ### OBJS ###
	
	// Cria objeto
	BlockOffset block;
	if (!createTableStructure(C_OBJECTS, NULL, &block))
		goto freeCreateDataDictionary; 
	
	// ### COLS ###
	
	// Cria objeto
	if (!createTableStructure(C_COLUMNS, NULL, &block))
		goto freeCreateDataDictionary; 
	
	// ### OBJS:DATA ###
	
	objsCursor = openCursor(objs, NULL);
	if (!objsCursor)
		goto freeCreateDataDictionary;

	// Pula o cabeçalho
	GenericPointer p = objsCursor->block->buffer;
	p+=sizeof(BlockHeaderType);
	
	int i;
	int objsRids[] = {1, 2};
	char * objsNames[] = {C_OBJECTS, C_COLUMNS};
	int colCounts[] = {C_OBJECTS_COLS_COUNT, C_COLUMNS_COLS_COUNT};
	int recordCounts[] = {2, 9};
	
	// Ponteiro do próximo bloco (Se é ele mesmo, não há pq preencher)
	if (!skipData(&p))
		goto freeCreateDataDictionary;

	// Grava a quantidade de registros no bloco
	if (!writeNumber(sizeof(objsRids) / sizeof(objsRids[0]), &p))
		goto freeCreateDataDictionary;

	// Grava registros:
	for (i = 0; i < sizeof(objsRids) / sizeof(objsRids[0]); i++) {
		RidCursorType rid;
		rid.rid = objsRids[i];
		rid.dataList = createObjectData(objsNames[i], C_OBJECT_TYPE_TABLE, objsCursor->block->block, objsCursor->block->block, colCounts[i], recordCounts[i]);
		if (!rid.dataList) 
			goto freeCreateDataDictionary;
		if (!writeRid(objsCursor, &rid)) {
			stFree(rid.dataList);
			goto freeCreateDataDictionary;
		}
		stFree(rid.dataList);
	}
	
	
	// ### COLS:DATA ###
	
	colsCursor = openCursor(cols, NULL);
	if (!colsCursor)
		goto freeCreateDataDictionary;
	
	// Pula o cabeçalho
	p = colsCursor->block->buffer;
	p+=sizeof(BlockHeaderType);
	
	int colsRids = 1;
	int colsTabIds[] = {objsRids[0], objsRids[0], objsRids[0], objsRids[0], objsRids[0], objsRids[0], objsRids[1], objsRids[1], objsRids[1], objsRids[1]};
	int colsIds[] = {1, 2, 3, 4, 5, 6, 1, 2, 3, 4};
	char * colsNames[] = C_COLUMNS_NAMES;
	char * colsTypes[] = C_COLUMNS_TYPES;
	
	// Ponteiro do próximo bloco (Se é ele mesmo, não há pq preencher)
	if (!skipData(&p))
		goto freeCreateDataDictionary;

	// Grava a quantidade de registros no bloco
	if (!writeNumber(sizeof(colsTabIds) / sizeof(colsTabIds[0]), &p))
		goto freeCreateDataDictionary;

	// Grava registros:
	for (i = 0; i < sizeof(colsTabIds) / sizeof(colsTabIds[0]); i++) {
		RidCursorType rid;
		rid.rid = colsRids++;
		rid.dataList = createColumnData(colsTabIds[i], colsIds[i], colsNames[i], colsTypes[i]);
		if (!rid.dataList) 
			goto freeCreateDataDictionary;
		if (!writeRid(colsCursor, &rid)) {
			stFree(rid.dataList);
			goto freeCreateDataDictionary;
		}
		stFree(rid.dataList);
	}

	// Efetiva gravação dos blocos
	if (!writeBlock(objsCursor->block))
		goto freeCreateDataDictionary;
	if (!writeBlock(colsCursor->block)) {
		// TODO (Pendência): Aqui deve ser limpado o cursor anterior pois já gravou
		goto freeCreateDataDictionary;
	}

	ret = TRUE;
	
freeCreateDataDictionary:
	closeCursor(objsCursor);
	closeCursor(colsCursor);	

	return ret;

}

int writeBlock(BlockCursorType* cursor) {
	return writeOnBlock(cursor->buffer, cursor->block);
}

DataTypes dataType(char * src) {
	if (!strcmp(src, C_TYPE_VARCHAR))
		return VARCHAR;
	if (!strcmp(src, C_TYPE_NUMBER))
		return NUMBER;
	if (!strcmp(src, C_TYPE_DATE))
		return DATE;
	if (!strcmp(src, C_TYPE_FLOAT))
		return FLOAT;
	perror("Tipo de dado não identificado!");
	return UNKNOWN;
}

int newTable(char* name, StringTreeRoot* columns) {
	
	// Verifica se a tabela existe
    ObjectCursorType * cursor = openTable(name); 
	if (cursor) {
		closeTable(cursor);
		return FALSE;
	}

	BlockOffset tableBlock;
	if (!createTableStructure(name, columns, &tableBlock))
		return FALSE; 
	
	return TRUE;
}

// Importante!!! Implementar mecanismo de redução do "size" para o mínimo possível, ou seja,
// qdo vier um long long int de 64 bits com o valor "100", reduzí-lo para 8 bits (1 byte), pois 
// é o suficiente para representar esse valor no banco.
// *OBSERVAÇÃO: APENAS ESTÁ FUNCIONANDO PARA CAMPOS COM NO MÁXIMO 127 BYTES*
// TODO (Limitação): Terminar rotina de gravação do tamanho dinâmico (Inclusive, permitir tamanhos maiores que *int*)
int writeDataSize(GenericPointer* dest, BigNumber size) {
	//do {
		VariableSizeType * var_p = (VariableSizeType *)(*dest);
		var_p->more = FALSE; 
		var_p->size = size;
		size = 0;
		(*dest)+=sizeof(VariableSizeType);
	//} while (size);*/
	return TRUE;
}

// *OBSERVAÇÃO: APENAS ESTÁ FUNCIONANDO PARA CAMPOS COM NO MÁXIMO 127 BYTES*
// Tamanho zero significa NULL
int readDataSize(GenericPointer * src, BigNumber * dest) {
	(*dest) = 0; // Primeiro zera a variável da recepção
	VariableSizeType * var_p = (VariableSizeType *)(*src);
	(*dest) = var_p->size;
	(*src)+=sizeof(VariableSizeType);
	return TRUE;	
}


int writeInt(int intValue, GenericPointer* where) {
	return writeData(where, (GenericPointer)&intValue, sizeof(intValue), NUMBER);
}

int writeNumber(BigNumber number, GenericPointer* where) {
	return writeData(where, (GenericPointer)&number, sizeof(number), NUMBER);
}

int writeString(char * strValue, GenericPointer* where) {
	return writeData(where, (GenericPointer)strValue, strlen(strValue), VARCHAR);
}

int writeBlockPointer(BlockOffset id, BlockOffset offset, GenericPointer* dest) {
	if (!writeNumber(id, dest))
		return FALSE;
	if (!writeNumber(offset, dest))
		return FALSE;
	return TRUE;
}

int writeAnyData(GenericPointer* dest, GenericPointer src, DataTypes dataType) {
	switch (dataType) {
		case VARCHAR:
			return writeString((char *)src, dest);
		case NUMBER:
			return writeNumber((*(BigNumber*)src), dest);
		default:
			perror("Tipo ainda não implementado");
			return FALSE;
	}
}

int writeData(GenericPointer* dest, GenericPointer src, BigNumber dataBytes, DataTypes dataType) {
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
	if (!writeDataSize(dest, dataBytes))
		return FALSE;
	// Grava o dado
	memcpy((*dest), src, dataBytes);
	(*dest) += dataBytes;
	
	return TRUE;
}

// dataType será relevante por causa da redução que se dá ao gravar (O retorno pode variar)
int readData(GenericPointer * src, GenericPointer dest, BigNumber dataBytes, BigNumber destSize, DataTypes dataType) {
	if (destSize < dataBytes) {
		perror("Espaço disponível para alocação insuficiente para o campo!");
		return FALSE;
	}
	memcpy(dest, (*src), dataBytes);
	(*src)+=dataBytes;
	return TRUE;
}

int readNumber(GenericPointer* src, BigNumber* number, BigNumber dataBytes) {
	(*number) = 0;
	return readData(src, (GenericPointer)number, dataBytes, sizeof(number), NUMBER);
}

int skipData(GenericPointer* src) {
	BigNumber dataBytes;
	if (!readDataSize(src, &dataBytes)) {
		perror("Erro ao ler o tamanho do número!");
		return FALSE;
	}
	(*src)+=dataBytes;
	return TRUE;
}

int skipAllData(GenericPointer* src, BigNumber count) {
	for (; count; count--)
		if (!skipData(src))
			return FALSE;
	return TRUE;
}

int readSizedNumber(GenericPointer* src, BigNumber* number) {
	BigNumber dataBytes;
	if (!readDataSize(src, &dataBytes)) {
		perror("Erro ao ler o tamanho do número!");
		return FALSE;
	}
	return readNumber(src, number, dataBytes);
}

int lockFreeBlock(BlockOffset* pos) {
	int ret = llPop(freeBlocks, pos);
	if (ret) {
		ret = llPush(lockedBlocks, (*pos));
		if (!ret) {
			llPush(freeBlocks, (*pos));
		}
	}
	return ret;
}

// Blocos de 0 a 8 são utilizados pelo dicionário de dados já na criação
int unlockBlock(BlockOffset pos) {
	if (!pos)
		return FALSE;
	int ret = llRemove(lockedBlocks, pos);
	if (ret) {
		ret = llPush(freeBlocks, pos);
	}
	return ret;
}

// Aqui deve ser criado um buffer para reaproveitamento e não realocação, ou seja, deixar uma heap pré-alocada
GenericPointer blankBuffer() {
	GenericPointer buf = (GenericPointer) malloc(C_BLOCK_SIZE);
	
	//DEBUG("Obtendo buffer vazio!");
	memset(buf, 0x00, C_BLOCK_SIZE);
	
	return buf;
}

// TODO (Pendência): Todo este processo de busca das tabelas e colunas deve ser bufferizado para melhorar o desempenho
//   e evitar acesso a disco desnecessariamente!
ObjectCursorType * openTable(char * tableName) {
	// Auxiliares
	int i;
	int ret = FALSE;
	
	CursorType * objsCursor = NULL;
	CursorType * colsCursor = NULL;
	
	// Começa a inicializar o cursor
	ObjectCursorType * cursor = malloc(sizeof(ObjectCursorType));
	cursor->name = tableName;

	// Se está tentando abrir a tabela de objetos, a inicialização deve eser manual pois não há donde obter sua definição
	if (!strcmp(tableName,C_OBJECTS)) {
		cursor->block = C_OBJECTS_BLOCK;
		cursor->colsList = stNew(FALSE, TRUE);
		if (!cursor->colsList)
			goto freeOpenTable;
		char * cols[] = C_COLUMNS_NAMES;
		char * types[] = C_COLUMNS_TYPES;
		for (i = 0; i < C_OBJECTS_COLS_COUNT; i++) {
			ColumnCursorType * column = malloc(sizeof(ColumnCursorType));
			column->id = i+1;
			column->name = cols[i];
			column->type = dataType(types[i]);
			if (!stPut(cursor->colsList, column->name, column))
				goto freeOpenTable;
		}
		ret = TRUE;
		goto freeOpenTable;
	}
	
	// Se está tentando abrir a tabela de colunas, a inicialização deve eser manual pois não há donde obter sua definição
	if (!strcmp(tableName,C_COLUMNS)) {
		cursor->block = C_COLUMNS_BLOCK;
		cursor->colsList = stNew(FALSE, TRUE);
		if (!cursor->colsList)
			goto freeOpenTable;
		char * cols[] = C_COLUMNS_NAMES;
		char * types[] = C_COLUMNS_TYPES;
		for (i = 0; i < C_COLUMNS_COLS_COUNT; i++) {
			ColumnCursorType * column = malloc(sizeof(ColumnCursorType));
			column->id = i+1;
			column->name = cols[i+C_OBJECTS_COLS_COUNT];
			column->type = dataType(types[i+C_OBJECTS_COLS_COUNT]);
			if (!stPut(cursor->colsList, column->name, column))
				goto freeOpenTable;
		}
		ret = TRUE;
		goto freeOpenTable;
	}
	
	if (!objs || !cols) {
		perror("Objetos básicos do metadados não foram inicializados!");
		goto freeOpenTable;
	} 
	
	// Senão, deve obter através das tabelas do dicionário de dados
	StringTreeRoot * tree = stNew(FALSE, FALSE);
	stPut(tree, C_NAME_KEY, tableName);
	objsCursor = openCursor(objs, tree);
	if (!objsCursor) {
		perror("Impossível abrir cursor dos objetos!");
		goto freeOpenTable;
	}

	// Procura a tabela	
	if (!fetch(objsCursor)) {
		perror("Tabela não encontrada!"); 
		goto freeOpenTable;
	}
	
	// Obtém as colunas da tablea
	tree = stNew(FALSE, FALSE);
	stPut(tree, C_TABLE_RID_KEY, &objsCursor->fetch->rid);
	colsCursor = openCursor(cols, tree);
	if (!colsCursor) {
		perror("Impossível abrir cursor das colunas!");
		goto freeOpenTable;
	}
	// Varre as colunas da tabela	
	cursor->colsList = stNew(FALSE, TRUE);
	if (!cursor->colsList)
		goto freeOpenTable;
	while (fetch(colsCursor)) {
		TreeNodeValue value;

		ColumnCursorType * column = malloc(sizeof(ColumnCursorType));

		if (!stGet(colsCursor->fetch->dataList, C_COLUMN_SID_KEY, &value))
			goto freeOpenTable;
		column->id = (* (BigNumber*) value );
		
		if (!stGet(colsCursor->fetch->dataList, C_NAME_KEY, &value))
			goto freeOpenTable;
		column->name = (char*) value; 
		
		if (!stGet(colsCursor->fetch->dataList, C_TYPE_KEY, &value))
			goto freeOpenTable;
		column->type = dataType( (char*) value );
		
		if (!stPut(cursor->colsList, column->name, column)) 
			goto freeOpenTable;
		
	}

	ret = TRUE;
	
freeOpenTable:
	closeCursor(objsCursor);
	closeCursor(colsCursor);
	if (!ret) {
		if (cursor)
			closeTable(cursor);
		cursor = NULL;
	}
	
	return cursor;
}

// Aqui está acessando direto o disco - Ajustar para obter da memória 
int fetch(CursorType * cursor) {
	int ret = FALSE;
	
	if (!cursor->hasMore) {
		perror("Não há mais dados");
		goto freeFetch; 
	}

	int validFetch = FALSE; 

	cursor->fetch = malloc(sizeof(RidCursorType));
	while (readRid(cursor, cursor->fetch)) {
		// Valido?
		validFetch = TRUE; // True pois se não houver filtro, o primeiro registro é válido
		
		// Processo de comparação por todos os campos do filtro
		StringTreeNode* iNode = NULL;
		StringTreeIterator* iter;
		if (cursor->filter)
			iter = stCreateIterator(cursor->filter, &iNode);
		while (iNode) {
			// Obtenção de informações
			TreeNodeValue value;
			if (!stGet(cursor->table->colsList, stNodeName(iNode), &value))
				goto freeFetch;    
			ColumnCursorType * column = value;
			if (!stGet(cursor->fetch->dataList, (char*) stNodeName(iNode), &value))
				goto freeFetch;    
			DataCursorType * data = value;

			// Comparação
			int comparision;
			switch (column->type) {
				case NUMBER:
					comparision = (BigNumber*)data->content - (BigNumber*) stNodeValue(iNode);
					break;
				case VARCHAR:
					// Comparação
					comparision = memcmp(data->content, stNodeValue(iNode), MIN(data->allocSize, strlen((char*)stNodeValue(iNode))));
					break;
				default:
					perror("Comparação não preparada para o tipo!");
					goto freeFetch;
			}
			if (comparision) {
				validFetch = FALSE;
				break;
			}
	
			iNode = stNext(iter);
		}
		stFreeIterator(iter);
		
		if (validFetch) {
			cursor->rowCount ++;
			break;
		}
		
	} 
	
	if (validFetch) {
		ret = TRUE;
	} else {
		cursor->hasMore = FALSE;
	}
	
freeFetch:
	return ret;
	
}

int readRid(CursorType * cursor, RidCursorType * rid) {
	int ret = FALSE;
	
	// Cria objeto RID
	if (!readSizedNumber(&cursor->offset, &rid->rid))
		goto freeReadRid;
		
	// Os RIDs devem iniciar de 1, caso seja zero é que os registros acabaram
	if (!rid->rid) {
		DEBUG("EOR");
		goto freeReadRid;
	}
	
	if (rid->dataList)
		stFree(rid->dataList);
	rid->dataList = stNew(FALSE, TRUE); 
		
	BigNumber fieldCount;
	if (!readDataSize(&cursor->offset, &fieldCount))
		goto freeReadRid;
	
	for (; fieldCount > 0; fieldCount--) {
		// Obtém o ID da coluna
		BigNumber colId;
		if (!readDataSize(&cursor->offset, &colId))
			goto freeReadRid;

		// Procura a coluna com determinado ID
		ColumnCursorType * column = NULL;
		
		StringTreeNode * iNode;
		StringTreeIterator * iter = stCreateIterator(cursor->table->colsList, &iNode);
		while (iNode) {
			column = (ColumnCursorType*) stNodeValue(iNode);
			if (column->id == colId) 
				break;
			iNode = stNext(iter);
		}
		stFreeIterator(iter);
		
		if (!iNode)
			goto freeReadRid;

		BigNumber colDataSize;			
		// Obtém tamanho do dado
		if (!readDataSize(&cursor->offset, &colDataSize)) {
			perror("Erro ao ler o tamanho do dado!");
			goto freeReadRid;
		}
		if (!colDataSize) {
			DEBUG("Dado nulo. Pulando pro próximo."); // Aqui deveria gerar erro por registros nulos nem serem gravados, porém em caso de atualização isso pode acontecer (supostamente)
			continue;
		}

		// Achou a coluna portanto, carrega conforme seu tipo
		DataCursorType * data = malloc(sizeof(DataCursorType));
		data->content = NULL;
		
		// Adiciona a lista de dados dos fetch 
		stPut(rid->dataList, column->name, data);
		
		// Carga e alocação dos dados
		switch (column->type) {
			case NUMBER:
				data->allocSize = sizeof(BigNumber); // Sempre é númeral máximo tratado, no caso LONG LONG INT (64BITS)  
				break;
			case VARCHAR:
				// Tamanho máximo permitido até o momento
				data->allocSize = colDataSize;
				break;
			default: // TODO (Pendência): Implementar comparação de todos os tipos de dados
				perror("Tipo de campo ainda não implementado!");
				goto freeReadRid;
		}
		data->content = malloc(data->allocSize);
		if (!readData(&cursor->offset, data->content, colDataSize, data->allocSize, column->type)) {
			perror("Erro ao ler dado do bloco!");
			goto freeReadRid;
		}
	}
	
	ret = TRUE;
	
freeReadRid:
	return ret;
	
}

CursorType * openCursor(ObjectCursorType * object, StringTreeRoot * filter) {
	int ret = FALSE;
	
	CursorType * cursor = malloc(sizeof(CursorType));
	cursor->fetch = NULL;
	cursor->table = object;
	cursor->filter = filter;
	cursor->rowCount = 0;
	cursor->hasMore = TRUE;
	cursor->block = NULL;

	cursor->block = openBlock(cursor->table->block);
	if (!cursor->block)
		goto freeOpenCursor;
	
	// Pula o cabeçalho
	cursor->offset = cursor->block->buffer;
	cursor->offset+=sizeof(BlockHeaderType);
	
	// Ponteiro do próximo bloco
	cursor->nextBlockOffset = cursor->offset;
	if (!readSizedNumber(&cursor->offset, &cursor->nextBlock))
		goto freeOpenCursor;

	// Lê a quantidade de registros no bloco
	cursor->blockRegCountOffset = cursor->offset;
	if (!readSizedNumber(&cursor->offset, &cursor->blockRegCount))
		goto freeOpenCursor;
		
	ret = TRUE;

freeOpenCursor:
	if (!ret) {
		closeBlock(cursor->block);
		free(cursor);
		cursor = NULL;
	}

	return cursor;	
}

int closeCursor(CursorType * cursor) {
	if (!cursor)
		return TRUE;
	if (cursor->fetch) {
		if (cursor->fetch->dataList)
			stFree(cursor->fetch->dataList);
		free(cursor->fetch);
	}
	/* A tabela não pode ser fechada pois o controle é outro
	if (cursor->table)
		closeTable(cursor->table);
	*/
	if (cursor->filter)
		stFree(cursor->filter);
	if (cursor->block) 
		closeBlock(cursor->block);
	free(cursor);
	return TRUE;
}

int closeTable(ObjectCursorType * object) {
	// TODO (Pendência): Implementar fechamento da tabela (Finalização dos ponteiros, remoção da lista e limpeza de memória)
	if (!object)
		return TRUE;

	// Fecha os buffers do cursors e o limpa da memória (junto com as colunas se houverem)
	if (object->colsList) {
		StringTreeNode* iNode;
		StringTreeIterator* iterator = stCreateIterator(object->colsList, &iNode);
		while (iNode) {
			ColumnCursorType * column = (ColumnCursorType *) stNodeValue(iNode); 
			
			free(column->name);
			//freeBuffer(column->buffer);
			free(column);
			
			iNode = stNext(iterator);
		}
		stFreeIterator(iterator);
	}
	//free(object->name);
	//freeBuffer(object->buffer);
	free(object);

	return TRUE;
}

int createTableStructure(char* name, StringTreeRoot* columns, BlockOffset* block) {
	int ret = FALSE;
	
	BlockCursorType *table = malloc(sizeof(BlockCursorType));
	
	int dict = FALSE;
	if (!strcmp(name, C_OBJECTS)) {
		(*block) = C_OBJECTS_BLOCK;
		dict = TRUE;
	} else if (!strcmp(name, C_COLUMNS)) {
		(*block) = C_COLUMNS_BLOCK;
		dict = TRUE;
	} else if (!lockFreeBlock(block)) {
		perror("Não existem blocos disponíveis para criação da tabela!");
		goto freeCreateTableStructure;
	}
	table->block = (*block);
	table->buffer = blankBuffer();

	// Criando estrutura da tabela	
	if (!createStructure(C_BLOCK_TYPE_MAIN_TAB, table->buffer))
		goto freeCreateTableStructure;
	
	// Pula o cabeçalho
	GenericPointer p = table->buffer;
	p+=sizeof(BlockHeaderType);
	
	// Ponteiro do próximo bloco (Se é ele mesmo, não há pq preencher)
	if (!writeNumber(0, &p))
		goto freeCreateTableStructure;

	// Grava a quantidade de registros no bloco
	if (!writeNumber(0, &p))
		goto freeCreateTableStructure;
		
	// Se não for do dicionário de dados, insere os registros
	if (!dict) {
		if ((!objs) || (!cols)) {
			perror("Dicionário de dados não inicializado corretamente!");
			goto freeCreateTableStructure;
		}
		
		// Prepara dados para inserção na tabela OBJS
		StringTreeRoot * dataList = createObjectData(name, C_OBJECT_TYPE_TABLE, table->block, table->block, stCount(columns), 0);
		if (!dataList) 
			goto freeCreateTableStructure;
		RidCursorType * rid = insertData(objs, dataList);
		if (!rid)
			goto freeCreateTableStructure;
			
		// Insere colunas na COLS
		StringTreeNode * iNode = NULL;
		StringTreeIterator * iter = stCreateIterator(columns, &iNode);
		int colId = 1;
		while (iNode) {			
			char* colName = (char*) stNodeName(iNode); 
			char* colType = (char*) stNodeValue(iNode);
			dataList = createColumnData(rid->rid, colId++, colName, colType);
			if (!dataList) 
				goto freeCreateTableStructure;
			RidCursorType * colRid = insertData(cols, dataList);
			if (!colRid)
				goto freeCreateTableStructure;
			iNode = stNext(iter);
		}
		stFreeIterator(iter);
		 
	}
	
	if (!writeBlock(table))
		goto freeCreateTableStructure;
		
	ret = TRUE;
	
freeCreateTableStructure:
	if (table) {
		// Desbloqueia blocos travados para gravação da tabela
		if (!ret && !dict)
			unlockBlock(table->block);
		closeBlock(table);
	}

	return ret;	
}

RidCursorType * insertData(ObjectCursorType* obj, StringTreeRoot* data) {
	RidCursorType * newRid = NULL;
	CursorType * cursor = NULL;
	
	cursor = openCursor(obj, NULL);	
	if (!cursor)
		goto freeInsertData;
		
	cursor->blockRegCount++;
	GenericPointer p = cursor->blockRegCountOffset;
	if (!writeNumber(cursor->blockRegCount, &p))
		goto freeInsertData;
	
	// Vai até o último registro da tabela:: Tem que otimizar para pegar a informação do LAST_BLOCK da tabela
	RidCursorType * lastRid = NULL;
	while (fetch(cursor)) {
		lastRid = cursor->fetch; 
	}
	
	// Se achou o último RID, gera um novo número
	newRid = malloc(sizeof(RidCursorType));
	if (lastRid)
		newRid->rid = lastRid->rid + 1;
	else
		newRid->rid = 1;
	newRid->dataList = data;
	
	if (!writeRid(cursor, newRid))	
		goto freeInsertData;
		
	return newRid;
	
freeInsertData:
	closeCursor(cursor);
	if (newRid)
		free(newRid);
		
	return NULL;

}

int writeRid(CursorType * cursor, RidCursorType * rid) {
	int ret = FALSE;
	
	// Os RIDs devem iniciar de 1
	if (rid->rid <= 0) {
		perror("Código RID inválido!");
		goto freeWriteRid;
	}

	// Escreve o número do RID
	if (!writeNumber(rid->rid, &cursor->offset))
		goto freeWriteRid;
	
	// Escreve a quantidade de dados no registro
	BigNumber fieldCount = 0;
	if (rid->dataList) 
		fieldCount = stCount(rid->dataList);
	if (!writeNumber(fieldCount, &cursor->offset))
		goto freeWriteRid;
	
	// Itera os dados do RID para gravar. Problema: Esta iteração ocorre por ordem alfabética
	// Obs: É checado o fieldCount pois pd haver casos em que a tabela só tenha registros nulos
	if (fieldCount) {
		StringTreeNode * iNode = NULL;
		StringTreeIterator * iter = stCreateIterator(rid->dataList, &iNode);
		while (iNode) {
			DataCursorType * data = (DataCursorType *) stNodeValue(iNode);
			// Grava o ID da coluna
			if (!writeDataSize(&cursor->offset, data->column->id))
				goto freeWriteRid;
			// Grava o dado
			if (!writeAnyData(&cursor->offset, data->content, data->column->type))
				goto freeWriteRid;
			// Próximo dado
			iNode = stNext(iter);
		}
		stFreeIterator(iter);
	}
	ret = TRUE;

freeWriteRid:

	return ret;

}

StringTreeRoot * createObjectData(char* name, char* type, BlockOffset location, BlockOffset lastBlock, BigNumber columnCount, BigNumber registryCount) {
	TreeNodeValue value;

	StringTreeRoot * dataList = stNew(FALSE, TRUE);

	if (!stGet(objs->colsList, C_NAME_KEY, &value))
		goto freeCreateObjectData;
	DataCursorType * data = malloc(sizeof(DataCursorType));
	data->column = value;
	data->allocSize = strlen(name);
	data->content = (GenericPointer) name;
	if (!stPut(dataList, data->column->name, data))
		goto freeCreateObjectData;

	if (!stGet(objs->colsList, C_TYPE_KEY, &value))
		goto freeCreateObjectData;
	data = malloc(sizeof(DataCursorType));
	data->column = value;
	data->content = (GenericPointer) type;
	data->allocSize = strlen(type);
	if (!stPut(dataList, data->column->name, data))
		goto freeCreateObjectData;

	if (!stGet(objs->colsList, C_LOCATION_KEY, &value))
		goto freeCreateObjectData;
	data = malloc(sizeof(DataCursorType));
	data->column = value;
	data->content = (GenericPointer) &location;
	data->allocSize = sizeof(location);
	if (!stPut(dataList, data->column->name, data))
		goto freeCreateObjectData;

	if (!stGet(objs->colsList, C_LAST_BLOCK_KEY, &value))
		goto freeCreateObjectData;
	data = malloc(sizeof(DataCursorType));
	data->column = value;
	data->content = (GenericPointer) &lastBlock;
	data->allocSize = sizeof(lastBlock);
	if (!stPut(dataList, data->column->name, data))
		goto freeCreateObjectData;
	
	if (!stGet(objs->colsList, C_COLUMN_COUNT_KEY, &value))
		goto freeCreateObjectData;
	data = malloc(sizeof(DataCursorType));
	data->column = value;
	data->content = (GenericPointer) &columnCount;
	data->allocSize = sizeof(columnCount);
	if (!stPut(dataList, data->column->name, data))
		goto freeCreateObjectData;

	if (!stGet(objs->colsList, C_RECORD_COUNT_KEY, &value))
		goto freeCreateObjectData;
	data = malloc(sizeof(DataCursorType));
	data->column = value;
	data->content = (GenericPointer) &registryCount;
	data->allocSize = sizeof(registryCount);
	if (!stPut(dataList, data->column->name, data))
		goto freeCreateObjectData;

	return dataList;
	
freeCreateObjectData:
	stFree(dataList);

	return NULL;
}

StringTreeRoot * createColumnData(BigNumber tableRid, BigNumber columnSid, char* name, char* type) {
	TreeNodeValue value;

	StringTreeRoot * dataList = stNew(FALSE, TRUE);

	if (!stGet(cols->colsList, C_TABLE_RID_KEY, &value))
		goto freeCreateColumnData;
	DataCursorType * data = malloc(sizeof(DataCursorType));
	data->column = value;
	data->content = (GenericPointer) &tableRid;
	data->allocSize = sizeof(tableRid);
	if (!stPut(dataList, data->column->name, data))
		goto freeCreateColumnData;

	if (!stGet(cols->colsList, C_COLUMN_SID_KEY, &value))
		goto freeCreateColumnData;
	data = malloc(sizeof(DataCursorType));
	data->column = value;
	data->content = (GenericPointer) &columnSid;
	data->allocSize = sizeof(columnSid);
	if (!stPut(dataList, data->column->name, data))
		goto freeCreateColumnData;

	if (!stGet(cols->colsList, C_NAME_KEY, &value))
		goto freeCreateColumnData;
	data = malloc(sizeof(DataCursorType));
	data->column = value;
	data->allocSize = strlen(name);
	data->content = (GenericPointer) name;
	if (!stPut(dataList, data->column->name, data))
		goto freeCreateColumnData;

	if (!stGet(cols->colsList, C_TYPE_KEY, &value))
		goto freeCreateColumnData;
	data = malloc(sizeof(DataCursorType));
	data->column = value;
	data->content = (GenericPointer) type;
	data->allocSize = strlen(type);
	if (!stPut(dataList, data->column->name, data))
		goto freeCreateColumnData;

	return dataList;
	
freeCreateColumnData:
	stFree(dataList);

	return NULL;
}

char * cloneString(char * from) {
	char * to = malloc(sizeof(char) * strlen(from));
	return strcpy(to, from);
}
