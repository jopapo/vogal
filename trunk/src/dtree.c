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
	
	/*
	// TESTE DE ÁRVORE
	StringTreeRoot * root = stNew(FALSE, FALSE);
	stPut(root, "Marcos", NULL);
	stPut(root, "Armero", NULL);
	stPut(root, "Danilo", NULL);
	stPut(root, "Maurício", NULL);
	stPut(root, "Fabinho Capixaba", NULL);
	stPut(root, "Pierre", NULL);
	stPut(root, "Sandro Silva", NULL);
	stPut(root, "Diego Souza", NULL);
	stPut(root, "Marquinhos", NULL);
	stPut(root, "Keirrison", NULL);
	stPut(root, "Williams", NULL);
	
	// TESTE DA ITERAÇÃO
	StringTreeNode* iNode;
	StringTreeIterator* iterator = stCreateIterator(root, &iNode);
	while (iNode) {
		printf("%s - %s\n", stNodeName(iNode), (char*) stNodeValue(iNode));
		iNode = stNext(iterator);
	}
	stFreeIterator(iterator);
	
	stFree(root);
	*/
	
	
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
	if (!openTable(C_OBJECTS, &objs))
		return FALSE;
	if (!openTable(C_COLUMNS, &cols))
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
	PairListRoot * pair = plNew();
	plAdd(pair, "CODIGO", "NUMBER");
	plAdd(pair, "DESCRICAO", "VARCHAR");
	if (!newTable("TESTE", pair)) {
		perror("Impossível criar tabela, tabela já exite!");
		goto free;
	}
	
	ret = TRUE;
		
free:
	plFree(pair);
	DEBUG("Tabela criada!");

	return ret;
}

int readBlock(BlockOffset block, GenericPointer buffer) {
	if (!fseek(pDb, BI2BO(block), SEEK_SET)) // Zero é sucesso
		if (fread(buffer, C_BLOCK_SIZE, 1, pDb)) // Zero é erro
			return TRUE;
	return FALSE;
}


int openBlock(BlockOffset block, BlockCursorType * cursor) {
	int ret = FALSE;

	GenericPointer buffer = blankBuffer(); 
	if (!readBlock(block, buffer)) {
		perror("Erro ao ler bloco!");
		goto free;
	}
	
	cursor->block = block;
	cursor->buffer = buffer;
	
	ret = TRUE;
	buffer = NULL;
	
free:
	freeBuffer(buffer);
	
	return ret;
}

int closeBlock(BlockCursorType * cursor) {
	cursor->block = 0;
	freeBuffer(cursor->buffer);
	cursor->buffer = NULL;
	return TRUE;
}

/*
RidCursorType * findRidInfo(BlockOffset block, BigNumber rid) {
	RidCursorType * ridCursor = NULL;
	
	GenericPointer buffer = blankBuffer();
	
	GenericPointer p = buffer;
	
	BlockHeaderType * header = (BlockHeaderType *)p;
	if (!header->valid) {
		perror("Bloco inválido para leitura do rid!");
		goto free;
	} 
	if (header->type != C_BLOCK_TYPE_MAIN_TAB) {
		perror("Bloco de tipo incorreto para leitura do rid!");
		goto free;
	}
	p+=sizeof(BlockHeaderType);

	// Lê o offset do próximo bloco
	BigNumber nextBlock;
	if (!readSizedNumber(&p, &nextBlock))
		goto free;
		
	BigNumber colCount;
	if (!readSizedNumber(&p, &colCount))
		goto free;
	
	// Ignora os ponteiros das raizes das colunas
	if (!skipAllData(&p, colCount))
		goto free;
		
	// Lê a quantidade de registros no bloco
	BigNumber regCount;
	if (!readSizedNumber(&p, &regCount))
		goto free;
		
	// Lê a quantidade de colunas, no caso da tabela OBJS (bloco 0), sempre haverá 1 a mais, que é a localização
	// da tabela
	int dataCount = colCount * 2; // BlockId & Offset
	if (!block)
		dataCount++;
	BigNumber * data = NULL;
		
	// Inicia a varredura
	while (regCount) {
		BigNumber ridData;
		if (!readSizedNumber(&p, &ridData))
			goto free;
		int comparision = ridData - rid;
		
		// Se achou...
		if (!comparision) {
			int i;
			data = malloc(dataCount * sizeof(BigNumber*));
			for (i = 0; i < dataCount; i++) 
				if (!readSizedNumber(&p, &data[i]))
					goto free; 
			
			ridCursor = malloc(sizeof(RidCursorType));
			ridCursor->rid = rid;
			ridCursor->dataCount = dataCount;
			ridCursor->data = data;
			
			break;
		}
		
		// Se o rid lido é maior que o solicitado, termina a busca
		if (comparision > 0)
			break;
			
		if (!skipAllData(&p, dataCount))
			goto free;
	}OBJECTS
		
free:
	if ((!ridCursor) && (data))
		free(data); 

	freeBuffer(buffer);
	
	return ridCursor;
}
*/

/*BigNumber * findString(BlockOffset block, char * str) {
	return findData(block, (GenericPointer)str, strlen(str), VARCHAR);	
}

// Return de RID os the data
BigNumber * findData(BlockOffset block, GenericPointer findData, BigNumber findDataSize, DataTypes findDataType) {
	BigNumber * ret = NULL;
	
	DEBUG("Buscando dado na tabela!");
	
	GenericPointer buf = blankBuffer();
	if (!readBlock(block, buf)) {
		perror("Erro ao ler o bloco da coluna de nomes das tabelas!");
		goto free;
	}
	
	GenericPointer p = buf;

	// Pula o cabeçalho
	BlockHeaderType * header = (BlockHeaderType *) p;
	// Verifica se o primeiro bit está ativo, se não estive, está vazio
	if (!header->valid) {
		perror("Bloco de da coluna é inválido!");
		goto free;
	}
	if (!((header->type == C_BLOCK_TYPE_INDEX_ROOT) ||
	      (header->type == C_BLOCK_TYPE_INDEX_LEAF))) {
		perror("Tipo de bloco da coluna é inválido!");
		goto free;
	}
	p+=sizeof(BlockHeaderType);
	
	// Variável para obter o tamanho das variáveis
	BigNumber dataSize;
	
	// Lê a quantidade de registros do bloco
	BigNumber regsQty;
	if (!readSizedNumber(&p, &regsQty)) {
		perror("Erro ao ler a quantidade de registros!");
		goto free;
	}
	
	if (!regsQty) {
		DEBUG("Nenhum registro neste bloco!");
		goto free;
	}

	// Campos para comparação
	BigNumber allocSize;
	GenericPointer genericData = NULL;
	switch (findDataType) {
		//case NUMBER:
		//	allocSize = sizeof(BigNumber); 
		//	break;
		case VARCHAR:
			// Tamanho máximo permitido até o momento
			allocSize = 128;
			break;
		default: // TODO (Pendência): Implementar comparação de todos os tipos de dados
			perror("Tipo de campo ainda não implementado!");
			goto free;
	}
	genericData = malloc(allocSize);  
	OBJECTS
	for (; regsQty > 0; regsQty--) {
		// Obtém tamanho do dado
		if (!readDataSize(&p, &dataSize)) {
			perror("Erro ao ler o tamanho do dado (2)!");
			goto free;
		}
		if (!dataSize) {
			DEBUG("Busca: Bloco nulo. Cancelando...");
			goto next_record;
		}
		
		if (dataSize > allocSize) {
			if (findDataType != VARCHAR) {
				perror("Erro devido a alocação inexata de memória");
				goto free;
			}
			
			DEBUG("Alocação insuficiente de memória para VARCHAR - Redimensionando");
			free(genericData);
			allocSize = dataSize;
			genericData = malloc(allocSize);
		}
		
		int comparision;
		switch (findDataType) {
			//case NUMBER:
				// Leitura
				//readNumber(&p, (BigNumber*)genericData, sizeof(genericData));
				// Comparação
				//comparision = (BigNumber*)genericData - cmpNumber;
				//break;
			case VARCHAR:
				// Leitura
				readData(&p, genericData, dataSize, sizeof(genericData), VARCHAR);
				// Comparação
				comparision = memcmp(findData, genericData, MIN(dataSize, findDataSize));
				break;
			default:
				perror("Comparação não preparada para o tipo!");
				goto free;
		}
		
	next_record:		
		// Rid do registro
		if (!readDataSize(&p, &dataSize)) {
			perror("Erro ao ler o tamanho do dado (3)!");
			goto free;
		}
		// Se não achou, continua
		if (comparision) {
			p+=dataSize;
			continue;
		}
			
		ret = malloc(sizeof(BigNumber));
		if (!readNumber(&p, ret, dataSize)) {
			perror("Erro durante a leitura do RID!");
			ret = NULL;
			goto free;
		}
		break;

	};
	
free:
	freeBuffer(buf);
	
	if (genericData)
		free(genericData);
	
	return ret;
}*/

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
	rewind(pDb); // Posiciona no início do arquivo
	long int offset = 0;
	while (fread(buf, C_BLOCK_SIZE, 1, pDb)) {
		BlockHeaderType * header = (BlockHeaderType *) buf;
		// Verifica se o primeiro bit está ativo, se não estive, está vazio
		if (!header->valid) {
			llPush(freeBlocks, offset);
		}
		offset++;
	}
	lockedBlocks = llNew();
	
	/* ### TESTE DA RANGE LINKED LIST ###
	llPush(freeBuffers, 1);
	llPush(freeBuffers, 7002);
	llPush(freeBuffers, 7000);
	llPush(freeBuffers, 6999);
	llPush(freeBuffers, 7001);
	llPush(freeBuffers, 3000);
	llPush(freeBuffers, 4097);
	//llPush(freeBuffers, 2);
	llPush(freeBuffers, 4096);
	llDump(freeBuffers);
	long int v;
	llPop(freeBuffers, &v);
	printf("%li\n", v);
	llPop(freeBuffers, &v);
	printf("%li\n", v);*/
	
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

/*int insertRawColumn(BlockOffset mainBlock, GenericPointer data, DataTypes dataType, BlockOffset rid, BlockPointerType * location) {
	int ret = FALSE;
	
	// Abre o bloco principal
	BlockCursorType cursor;
	if (openBlock(mainBlock, &cursor)) {

		// Validações	
		if (!cursor.header->valid) {
			perror("Bloco inválido!");
			goto close;
		}
		if (cursor.header->type != C_BLOCK_TYPE_INDEX_ROOT) {
			perror("Bloco de tipo inválido!");
			goto close;
		}
	
		// Pula o cabeçalho
		GenericPointer p = cursor.buffer;
		p+=sizeof(BlockHeaderType);
		
		// Lê a quantidade de registros
		BigNumber regCount;
		if (!readSizedNumber(&p, &regCount))
			goto close;
	
		// Varre para localizar um local livre
		int i;
		for (i = 0; i < regCount; i++) {
			// Pula dado
			if (!skipAllData(&p, 1 + // Dado
							1  // RID
							))
				goto close;
		}
		
		// Grava o registro
		if (!writeAnyData(&p, data, dataType))
			goto close;
		
		
		// Grava o bloco no arquivo
		if (!writeOnBlock(cursor.buffer, cursor.block))
			goto close;
		
		/// Retorna o local e o offset
		location->id = cursor.block;
		location->offset = regCount;
		
		ret = TRUE;
		
	} else {
		perror("Erro ao abrir o bloco!");
		goto free;
	}		
	
close:
	closeBlock(&cursor);
	
free:

	return ret;
}*/
	
int createDataDictionary() {
	
	int ret = FALSE;
	
	BlockCursorType *objsBlock = NULL, *colsBlock = NULL;
	
	// ### OBJS ###
	
	// Cria objeto
	objsBlock = malloc(sizeof(BlockCursorType));
	objsBlock->block = C_OBJECTS_BLOCK;
	objsBlock->buffer = blankBuffer();

	// Criando estrutura da tabela	
	if (!createStructure(C_BLOCK_TYPE_MAIN_TAB, objsBlock->buffer))
		goto free;
	
	// ### COLS ###
	
	// Cria objeto
	colsBlock = malloc(sizeof(BlockCursorType));
	colsBlock->block = C_COLUMNS_BLOCK;
	colsBlock->buffer = blankBuffer();

	// Criando estrutura da tabela	
	if (!createStructure(C_BLOCK_TYPE_MAIN_TAB, colsBlock->buffer))
		goto free;
	

	// ### OBJS:DATA ###
	
	// Pula o cabeçalho
	GenericPointer p = objsBlock->buffer;
	p+=sizeof(BlockHeaderType);
	
	int i;
	int objsRids[] = {1, 2};
	char * objsNames[] = {C_OBJECTS, C_COLUMNS};
	int colCounts[] = {C_OBJECTS_COLS_COUNT, C_COLUMNS_COLS_COUNT};
	int recordCounts[] = {2, 9};
	
	// Ponteiro do próximo bloco (Se é ele mesmo, não há pq preencher)
	if (!writeNumber(0, &p))
		goto free;

	// Grava a quantidade de registros no bloco
	if (!writeNumber(sizeof(objsRids) / sizeof(objsRids[0]), &p))
		goto free;

	// Grava registros:
	for (i = 0; i < sizeof(objsRids) / sizeof(objsRids[0]); i++) {
		int colId = 1;
		// Grava RID
		if (!writeNumber(objsRids[i], &p) ||
		// Grava quantidade de campos gravados
			!writeDataSize(&p, colCounts[0]) ||
		// Grava coluna NAME
			!writeDataSize(&p, colId++) || // Sid da coluna
			!writeString(objsNames[i], &p) || // Dado da coluna
		// Grava coluna TYPE
			!writeDataSize(&p, colId++) || // Sid da coluna
			!writeString("TABLE", &p) || // Dado da coluna
		// Grava coluna COLUMN_COUNT
			!writeDataSize(&p, colId++) || // Sid da coluna
			!writeNumber(colCounts[i], &p) || // Dado da coluna
		// Grava coluna RECORD_COUNT
			!writeDataSize(&p, colId++) || // Sid da coluna
			!writeNumber(recordCounts[i], &p) || // Dado da coluna
		// Grava coluna LAST_BLOCK
			!writeDataSize(&p, colId++) || // Sid da coluna
			!writeNumber(objsBlock->block, &p)) // Dado da coluna
			goto free;
	}
	
	
	// ### COLS:DATA ###
	
	// Pula o cabeçalho
	p = colsBlock->buffer;
	p+=sizeof(BlockHeaderType);
	
	int colsRids = 1;
	int colsTabIds[] = {objsRids[0], objsRids[0], objsRids[0], objsRids[0], objsRids[0], objsRids[1], objsRids[1], objsRids[1], objsRids[1]};
	int colsIds[] = {1, 2, 3, 4, 5, 1, 2, 3, 4};
	char * colsNames[] = {C_NAME_KEY,"TYPE","COLUMN_COUNT","RECORD_COUNT","LAST_BLOCK","TABLE_RID","COLUMN_SID",C_NAME_KEY,"TYPE"};
	char * colsTypes[] = {"VARCHAR","VARCHAR","NUMBER","NUMBER","NUMBER","NUMBER","NUMBER","VARCHAR","VARCHAR"};
	
	// Ponteiro do próximo bloco (Se é ele mesmo, não há pq preencher)
	if (!writeNumber(0, &p))
		goto free;

	// Grava a quantidade de registros no bloco
	if (!writeNumber(sizeof(colsTabIds) / sizeof(colsTabIds[0]), &p))
		goto free;

	// Grava registros:
	for (i = 0; i < sizeof(colsTabIds) / sizeof(colsTabIds[0]); i++) {
		int colId = 1;
		// Grava RID
		if (!writeNumber(colsRids++, &p) ||
		// Grava quantidade de campos gravados
			!writeDataSize(&p, colCounts[1]) ||
		// Grava coluna TABLE_RID
			!writeDataSize(&p, colId++) || // Sid da coluna
			!writeNumber(colsTabIds[i], &p) || // Dado da coluna
		// Grava coluna COLUMN_SID
			!writeDataSize(&p, colId++) || // Sid da coluna
			!writeNumber(colsIds[i], &p) || // Dado da coluna
		// Grava coluna NAME
			!writeDataSize(&p, colId++) || // Sid da coluna
			!writeString(colsNames[i], &p) || // Dado da coluna
		// Grava coluna TYPE
			!writeDataSize(&p, colId++) || // Sid da coluna
			!writeString(colsTypes[i], &p)) // Dado da coluna
			goto free;
	}
	
	// Efetiva gravação dos blocos
	if (!writeBlock(objsBlock))
		goto free;
	if (!writeBlock(colsBlock)) {
		// TODO (Pendência): Aqui deve ser limpado o cursor anterior pois já gravou
		goto free;
	}

	ret = TRUE;
	
free:
	closeBlock(objsBlock);
	closeBlock(colsBlock);

	return ret;

}

int writeBlock(BlockCursorType* cursor) {
	return writeOnBlock(cursor->buffer, cursor->block);
}

/*int insertRawData(BlockOffset block, PairListRoot * list, BigNumber * rid) {
	int ret = FALSE;
	
	// Abre o bloco principal
	BlockCursorType cursor;
	if (openBlock(mainBlock, &cursor)) {

		// Validações	
		if (!cursor.header->valid) {
			perror("Bloco inválido!");
			goto close;
		}
		if (cursor.header->type == C_BLOCK_TYPE_MAIN_TAB) {
			perror("Bloco de tipo inválido!");
			goto close;
		}
		
		// Pula as partes não importantes
		GenericPointer p = cursor.buffer;
		p+=sizeof(BlockHeaderType);
		
		// Ponteiro do próximo bloco
		BigNumber nextBlock;
		if (!readSizedNumber(&p, &nextBlock))
			goto close;
	
		// Obtém a quantidade de colunas do bloco
		BigNumber colCount;
		if (!readSizedNumber(&p, &colCount))
			goto close;
		
		// Obtém a quantidade de colunas fixas do bloco
		BigNumber fixedColCount;
		if (!readSizedNumber(&p, &fixedColCount))
			goto close;
		
		// Obtém os endereços das colunas
		int i;
		BlockOffset colBlocks[colCount];																								
		for (i = 0; i < colCount; i++) 
			if (!readSizedNumber(&p, &colBlocks[i]))
				goto close;
				
		// Obtém a quantidade de registros no bloco
		GenericPointer regCount_p = p;
		BigNumber regCount;
		if (!readSizedNumber(&p, &regCount))
			goto close;
	
		// Obtém o número do último RID
		GenericPointer lastRid_p = p;
		BigNumber lastRid;
		if (!readDataSize(&p, &lastRid))
			goto close;

		// Busca espaço livre no bloco (pula rids)
		for (i = regCount; i; i--)
			skipAllData(&p, 1 + //RID 
							plCount(list) //COLS (fixed + normal)
							);
		
		// Incrementa RID e contador de registros
		lastRid++;
		regCount++;

		// Grava as colunas fixas
		for (i = 0; i < fixedColCount; i++) {
			ListValueType a, b;
			if (!plGet(list, i, &a, &b))
				goto close;
			// Verifica o tipo do campo
			if (!writeAnyData(&p, a, dataType(b))) {
				perror("Erro durante a gravação do dado fixo!");
				goto close;
			}
		}
		
		// Preenchimento das colunas normais... :P
		for (i = fixedColCount; i < plCount(list); i++) {
			ListValueType a, b;
			if (!plGet(list, i, &a, &b))
				goto close;
			// Verifica o tipo do campo
			if (!writeAnyData(&p, a, dataType(b))) {
				perror("Erro durante a gravação do dado fixo!");
				goto close;
			}
			// Insere dado detalhe
			BlockPointerType location;
			//if (!insertRawColumn(colBlocks[i - fixedColCount], a, dataType(b), lastRid, &location))
			//	goto close;
			// Grava o dado local
			if (!writeNumber(location.id, &p))
				goto close;
			if (!writeNumber(location.offset, &p))
				goto close;			
		}
		
		// Grava RID incrementado
		if (!writeNumber(lastRid, &lastRid_p))
			goto close;
		
		// Grava contador de registros incrementado		
		if (!writeNumber(regCount, &regCount_p))
			goto close;
			
		// Grava o bloco
		if (!writeOnBlock(cursor.buffer, cursor.block))
			goto close;
		
		ret = TRUE;		
		
	} else {
		perror("Erro ao abrir o bloco!");
		goto free;
	}		
	
close:
	closeBlock(&cursor);
	
free:

	return ret;

}*/

DataTypes dataType(char * src) {
	if (!strcmp(src, "VARCHAR"))
		return VARCHAR;
	if (!strcmp(src, "NUMBER"))
		return NUMBER;
	if (!strcmp(src, "DATE"))
		return DATE;
	if (!strcmp(src, "FLOAT"))
		return FLOAT;
	perror("Tipo de dado não identificado!");
	return UNKNOWN;
}

int newTable(char* name, PairListRoot* columns) {
	
	// Verifica se a tabela existe
    ObjectCursorType * cursor;
	if (openTable(name, &cursor)) {
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
		if (ret) {
			(*pos) *= C_BLOCK_SIZE; // Converter para o offset (em bytes) no arquivo
		} else {
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
int openTable(char * tableName, ObjectCursorType ** tableCursor) {
	int ret = FALSE;
	int i;
	
	// Começa a inicializar o cursor
	(*tableCursor) = malloc(sizeof(ObjectCursorType));
	ObjectCursorType * cursor = (*tableCursor);
	cursor->name = tableName;

	// Se está tentando abrir a tabela de objetos, a inicialização deve eser manual pois não há donde obter sua definição
	if (!strcmp(tableName,C_OBJECTS)) {
		cursor->block = C_OBJECTS_BLOCK;
		cursor->colsList = stNew(FALSE, TRUE);
		if (!cursor->colsList)
			goto free;
		char * cols[] = C_COLUMNS_NAMES;
		char * types[] = C_COLUMNS_TYPES;
		for (i = 0; i < C_OBJECTS_COLS_COUNT; i++) {
			ColumnCursorType * column = malloc(sizeof(ColumnCursorType));
			column->id = i+1;
			column->name = cols[i];
			column->type = dataType(types[i]);
			if (!stPut(cursor->colsList, column->name, column))
				goto free;
		}
		ret = TRUE;
		goto free;
	}
	
	// Se está tentando abrir a tabela de colunas, a inicialização deve eser manual pois não há donde obter sua definição
	if (!strcmp(tableName,C_COLUMNS)) {
		cursor->block = C_COLUMNS_BLOCK;
		cursor->colsList = stNew(FALSE, TRUE);
		if (!cursor->colsList)
			goto free;
		char * cols[] = C_COLUMNS_NAMES;
		char * types[] = C_COLUMNS_TYPES;
		for (i = 0; i < C_COLUMNS_COLS_COUNT; i++) {
			ColumnCursorType * column = malloc(sizeof(ColumnCursorType));
			column->id = i+1;
			column->name = cols[i+C_OBJECTS_COLS_COUNT];
			column->type = dataType(types[i+C_OBJECTS_COLS_COUNT]);
			if (!stPut(cursor->colsList, column->name, column))
				goto free;
		}
		ret = TRUE;
		goto free;
	}
	
	if (!objs || !cols) {
		perror("Objetos básicos do metadados não foram inicializados!");
		goto free;
	} 
	
	// Senão, deve obter através das tabelas do dicionário de dados
	CursorType objsCursor;
	StringTreeRoot * tree = stNew(FALSE, FALSE);
	stPut(tree, C_NAME_KEY, tableName);
	if (!openCursor(objs, tree, &objsCursor)) {
		perror("Impossível abrir cursor!");
		goto free;
	}

	// Procura a tabela	
	if (!fetch(&objsCursor)) {
		perror("Tabela não encontrada!"); 
		goto free;
	}
	
	

	// AQUI!!!
	
	
	BlockCursorType objsBlock;
	if (!openBlock(C_OBJECTS_BLOCK, &objsBlock))
		goto free;
	cursor->block = objsBlock.block;
	// AQUI!!! TIRAR???
	//cursor->buffer = objsBlock.buffer;
	cursor->colsList = stNew(FALSE, TRUE);
	if (!cursor->colsList)
		goto free;
	char * cols[] = C_COLUMNS_NAMES;
	char * types[] = C_COLUMNS_TYPES;
	for (i = 0; i < C_OBJECTS_COLS_COUNT; i++) {
		ColumnCursorType * column = malloc(sizeof(ColumnCursorType));
		column->id = i+1;
		column->name = cols[i];
		column->type = dataType(types[i]);
		if (!stPut(cursor->colsList, column->name, column))
			goto free;
	}
	ret = TRUE;
	goto free;
	
	// Senão, deve obter através das tabelas do dicionário de dados
	//RidCursorType colsRid;
	//if (!findRidFromColumn(cols, C_NAME_KEY, C_OBJECTS, &objsRid))
		//goto free;
	
	
	ret = TRUE;
	
	//AQUI!!!
	
	 
	 
	/*
	// Preenchimento dos dados
	GenericPointer p = buf;
	
	// Pula o cabeçalho
	p+=sizeof(BlockHeaderType);
	
	// Pula o ponteiro para o bloco
	if (!skipData(&p)) {
		perror("Erro ao ler o ponteiro do proximo bloco!");
		goto free;
	}

	// Lê a quantidade de colunas do bloco
	BigNumber colsQty;
	if (!readSizedNumber(&p, &colsQty)) {
		perror("Erro durante a leitura da quantidade de colunas do registro!");
		goto free;
	} 
	if (colsQty != 1) {
		perror("Quantidade de colunas para a tabelas TABS é inválido!");
		goto free;
	}

	// Lê a localização do bloco da raiz da única coluna da tabelas TABS: name.
	// Le o tamanho do ponteiro
	BlockOffset blockId;
	if (!readSizedNumber(&p, &blockId)) {
		perror("Erro ao ler a localização da coluna name da tabela TABS!");
		goto free;
	}
	
	// Não há necessidade na leitura do restante
	
	DEBUG("Efetuando a busca do nome da tabela!");
	// Agora deve ser veita a busca do dado no block (conforme indexação)
	
	BigNumber * rid = findString(blockId, tableName);
	
	if (!rid) // Tabela não localizada
		goto free;
	
	// Se achou, obtém o bloco da tabela
	ridCursor = findRidInfo(C_TABLES_BLOCK, (*rid));
	if (!ridCursor) {
		perror("Impossível localizar o RID!");
		goto free;
	}
	// 3: TableLoc, ColLoc, ColOffset
	if (ridCursor->dataCount != 3) {
		perror("Quantidade de dados associados ao RID da tabelas OBJS é inválido!");
		goto free;
	}

	// TODO (Pendência): Se encontrou, blz... aqui que as colunas deveriam ter bufferização parcial para o fetch

	BigNumber tableBlock = ridCursor->data[0];
	tableBuffer = blankBuffer();
	// Lê o bloco da tabela para pegar inicializar dados das colunas
	if (!readBlock(tableBlock, tableBuffer)) {
		perror("Impossível ler bloco da tabela solicitada!");
		goto free;
	}
	 
	p = tableBuffer;
	// Pula o cabeçalho
	p+=sizeof(BlockHeaderType);
	// Pula o ponteiro para o bloco
	if (!skipData(&p)) {
		perror("Erro ao ler o tamanho do dado (1)!");
		goto free;
	}
	// Lê a quantidade de colunas do bloco
	if (!readSizedNumber(&p, &colsQty)) {
		perror("Erro durante a leitura da quantidade de colunas do registro da tabela solicitada!");
		goto free;
	} 
	
	object = malloc(sizeof(ObjectCursorType));
	object->name = malloc(strlen(tableName) + 1);
	strcpy(object->name, tableName);
	object->rootBlock = tableBlock;
	object->rootBuffer = tableBuffer;
	tableBuffer = NULL; // Para não sair da memória
	object->colCount = colsQty;
	object->cols = NULL; // Não precisa alocar esta informação neste momento
	*/
	
free:
    /*freeBuffer(tableBuffer);
	freeBuffer(buf);
	
	if (ridCursor) {
		for (; ridCursor->dataCount; ridCursor->dataCount--)
			free(&ridCursor->data[ridCursor->dataCount - 1]);
		free(ridCursor);
	}
	*/
	if (!ret)
		free(cursor);
	
	// TODO (Pendência): Implementar abertura da tabela (Inicialização dos ponteiros e adição em lista)
	return ret;
}

// Aqui está acessando direto o disco - Ajustar para obter da memória 
int fetch(CursorType * cursor) {
	int ret = FALSE;
	
	// Neste caso deve ser localizado o block e o primeiro registro do filtro
	if ((!cursor->fetch) && (cursor->hasMore)) {
		cursor->block = (BlockCursorType*) malloc(sizeof(BlockCursorType));
		if (!openBlock(cursor->table->block, cursor->block))
			goto freeFetch;
		
		// Pula o cabeçalho
		cursor->offset = cursor->block->buffer;
		cursor->offset+=sizeof(BlockHeaderType);
		
		// Ponteiro do próximo bloco
		if (!readSizedNumber(&cursor->offset, &cursor->nextBlock))
			goto freeFetch;
	
		// LẼ a quantidade de registros no bloco
		if (!readSizedNumber(&cursor->offset, &cursor->blockRegCount))
			goto freeFetch;
	}
	
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
		StringTreeNode* iNode;
		StringTreeIterator* iter = stCreateIterator(cursor->filter, &iNode);
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

int openCursor(ObjectCursorType * object, StringTreeRoot * filter, CursorType * cursor) {
	cursor->fetch = NULL;
	cursor->table = object;
	cursor->filter = filter;
	cursor->rowCount = 0;
	cursor->block = NULL;
	cursor->hasMore = TRUE;
	return TRUE;	
}

int closeTable(ObjectCursorType * object) {
	// TODO (Pendência): Implementar fechamento da tabela (Finalização dos ponteiros, remoção da lista e limpeza de memória)

	// Fecha os buffers do cursors e o limpa da memória (junto com as colunas se houverem)
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
	
	free(object->name);
	//freeBuffer(object->buffer);
	free(object);

	return TRUE;
}

int createTableStructure(char* name, PairListRoot* columns, BlockOffset* block) {
	int ret = FALSE;
	
	BlockCursorType *table = malloc(sizeof(BlockCursorType));
	
	int dict = FALSE;
	if (!strcmp(name, C_OBJECTS)) {
		table->block = C_OBJECTS_BLOCK;
		dict = TRUE;
	} else if (!strcmp(name, C_COLUMNS)) {
		table->block = C_COLUMNS_BLOCK;
		dict = TRUE;
	} else if (!lockFreeBlock(&table->block)) {
		perror("Não existem blocos disponíveis para criação da tabela!");
		goto freeCreateTableStructure;
	}
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
		
		insertData(objs, );

		// Insere registro da tabela
		
	// Pula o cabeçalho
	GenericPointer p = objsBlock->buffer;
	p+=sizeof(BlockHeaderType);
	
	int i;
	int objsRids[] = {1, 2};
	char * objsNames[] = {C_OBJECTS, C_COLUMNS};
	int colCounts[] = {C_OBJECTS_COLS_COUNT, C_COLUMNS_COLS_COUNT};
	int recordCounts[] = {2, 9};
	
	// Ponteiro do próximo bloco (Se é ele mesmo, não há pq preencher)
	if (!writeNumber(0, &p))
		goto free;

	// Grava a quantidade de registros no bloco
	if (!writeNumber(sizeof(objsRids) / sizeof(objsRids[0]), &p))
		goto free;

	// Grava registros:
	for (i = 0; i < sizeof(objsRids) / sizeof(objsRids[0]); i++) {
		int colId = 1;
		// Grava RID
		if (!writeNumber(objsRids[i], &p) ||
		// Grava quantidade de campos gravados
			!writeDataSize(&p, colCounts[0]) ||
		// Grava coluna NAME
			!writeDataSize(&p, colId++) || // Sid da coluna
			!writeString(objsNames[i], &p) || // Dado da coluna
		// Grava coluna TYPE
			!writeDataSize(&p, colId++) || // Sid da coluna
			!writeString("TABLE", &p) || // Dado da coluna
		// Grava coluna COLUMN_COUNT
			!writeDataSize(&p, colId++) || // Sid da coluna
			!writeNumber(colCounts[i], &p) || // Dado da coluna
		// Grava coluna RECORD_COUNT
			!writeDataSize(&p, colId++) || // Sid da coluna
			!writeNumber(recordCounts[i], &p) || // Dado da coluna
		// Grava coluna LAST_BLOCK
			!writeDataSize(&p, colId++) || // Sid da coluna
			!writeNumber(objsBlock->block, &p)) // Dado da coluna
			goto free;
	}
		
		
	}
	
	


	// ### COLUNAS ###

	//AQUI!!!

	/*for (i = 1; i < sizeof(blocks) / sizeof(blocks[0]); i++) {
		p = buffers[i];
		p+=sizeof(BlockHeaderType);

		// Grava a quantidade registros no bloco
		if (!writeNumber(0, &p))
			goto free;
			
	}

	// Grava blocos da tabela no arquivo 
	DEBUG("Gravando buffers no arquivo!");
	// Gravação dos blocos de dados
	for (i = 0; i < sizeof(buffers) / sizeof(buffers[0]); i++)
		writeOnBlock(buffers[i], blocks[i]);
	
    // Preeche os blocos de retorno
    (*tableBlock) = blocks[0];
    for (i = 1; i < sizeof(blocks) / sizeof(blocks[0]); i++)
    	colsBlocks[i - 1] = blocks[i];
    ret = TRUE;
*/
freeCreateTableStructure:
/*
	// Desbloqueia blocos travados para gravação da tabela
	if (!ret)
		for (i = 0; i < sizeof(blocks) / sizeof(blocks[0]); i++)
			unlockBlock(blocks[i]);

	for (i = 0; i < sizeof(buffers) / sizeof(buffers[0]); i++)
		freeBuffer(buffers[i]); 
*/
	return ret;	
}

