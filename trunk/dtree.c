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

#define BI2BO(x) x * C_BLOCK_SIZE

// ### Variáveis globais ###
FILE *pDb;

linkedListRoot_p 
	freeBlocks,
	lockedBlocks;


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
	
	
	pDb = fopen(C_FILENAME, "r+");

	// Se o arquivo não existir
	if (!pDb) {
		if (initialize())  {
			pDb = fopen(C_FILENAME, "r+");
			if (!pDb) {
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
	
	if (!bufferize()) {
		perror("Erro durante bufferização!");
		return C_EXIT_FAILURE;
	}
	
	// Iniciando processo de gravação dos blocos
	insertData();
		
	DEBUG("Finalizando aplicação!");
	destroy();
	
	return C_EXIT_SUCCESS;
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
	generic_pointer_p buf = blankBuffer();
	for (i = 0; i < C_MAX_BLOCKS; i++) {
		fwrite(buf, C_BLOCK_SIZE, 1, pFile);
	}
	freeBuffer(buf);

	DEBUG("Criou arquivo de 1mb vazio!");
	fclose(pFile);
	
	return TRUE;
	
}

void destroy() {
	
	// Limpando buffer de blocos livres
	llFree(freeBlocks);
	llFree(lockedBlocks);
	
	DEBUG("Fechando base!");
	// Atualiza arquivo
	fclose(pDb);
	
}

int bufferize() {
	
	DEBUG("Bufferizando!");

	// Bufferizando blocos vazios de dados
	freeBlocks = llNew();
	generic_pointer_p buf = blankBuffer();
	rewind(pDb); // Posiciona no início do arquivo
	long int offset = 0;
	while (fread(buf, C_BLOCK_SIZE, 1, pDb)) {
		struct block_header_st * header = (struct block_header_st *) buf;
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

int createStructure(int type, generic_pointer_p buf) {
	struct block_header_st * index = (struct block_header_st *) buf;
	
	index->valid = TRUE;
	index->type  = type;
	index->empty = TRUE;

	return TRUE;
}

int writeOnEmptyBlock(generic_pointer_p buf, block_offset_p pBlock) {
	if (lockFreeBlock(pBlock))
		return writeOnBlock(buf, (*pBlock));
	return FALSE;
}

int writeOnBlock(generic_pointer_p buf, block_offset_t block) {
	// Grava no arquivo
	fseek(pDb, block, SEEK_SET);
	fwrite(buf, C_BLOCK_SIZE, 1, pDb);

	// Remove da pilha de pendências
	llRemove(lockedBlocks, block / C_BLOCK_SIZE);
	llDump(lockedBlocks);
	
	return TRUE;
}

int writeIt(generic_pointer_p buf) {
	block_offset_p pBlock;
	return writeOnEmptyBlock(buf, pBlock);
}

void freeBuffer(generic_pointer_p buf) {
	if (buf) {
		free(buf);
	}
}

int createDataDictionary() {
	
	int i;
	int res = FALSE;
	
	#define C_OBJS_I 0
	#define C_COLS_I 1

	#define C_OBJS_NAME_I 0
	
	#define C_COLS_TABID_I 1
	#define C_COLS_NAME_I 2
	#define C_COLS_TYPE_I 3
	#define C_COLS_ORDER_I 4
	#define C_COLS_MANDATORY_I 5
	#define C_COLS_LOCATION_I 6
	
	#define C_COLS_FIRST_COL_I C_COLS_TABID_I
	#define C_COLS_LAST_COL_I C_COLS_LOCATION_I

	generic_pointer_p
		objs_p[] = {NULL, NULL},
		cols_p[] = {NULL, NULL, NULL, NULL, NULL, NULL, NULL};
	
	block_offset_t 
		objs_cols_loc[] = {2, 3, 4, 5, 6, 7, 8};

	DEBUG("Inicializando buffers do dicionário de dados");
	
	// Inicializa buffers das tabelas
	for (i = 0; i < sizeof(objs_p) / sizeof(objs_p[0]); i++) { 
		objs_p[i] = blankBuffer();
		if (!createStructure(C_BLOCK_TYPE_MAIN_TAB, objs_p[i]))
			goto free;
	}
	
	// Inicializa buffers das colunas
	for (i = 0; i < sizeof(cols_p) / sizeof(cols_p[0]); i++) { 
		cols_p[i] = blankBuffer();
		if (!createStructure(C_BLOCK_TYPE_INDEX, cols_p[i]))
			goto free;
	}
	
	// ### OBJS ###
	
	DEBUG("Gravando cabeçalho do bloco de rowids da tabela TABS");
	
	// Preenchimento dos dados
	generic_pointer_p p = objs_p[C_OBJS_I];
	p+=sizeof(struct block_header_st);

	// Ponteiro do último bloco (Se é ele mesmo, não há pq preencher)
	unsigned char id = 0, offset = 0;
	p = writeVariableSizeData(p, &id, 1);
	p = writeVariableSizeData(p, &offset, 1);
																								
	char * objsName[] = {"OBJS", "COLS"};

	DEBUG("Gravando cabeçalho rowids da tabela TABS");

	// RID dos primeiros registros: OBJS e COLS (Sempre começará do zero e nunca repetirá dentro da mesma tabela)
	for (i = 0; i < sizeof(objsName) / sizeof(objsName[0]); i++) {
		struct rid_st * rid = (struct rid_st *)p;
		rid->valid = TRUE;
		rid->more = FALSE;
		rid->id = i;
		p+=sizeof(struct rid_st);
		// Somente a coluna NAME da tabela OBJS
		// Ponteiro da coluna name associada ao RID
		id = objs_cols_loc[i];
		offset = 0;
		p = writeVariableSizeData(p, &id, 1);
		p = writeVariableSizeData(p, &offset, 1);
	}
	
	// Conteúdo da coluna name da OBJS
	p = cols_p[C_OBJS_NAME_I];
	p+=sizeof(struct block_header_st);
	
	DEBUG("Gravando a coluna name da tabela TABS");
/*
	for (i = 0; i < sizeof(objsName) / sizeof(objsName[0]); i++) {
		writeString(objsName[i], p);
		p+=sizeof(data_t);
		writeRidPointer(i, p);
		p+=sizeof(rid_t);
	}
*/
	/*
	// ### COLS ###
	
	DEBUG("Gravando a tabela COLS");

	// Preenchimento dos dados
	p = objs_p[C_COLS_I];
	p+=sizeof(block_header_t);

	// Ponteiro do último bloco (Se é ele mesmo, não há pq preencher)
	pon = (pointer_p)p;
	pon->valid = FALSE;
	pon->more = FALSE;
	pon->id = 0;
	pon->offset = 0;
	p+=sizeof(pointer_t);
	
	int colsTabIds[] = {0, 1, 1, 1, 1, 1, 1};
	char * colsName[] = {"name", "tabId", "name", "type", "order", "mandatory", "location"};
	char * colsType[] = {"VARCHAR", "RID", "VARCHAR", "VARCHAR", "NUMBER", "NUMBER", "NUMBER"};
	int colsOrder[] = {0, 0, 1, 2, 3, 4, 5};
	int colsMandatory[] = {1, 1, 1, 1, 1, 1, 1};

	// RID dos registros
	DEBUG("Gravando RIDs da tabela COLS");

	for (i = 0; i < sizeof(colsName) / sizeof(colsName[0]); i++) {
		rid_p rid = (rid_p)p;
		rid->valid = TRUE;
		rid->more = FALSE;
		rid->id = i;
		p+=sizeof(rid_t);
		
		
	}
	
	DEBUG("Obtendo blocos das colunas da tabela COLS");

	// 7 coluna, entre 1 e 7
	for (i = C_COLS_FIRST_COL_I; i <= C_COLS_LAST_COL_I; i++) {
			// Ponteiro das colunas associadas ao RID
		pon = (pointer_p)p;
		pon->valid = TRUE;
		pon->more = FALSE;
		pon->id = objs_cols_loc[i];
		pon->offset = 0;
		p+=sizeof(pointer_t);
	}
	
	// Conteúdo da coluna "tabsId" da COLS
	DEBUG("Gravando dados da coluna tabId da tabela COLS");
	p = cols_p[C_COLS_TABID_I];
	p+=sizeof(block_header_t);
	for (i = 0; i < sizeof(colsTabIds) / sizeof(colsTabIds[0]); i++) {
		writeInt(colsTabIds[i], p);
		p+=sizeof(data_t);
		writeRidPointer(i, p);
		p+=sizeof(rid_t);
	}
	// Conteúdo da coluna "name" da COLS
	DEBUG("Gravando dados da coluna name da tabela COLS");
	p = cols_p[C_COLS_NAME_I];
	p+=sizeof(block_header_t);
	for (i = 0; i < sizeof(colsName) / sizeof(colsName[0]); i++) {
		writeString(colsName[i], p);
		p+=sizeof(data_t);
		writeRidPointer(i, p);
		p+=sizeof(rid_t);
	}
	// Conteúdo da coluna "type" da COLS
	DEBUG("Gravando dados da coluna type da tabela COLS");
	p = cols_p[C_COLS_TYPE_I];
	p+=sizeof(block_header_t);
	for (i = 0; i < sizeof(colsType) / sizeof(colsType[0]); i++) {
		writeString(colsType[i], p);
		p+=sizeof(data_t);
		writeRidPointer(i, p);
		p+=sizeof(rid_t);
	}
	// Conteúdo da coluna "order" da COLS
	DEBUG("Gravando dados da coluna order da tabela COLS");
	p = cols_p[C_COLS_ORDER_I];
	p+=sizeof(block_header_t);
	for (i = 0; i < sizeof(colsOrder) / sizeof(colsOrder[0]); i++) {
		writeInt(colsOrder[i], p);
		p+=sizeof(data_t);
		writeRidPointer(i, p);
		p+=sizeof(rid_t);
	}
	// Conteúdo da coluna "mandatory" da COLS
	DEBUG("Gravando dados da coluna mandatory da tabela COLS");
	p = cols_p[C_COLS_MANDATORY_I];
	p+=sizeof(block_header_t);
	for (i = 0; i < sizeof(colsMandatory) / sizeof(colsMandatory[0]); i++) {
		writeInt(colsMandatory[i], p);
		p+=sizeof(data_t);
		writeRidPointer(i, p);
		p+=sizeof(rid_t);
	}
	
	DEBUG("Gravando buffers no arquivo!");
	// Gravação dos blocos de dados
	for (i = 0; i < sizeof(objs_p) / sizeof(objs_p[0]); i++)
		writeOnBlock(objs_p[i], BI2BO(i));
	for (i = 0; i < sizeof(cols_p) / sizeof(cols_p[0]); i++)
		writeOnBlock(cols_p[i], BI2BO(objs_cols_loc[i]));
	
	// Se chegou aqui, então sucesso!
	res = TRUE;
	*/
free:
	DEBUG("Liberando memória");
	
	// E então, libera memória
	for (i = 0; i < sizeof(objs_p) / sizeof(objs_p[0]); i++)
		freeBuffer(objs_p[i]);
	for (i = 0; i < sizeof(cols_p) / sizeof(cols_p[0]); i++)
		freeBuffer(cols_p[i]);

	return res;
	
}

// Importante!!! Implementar mecanismo de redução do "size" para o mínimo possível, ou seja,
// qdo vier um long long int de 64 bits com o valor "100", reduzí-lo para 8 bits (1 byte), pois 
// é o suficiente para representar esse valor no banco.
generic_pointer_p writeVariableSizeData(generic_pointer_p dest, generic_pointer_p src, int size) {
	do {
		struct variable_size_st * var_p = (struct variable_size_st *)dest;
		var_p->more = size > C_MAX_SIZE_PER_DATA_UNIT;
		if (var_p->more) {
			var_p->size = C_MAX_SIZE_PER_DATA_UNIT;
			size-=C_MAX_SIZE_PER_DATA_UNIT;
		} else {
			var_p->size = size;
			size = 0;
		}
		dest+=sizeof(struct variable_size_st);
		variable_size_data_st * varData_p = (variable_size_data_st *)dest;
		memcpy(varData_p, src, var_p->size);
		dest+=sizeof(struct variable_size_st);
		src+=var_p->size;
	} while (size);
	return dest;
}

generic_pointer_p writeInt(int intValue, generic_pointer_p where) {
	return writeData(where, (generic_pointer_p)&intValue, sizeof(int));
}

generic_pointer_p writeString(char * strValue, generic_pointer_p where) {
	return writeData(where, (generic_pointer_p)strValue, strlen(strValue));
}

generic_pointer_p writeData(generic_pointer_p dest, generic_pointer_p src, int dataSize) {
	dest = writeVariableSizeData(dest, (generic_pointer_p)&dataSize, sizeof(dataSize));
	memcpy(dest, src, dataSize);
	return dest+dataSize;
}

generic_pointer_p writeRidPointer(block_offset_t rid, generic_pointer_p where) {
	where = writeVariableSizeData(where, (generic_pointer_p)&rid, sizeof(rid));
	memcpy(where, &rid, sizeof(rid));
	return where+sizeof(rid);
}

int insertData() {
	DEBUG("Inserindo dados...");	

/*	short int value = 400; // bits 0000 0001 1001 0000

	// Bloco profundidade 2
	long int pos2 = 1024;
	generic_pointer_t buf2 = blankbuffer();
	leaf_block_header_p leaf = (leaf_block_header_p) buf2;
	
	leaf->header.notEmpty = C_BLOCK_NOT_EMPTY;
	leaf->header.type     = C_BLOCK_TYPE_INDEX_LEAF;
	
	leaf->entry.size = 1; // Restante do valor do campo
	leaf->entry.more = 0;
	
	// Aqui não é usado um struct pois o tamanho é variável
	memcpy(leaf + sizeof(leaf_block_header_t), &value + 1, leaf->entry.size);
	
	fseek(pDb, pos2, SEEK_SET);
	fwrite(buf2, C_BLOCK_SIZE, 1, pDb);
	
	freeBuffer(buf2);

	// Bloco profundidade 1
	long int pos1 = 0;

	generic_pointer_t buf = blankbuffer();
	index_block_header_p index = (index_block_header_p) buf;
	
	index->header.notEmpty = C_BLOCK_NOT_EMPTY;
	index->header.type     = C_BLOCK_TYPE_INDEX;

	index->entry.data.content = (*(&value));

	index->entry.pointer.blockId = 1;
	index->entry.pointer.inBlockOffset = 0;

	fseek(pDb, pos1, SEEK_SET);
	fwrite(buf, C_BLOCK_SIZE, 1, pDb);
	
	freeBuffer(buf);
		
	return TRUE;*/

	return FALSE;

}

// Rotina bruta. Precisa muita otimização. Varre todo o banco.
// Tbm deve permitir solicitações simultâneas

int lockFreeBlock(block_offset_p pos) {
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

int unlockBlock(block_offset_t pos) {
	if (pos >= 0)
		return FALSE;
	int ret = llRemove(lockedBlocks, pos);
	if (ret) {
		ret = llPush(freeBlocks, pos);
	}
	return ret;
}

generic_pointer_p blankBuffer() {
	generic_pointer_p buf = (generic_pointer_p) malloc(C_BLOCK_SIZE);
	
	//DEBUG("Obtendo buffer vazio!");
	memset(buf, 0x00, C_BLOCK_SIZE);
	
	return buf;
}
