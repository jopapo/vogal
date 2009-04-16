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
		block_header_p header = (block_header_p) buf;
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
	block_header_p index = (block_header_p) buf;
	
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

void freeBuffer(generic_pointer_p buf) {
	if (buf) {
		free(buf);
	}
}

int createDataDictionary() {
	
	int i;
	int res = FALSE;
	
	#define C_TABS_BUFS 2
	#define C_COLS_BUFS 7
	
	#define C_TABS_I 0
	#define C_COLS_I 1

	generic_pointer_p
		tabs_p[C_TABS_BUFS] = {NULL, NULL},
		cols_p[C_COLS_BUFS] = {NULL, NULL, NULL, NULL, NULL, NULL, NULL};
	
	// Inicializa buffers das tabelas
	for (i = 0; i < C_TABS_BUFS; i++) { 
		tabs_p[i] = blankBuffer();
		if (!createStructure(C_BLOCK_TYPE_MAIN_TAB, tabs_p[i]))
			goto free;
	}
	
	// Inicializa buffers das colunas
	for (i = 0; i < C_COLS_BUFS; i++) { 
		cols_p[i] = blankBuffer();
		if (!createStructure(C_BLOCK_TYPE_MAIN_TAB, cols_p[i]))
			goto free;
	}
	
	// Preenchimento dos dados
	generic_pointer_p p = tabs_p[C_TABS_I];
	p+=sizeof(block_header_t);

	// Ponteiro do último bloco (Se é ele mesmo, não há pq preencher)
	pointer_p pon = (pointer_p)p;
	pon->valid = FALSE;
	pon->more = FALSE;
	pon->id = 0;
	pon->offset = 0;
	p+=sizeof(pointer_t);
	
	// RID do primeiro registro (Sempre começará do zero e nunca repetirá dentro da mesma tabela)
	rid_p rid = (rid_p)p;
	rid->valid = TRUE;
	rid->more = FALSE;
	rid->id = 0;
	p+=sizeof(rid_t);
	
	// Ponteiro da única coluna associada ao RID => name
	block_offset_t tab_name = -1;
	if (!lockFreeBlock(&tab_name))
		goto free;
	
	pon = (pointer_p)p;
	pon->valid = TRUE;
	pon->more = FALSE;
	pon->id = 0;
	pon->offset = 0;
	p+=sizeof(pointer_t);
	
free:
	for (i = 0; i < C_TABS_BUFS; i++)
		freeBuffer(tabs_p[i]);
	for (i = 0; i < C_COLS_BUFS; i++)
		freeBuffer(cols_p[i]);

	// Se deu erro destrava os blocos
	if (!res) {
		unlockBlock(tab_name);
	}

	return res;
	
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
	
	DEBUG("Obtendo buffer vazio!");
	memset(buf, 0x00, C_BLOCK_SIZE);
	
	return buf;
}
