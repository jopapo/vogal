
#include "vogal_manipulation.h"

vogal_manipulation::vogal_manipulation(vogal_handler *handler){
	DBUG_ENTER("vogal_manipulation::vogal_manipulation");
	
	m_Handler = handler;
	
	DBUG_LEAVE;
}

vogal_manipulation::~vogal_manipulation(){
	DBUG_ENTER("vogal_manipulation::~vogal_manipulation");
	DBUG_LEAVE;
}

ColumnCursorType * vogal_manipulation::findColumn(CursorType* cursor, char * colName) {
	DBUG_ENTER("vogal_manipulation::findColumn");
	if (!cursor) {
		perror("Impossível obter uma coluna de nenhum cursor!");
		DBUG_RETURN(NULL);
	}
	if (!cursor->table) {
		perror("Impossível obter uma coluna de nenhuma tabela!");
		DBUG_RETURN(NULL);
	}
	for (int i = 0; i < vlCount(cursor->table->colsList); i++) {
		ColumnCursorType * col = (ColumnCursorType *) vlGet(cursor->table->colsList, i);
		if (!strcmp(col->name, colName))
			DBUG_RETURN(col);
	}
	DBUG_RETURN(NULL);
}

ValueListRoot * vogal_manipulation::createObjectData(CursorType * cursor, char* name, char* type, BlockOffset location) {
	DBUG_ENTER("vogal_manipulation::createObjectData");
	
	DataCursorType *data = NULL;
	ValueListRoot *dataList = vlNew(true);

	data = new DataCursorType();
	data->column = findColumn(cursor, C_NAME_KEY);
	if (!data->column)
		goto freeCreateObjectData;
	data->usedSize = strlen(name);
	data->allocSize = data->usedSize + 1;
	data->content = (GenericPointer) name;
	if (!vlAdd(dataList, data))
		goto freeCreateObjectData;

	data = new DataCursorType();
	data->column = findColumn(cursor, C_TYPE_KEY);
	if (!data->column)
		goto freeCreateObjectData;
	data->content = (GenericPointer) type;
	data->usedSize = strlen(type);
	data->allocSize = data->usedSize + 1;
	if (!vlAdd(dataList, data))
		goto freeCreateObjectData;

	data = new DataCursorType();
	data->column = findColumn(cursor, C_LOCATION_KEY);
	if (!data->column)
		goto freeCreateObjectData;
	data->content = (GenericPointer) &location;
	data->usedSize = sizeof(location);
	data->allocSize = data->usedSize;
	if (!vlAdd(dataList, data))
		goto freeCreateObjectData;

	DBUG_RETURN(dataList);
	
freeCreateObjectData:
	if (data)
		data->~DataCursorType();
	vlFree(&dataList);

	DBUG_RETURN(NULL);
}

ValueListRoot * vogal_manipulation::createColumnData(CursorType * cursor, BigNumber tableRid, char* name, char* type, BlockOffset location) {
	DBUG_ENTER("vogal_manipulation::createColumnData");
	
	DataCursorType * data = NULL;
	ValueListRoot * dataList = vlNew(true);

	data = new DataCursorType();
	data->column = findColumn(cursor, C_TABLE_RID_KEY);
	if (!data->column)
		goto freeCreateColumnData;
	data->content = (GenericPointer) &tableRid;
	data->usedSize = sizeof(tableRid);
	data->allocSize = data->usedSize;
	if (!vlAdd(dataList, data))
		goto freeCreateColumnData;

	data = new DataCursorType();
	data->column = findColumn(cursor, C_NAME_KEY);
	if (!data->column)
		goto freeCreateColumnData;
	data->usedSize = strlen(name);
	data->allocSize = data->usedSize + 1;
	data->content = (GenericPointer) name;
	if (!vlAdd(dataList, data))
		goto freeCreateColumnData;

	data = new DataCursorType();
	data->column = findColumn(cursor, C_TYPE_KEY);
	if (!data->column)
		goto freeCreateColumnData;
	data->content = (GenericPointer) type;
	data->usedSize = strlen(type);
	data->allocSize = data->usedSize + 1;
	if (!vlAdd(dataList, data))
		goto freeCreateColumnData;

	data = new DataCursorType();
	data->column = findColumn(cursor, C_LOCATION_KEY);
	if (!data->column)
		goto freeCreateColumnData;
	data->content = (GenericPointer) &location;
	data->usedSize = sizeof(location);
	data->allocSize = data->usedSize;
	if (!vlAdd(dataList, data))
		goto freeCreateColumnData;

	DBUG_RETURN(dataList);
	
freeCreateColumnData:
	if (data)
		data->~DataCursorType();
	vlFree(&dataList);

	DBUG_RETURN(NULL);
}

BigNumber vogal_manipulation::nextRid(CursorType * cursor) {
	DBUG_ENTER("vogal_manipulation::nextRid");

	BigNumber id = 0;
	
	RidCursorType * maxRid = new RidCursorType();
	maxRid->id = -1; // Não aceita negativo, portanto se tornará o maior valor possível

	// Busca o RID mais próximo
	SearchInfoType * info = findNearest(cursor, maxRid, NULL, cursor->table->block);
	if (info && info->findedNode && info->findedNode->rid) {
		id = info->findedNode->rid->id;
		info->~SearchInfoType();
	}
	maxRid->~RidCursorType();

	DBUG_RETURN(id + 1);
}

BigNumber vogal_manipulation::insertData(CursorType* cursor, ValueListRoot* data) {
	DBUG_ENTER("vogal_manipulation::insertData");

	RidCursorType * newRid = NULL;
	
	if (!cursor) {
		perror("Impossível inserir dado com cursor fechado!");
		goto freeInsertData;
	}

	// Cria registro RID
	newRid = new RidCursorType();
	newRid->id = nextRid(cursor);
	newRid->dataList = data;
	
	if (!writeRid(cursor, newRid)) {
		perror("Erro ao gravar RID");
		goto freeInsertData;
	}

	DBUG_RETURN(newRid->id);
	
freeInsertData:
	if (newRid)
		newRid->~RidCursorType();

	DBUG_RETURN(0);
}

CursorType * vogal_manipulation::openCursor(ObjectCursorType * object, PairListRoot * filter){
	DBUG_ENTER("vogal_manipulation::openCursor");

	int ret = false;

	CursorType *cursor = new CursorType();
	cursor->table = object;
	cursor->filter = filter;
	cursor->hasMore = true;
	
	if (!cursor->table || !cursor->table->block)
		goto freeOpenCursor;
	
	ret = true;

freeOpenCursor:
	if (!ret) {
		cursor->~CursorType();
		cursor = NULL;
	}

	DBUG_RETURN(cursor);
}

NodeType * vogal_manipulation::parseRecord(CursorType * cursor, ColumnCursorType * column, BlockCursorType * block, GenericPointer * offset) {
	DBUG_ENTER("vogal_manipulation::parseRecord");

	// Declara
	NodeType * node = NULL;

	// Inicializa
	node = new NodeType();
	node->dataOwner = true;
	node->rid = new RidCursorType();
	node->rid->dataList = vlNew(true);
		
	if (block->header->type == C_BLOCK_TYPE_MAIN_TAB) {
		if (!m_Handler->getStorage()->readSizedNumber(offset, &node->rid->id)) {
			perror("Erro ao ler o rid do registro!");
			goto freeParseRecord;
		}

		// Erro!!!
		if (!node->rid->id) {
			perror("Rid com valor zerado!");
			goto freeParseRecord;
		}
		
		// Lê deslocamentos
		for (int i = 0; i < vlCount(cursor->table->colsList); i++) {
			DataCursorType * data = new DataCursorType();
			// o endereço do bloco (tamanho + localização)
			if (!m_Handler->getStorage()->readSizedNumber(offset, &data->blockId)) {
				perror("Erro ao ler dados da árvore de registros");
				data->~DataCursorType();
				goto freeParseRecord;
			}
			// o offset no bloco (tamanho é o offset)
			if (!m_Handler->getStorage()->readDataSize(offset, &data->blockOffset)) {
				perror("Erro ao ler dados de tamanho da árvore de registros");
				data->~DataCursorType();
				goto freeParseRecord;
			}
			data->column = (ColumnCursorType*) vlGet(cursor->table->colsList, i);
			// Adiciona informação à lista
			vlAdd(node->rid->dataList, data);
		}
	} else {
		if (!column) {
			perror("A coluna é obrigatória na leitura de blocos de colunas!");
			goto freeParseRecord;
		}

		node->data = new DataCursorType();
		node->data->column = column;
		
		// Obtém tamanho do dado
		if (!m_Handler->getStorage()->readDataSize(offset, &node->data->usedSize)) {
			perror("Erro ao ler o tamanho do dado!");
			goto freeParseRecord;
		}

		// Campo nulo.. ? Ainda não implementado
		if (!node->data->usedSize) {
			perror("Campos nulos ainda não foram implementados, como ele apareceu aí?");
			goto freeParseRecord;
		}

		// Carga e alocação dos dados
		switch (node->data->column->type) {
			case NUMBER:
				node->data->allocSize = sizeof(BigNumber); // Sempre é númeral máximo tratado, no caso LONG LONG INT (64BITS)
				if (node->data->usedSize > node->data->allocSize) {
					perror("Informação inconsistente na base pois não pode haver númeoros maiores que 64 bits (20 dígitos)"); // Ainda...
					goto freeParseRecord;
				}
				break;
			case VARCHAR:
				// Tamanho máximo permitido até o momento
				node->data->allocSize = node->data->usedSize + 1; // Mais 1 para o Null Terminated String
				break;
			default: // TODO (Pendência): Implementar comparação de todos os tipos de dados
				perror("Tipo de campo ainda não implementado!");
				goto freeParseRecord;
		}
		node->data->content = vogal_cache::blankBuffer(node->data->allocSize);
		// Lê o dado
		if (!m_Handler->getStorage()->readData(offset, node->data->content, node->data->usedSize, node->data->allocSize, node->data->column->type)) {
			perror("Erro ao ler dado do bloco!");
			goto freeParseRecord;
		}

		// Lê o número do RID
		if (!m_Handler->getStorage()->readSizedNumber(offset, &node->rid->id)) {
			perror("Erro ao ler o rid do dado!");
			goto freeParseRecord;
		}
	}

	DBUG_RETURN(node);

freeParseRecord:
	if (node)
		node->~NodeType();
		
	DBUG_RETURN(NULL);

}

SearchInfoType * vogal_manipulation::findNearest(CursorType * cursor, RidCursorType * rid2find, DataCursorType * data2find, BlockCursorType * rootBlock) {
	DBUG_ENTER("vogal_manipulation::findNearest");

	// Declara
	SearchInfoType * info = NULL;
	BlockOffset * neighbor;
	int count;

	// Inicializa variáveis
	info = new SearchInfoType();
	info->rootBlock = rootBlock;
	info->blocksList = vlNew(true);
	info->findedBlock = rootBlock;

	while (true) {
		if (!info->findedBlock) {
			perror("Por qual bloco começar?!");
			goto freeFindNearest;
		}
		if (!info->findedBlock->buffer) {
			perror("Bloco não bufferizado para efetuar o parse!");
			goto freeFindNearest;
		}
		if (info->findedBlock->header->type == C_BLOCK_TYPE_MAIN_TAB) {
			if (!rid2find) {
				perror("Nenhum rid para localização!");
				goto freeFindNearest;
			}
		} else {
			if (!data2find) {
				perror("Nenhum dado para localização!");
				goto freeFindNearest;
			}
		}
		if (!m_Handler->getDefinition()->parseBlock(cursor, data2find?data2find->column:NULL, info->findedBlock)) {
			perror("Erro ao obter os registros sem complementos do block!");
			goto freeFindNearest;
		}
		count = vlCount(info->findedBlock->nodesList);
		if (count) {
			info->offset = -1;
			for (int i = 0; i < count; i++) {
				info->findedNode = (NodeType *) vlGet(info->findedBlock->nodesList, i);

				if (info->findedBlock->header->type == C_BLOCK_TYPE_MAIN_TAB) {
					info->comparison = DIFF(rid2find->id, info->findedNode->rid->id);
				} else {
					// Comparação
					switch (data2find->column->type) {
						case NUMBER:
							info->comparison = DIFF((BigNumber*)data2find->content, (BigNumber*)info->findedNode->data->content);
							break;
						case VARCHAR:
							info->comparison = strncmp((char *) data2find->content, (char *) info->findedNode->data->content, MIN(data2find->usedSize, info->findedNode->data->usedSize));
							break;
						default:
							perror("Comparação não preparada para o tipo!");
							goto freeFindNearest;
					}
				}
				
				// Se for o dato for maior que o a ser procurado e terminou a busca
				if (info->comparison <= 0) {
					if (info->comparison) {
						neighbor = (BlockOffset *) vlGet(info->findedBlock->offsetsList, i);
						if ((*neighbor)) {
							info->findedBlock = m_Handler->getStorage()->openBlock((*neighbor));
							if (!info->blocksList)
								info->blocksList = vlNew(true);
							vlAdd(info->blocksList, info->findedBlock);
							continue;
						}
					}
					info->offset = i;
					break;
				}
			}
			if (info->offset < 0)
				info->offset = count;
			
			neighbor = (BlockOffset *) vlGet(info->findedBlock->offsetsList, count); // Direita!
			if (neighbor && (*neighbor)) {
				info->findedBlock = m_Handler->getStorage()->openBlock((*neighbor));
				if (!info->blocksList)
					info->blocksList = vlNew(true);
				vlAdd(info->blocksList, info->findedBlock);
				continue;
			} else {
				break;
			}
		} else {
			DBUG_PRINT("INFO", ("Bloco vazio!"));
			goto freeFindNearest;
		}
	}
	DBUG_RETURN(info);

freeFindNearest:
	if (info)
		info->~SearchInfoType();
		
	DBUG_RETURN(NULL);
}

int vogal_manipulation::updateBlockBuffer(BlockCursorType * block) {
	DBUG_ENTER("vogal_manipulation::updateBlockBuffer");

	int ret = false;
	int count = 0;
	GenericPointer p;
	BlockOffset * neighbor;
	NodeType * node;

	if (!block) {
		perror("Bloco inválido para atualização!");
		goto freeUpdateBlockBuffer;
	}
	
	if (block->nodesList)
		count = vlCount(block->nodesList);

	p = block->buffer + sizeof(BlockHeaderType);
	if (!m_Handler->getStorage()->writeDataSize(&p, count)) {
		perror("Erro ao escrever a quantidade de dados do bloco!");
		goto freeUpdateBlockBuffer;
	}

	if (count > 0) {
		for (int i = 0; i < count; i++) {
			neighbor = (BlockOffset *) vlGet(block->offsetsList, i);
			node = (NodeType *) vlGet(block->nodesList, i);
			if (!m_Handler->getStorage()->writeNumber((*neighbor), &p)) {
				perror("Erro ao escrever o ponteiro para o nodo da esqueda!");
				goto freeUpdateBlockBuffer;
			}
			if (block->header->type == C_BLOCK_TYPE_MAIN_TAB) {
				// Escreve o número do RID
				if (!m_Handler->getStorage()->writeNumber(node->rid->id, &p)) {
					perror("Erro ao escrever o rid no buffer da tabela!");
					goto freeUpdateBlockBuffer;
				}
				// Escreve os deslocamentos
				for (int c = 0; c < vlCount(node->rid->dataList); c++) {
					DataCursorType * data = (DataCursorType *) vlGet(node->rid->dataList, c);
					// Escreve o endereço do bloco
					if (!m_Handler->getStorage()->writeNumber(data->blockId, &p)) {
						perror("Erro ao escrever o endereço do bloco no buffer da tabela!");
						goto freeUpdateBlockBuffer;
					}
					// Escreve o deslocamento no bloco
					if (!m_Handler->getStorage()->writeDataSize(&p, data->blockOffset)) {
						perror("Erro ao escrever o deslocamento no buffer da tabela!");
						goto freeUpdateBlockBuffer;
					}
				}
			} else {
				// Escreve o dado chave da coluna
				if (!writeDataCursor(&p, node->data)) {
					perror("Erro ao escrever o dado chave no buffer da coluna!");
					goto freeUpdateBlockBuffer;
				}
				// Escreve o número do RID
				if (!m_Handler->getStorage()->writeNumber(node->rid->id, &p)) {
					perror("Erro ao escrever o rid no buffer da coluna!");
					goto freeUpdateBlockBuffer;
				}
			}
		}
		if (vlCount(block->offsetsList) != count + 1) {
			perror("A quantidade de ponteiros no nó deve ser a de registros + 1!");
			goto freeUpdateBlockBuffer;
		}
		neighbor = (BlockOffset *) vlGet(block->offsetsList, count);
		if (!m_Handler->getStorage()->writeNumber((*neighbor), &p)) {
			perror("Erro escrever o ponteiro para o nodo da direita!");
			goto freeUpdateBlockBuffer;
		}
	}

	block->freeSpace = C_BLOCK_SIZE - (p - block->buffer); // O deslocamento é o espaço usado no bloco!

	ret = true;
	
freeUpdateBlockBuffer:
	DBUG_RETURN(ret);
}

int vogal_manipulation::writeDataCursor(GenericPointer* dest, DataCursorType * data) {
	DBUG_ENTER("vogal_manipulation::writeDataCursor");
	if (!data) {
		perror("Impossível gravar 'nada'!");
		DBUG_RETURN(false);
	}
	DBUG_RETURN( m_Handler->getStorage()->writeAnyData(dest, (GenericPointer)data->content, data->column->type) );
}

int vogal_manipulation::updateLocation(CursorType * cursor, NodeType * node, BlockOffset blockId, BlockOffset blockOffset) {
	DBUG_ENTER("vogal_manipulation::updateLocation");

	// Declarações
	int ret = false;
	SearchInfoType * info;

	// Se não achou em memória, faz a busca na base
	info = findNearest(cursor, node->rid, NULL, cursor->table->block);
	if (info && !info->comparison) {
		// Procura a coluna com offset atualizado
		DataCursorType * data = (DataCursorType *) vlGet(info->findedNode->rid->dataList, node->data->column->getId());
		if (!data) {
			perror("A coluna a com deslocamento a ser atualizado não foi encontrada!");
			goto freeUpdateLocation;
		}
		
		data->blockId = blockId;
		data->blockOffset = blockOffset;

		if (!updateBlockBuffer(info->findedBlock)) {
			perror("Erro ao atualizar buffer da coluna com deslocamento");
			goto freeUpdateLocation;
		}

		if (!m_Handler->getStorage()->writeBlock(info->findedBlock)) {
			perror("Erro ao gravar bloco de dados com o deslocamento atualizado!");
			goto freeUpdateLocation;
		}

		ret = true;
	} else {
		perror("Impossível atualizar o deslocamento no rid!");
		goto freeUpdateLocation;
	}

freeUpdateLocation:
	if (info)
		info->~SearchInfoType();

	DBUG_RETURN(ret);
}

// TODO: Tornar o índice balanceado
int vogal_manipulation::writeData(CursorType * cursor, RidCursorType * rid, DataCursorType * data) {
	DBUG_ENTER("vogal_manipulation::writeData");

	// Declaração das variáveis
	int ret = false;
	BigNumber neededSpace;
	BigNumber freeSpace;
	BlockCursorType *block;
	SearchInfoType *info = NULL;
	NodeType *node;
	int offset;
	BlockOffset * neighbor;

	// Inicialização das variáveis
	// TODO: Arrumar cálculo do espaço necessário pois valores numéricos estão sendo calculados com seu tamanho máximo e não com seu tamanho real
	// 		 causando desperdício no bloco.
	if (data) {
		block = data->column->block;
		neededSpace = 
			m_Handler->getStorage()->bytesNeeded(sizeof(BlockOffset)) + // Ponteiro bloco esquerda
			m_Handler->getStorage()->bytesNeeded(data->usedSize) + // Dado chave
			m_Handler->getStorage()->bytesNeeded(sizeof(rid->id)); // RID
			
	} else {
		block = cursor->table->block;
		neededSpace =
			m_Handler->getStorage()->bytesNeeded(sizeof(BlockOffset)) + // Ponteiro bloco esquerda
			m_Handler->getStorage()->bytesNeeded(sizeof(rid->id)) + // RID
			vlCount(rid->dataList) * m_Handler->getStorage()->bytesNeeded(sizeof(BlockOffset)) * 2; // Deslocamentos!!!
	}

	// Procura o dado mais próximo
	info = findNearest(cursor, rid, data, block);

	// Se achou, verifica posição de inserção
	if (info) {
		offset = info->offset;
		block = info->findedBlock;

		// Reorganizar deslocamentos dos rids a direita (se estivar gravando um dado)
		// TODO: Otimizar para fazer todas as operações em memória e gravar em disco apenas uma vez
		if (data && offset < vlCount(block->nodesList)) {
			for (int i = offset; i < vlCount(block->nodesList); i++) {
				NodeType * otherNode = (NodeType *) vlGet(block->nodesList, offset);
				if (!updateLocation(cursor, otherNode, block->id, offset + 1)) {
					perror("Erro ao atualizar o deslocamento das colunas do bloco!");
					goto freeWriteData;
				}
			}
		}

	} else {
		offset = 0;
		neededSpace += m_Handler->getStorage()->bytesNeeded(sizeof(BlockOffset)); // Ponteiro bloco direita
	}

	// Valida se o espaço livre do bloco é consistente
	if (block->freeSpace < 0 || block->freeSpace > C_BLOCK_SIZE - sizeof(BlockHeaderType)) {
		perror("Erro ao calular espaço disponível no bloco. Fora da faixa permitida");
		goto freeWriteData;
	}

	// Cria novo nodo e insere no local adequado
	node = new NodeType();
	node->rid = rid;
	node->data = data;
	if (data) {
		data->blockId = block->id;
		data->blockOffset = offset;
	}
	
	vlInsert(block->nodesList, node, offset);

	// Esquerda e direita apontando para o vazio!
	neighbor = new BlockOffset();
	(*neighbor) = 0;
	vlInsert(block->offsetsList, neighbor, offset);
	// Somente insere vizinho da direita se for o primeiro rid do bloco
	if (vlCount(block->offsetsList) == 1) {
		neighbor = new BlockOffset();
		(*neighbor) = 0;
		vlInsert(block->offsetsList, neighbor, offset + 1);
	}

	if (neededSpace > block->freeSpace) {
		// ################################################

		// Fudeu!!! Tratar SPLIT !!!
		// ################################################
		perror("AINDA NÃO FEITO!!!!!!");
		goto freeWriteData;
		
	} else {
		if (!updateBlockBuffer(block)) {
			perror("Erro ao atualizar o buffer do bloco!");
			goto freeWriteData;
		}

		if (!m_Handler->getStorage()->writeBlock(block)) {
			perror("Erro ao gravar bloco de dados!");
			goto freeWriteData;
		}
	}

	ret = true;

freeWriteData:
	if (info)
		info->~SearchInfoType();
		
	DBUG_RETURN(ret);	
}

int vogal_manipulation::writeRid(CursorType * cursor, RidCursorType * rid){
	DBUG_ENTER("vogal_manipulation::writeRid");

	int ret = false;
	BigNumber fieldCount = 0;

	// Os RIDs devem iniciar de 1
	if (rid->id <= 0) {
		perror("Código RID inválido!");
		goto freeWriteRid;
	}

	// Como ainda não são permitidos campos nulos, os dados e as colunas devem estar na mesma ordem e ter a mesma quantidade de registros
	if (vlCount(rid->dataList) != vlCount(cursor->table->colsList)) {
		perror("A quantidade de colunas e dados não confere!");
		goto freeWriteRid;
	}

	// Grava os dados primeiro para obter sua localização
	for (int i = 0; i < vlCount(rid->dataList); i++)
		if (!writeData(cursor, rid, (DataCursorType *) vlGet(rid->dataList, i))) {
			perror("Erro ao gravar dado do rid!");
			goto freeWriteRid;
		}

	// Daí grava o RID com sua localização
	if (!writeData(cursor, rid, NULL)) {
		perror("Erro ao gravar o rid!");
		goto freeWriteRid;
	}

	ret = true;

freeWriteRid:

	DBUG_RETURN(ret);
}

int vogal_manipulation::fetch(CursorType * cursor){
	// Aqui está acessando direto o disco - Ajustar para obter da memória 
	DBUG_ENTER("vogal_manipulation::fetch");

	int ret = false;
	/*int validFetch = false; 
	
	if (!cursor->hasMore) {
		perror("Não há mais dados");
		goto freeFetch; 
	}

	cursor->fetch = readRid(cursor);
	while (cursor->fetch) {
		// Valido?
		validFetch = true; // True pois se não houver filtro, o primeiro registro é válido
		
		// Processo de comparação por todos os campos do filtro
		StringTreeNode* iNode = NULL;
		StringTreeIterator* iter = NULL;
		if (cursor->filter)
			iter = stCreateIterator(cursor->filter, &iNode);
		while (iNode) {
			// Obtenção de informações
			TreeNodeValue value;
			ColumnCursorType * column;
			DataCursorType * data;
			int comparison;
			
			if (!stGet(cursor->table->colsList, stNodeName(iNode), &value))
				goto freeFetch;    
			column = (ColumnCursorType *) value;
			if (!stGet(cursor->fetch->dataList, (char*) stNodeName(iNode), &value))
				goto freeFetch;    
			data = (DataCursorType *) value;

			// Comparação
			switch (column->type) {
				case NUMBER:
					comparison = (BigNumber*)data->content - (BigNumber*) stNodeValue(iNode);
					break;
				case VARCHAR:
					// Comparação
					comparison = memcmp(data->content, stNodeValue(iNode), MIN(data->allocSize, strlen((char*)stNodeValue(iNode))));
					break;
				default:
					perror("Comparação não preparada para o tipo!");
					goto freeFetch;
			}
			if (comparison) {
				validFetch = false;
				break;
			}
	
			iNode = stNext(iter);
		}
		stFreeIterator(iter);
		
		if (validFetch) {
			cursor->rowCount ++;
			break;
		}
		
		// Limpa RID anterior
		freeRid(cursor->fetch);
		// Executa novo fetch
		cursor->fetch = readRid(cursor);
		
	}
	
	if (validFetch) {
		ret = true;
	} else {
		cursor->hasMore = false;
	}
	
freeFetch:*/

	DBUG_RETURN(ret);
}

RidCursorType * vogal_manipulation::readRid(CursorType * cursor){
	DBUG_ENTER("vogal_manipulation::readRid");

	/*int ret = false;
	BigNumber fieldCount;

	GenericPointer p = cursor->offset;
	
	RidCursorType * rid = new RidCursorType();
	rid->dataList = NULL;
	
	// Cria objeto RID
	if (!m_Handler->getStorage()->readSizedNumber(&p, &rid->rid))
		goto freeReadRid;
		
	// Os RIDs devem iniciar de 1, caso seja zero é que os registros acabaram
	if (!rid->rid) {
		goto freeReadRid;
	}
	
	rid->dataList = stNew(FALSE, TRUE); 
	
	if (!m_Handler->getStorage()->readDataSize(&p, &fieldCount))
		goto freeReadRid;
	
	for (; fieldCount > 0; fieldCount--) {
		// Obtém o ID da coluna
		BigNumber colId;
		// Procura a coluna com determinado ID
		ColumnCursorType * column = NULL;
		StringTreeNode * iNode;
		StringTreeIterator * iter;
		BigNumber colDataSize;			
		DataCursorType * data;


		if (!m_Handler->getStorage()->readDataSize(&p, &colId))
			goto freeReadRid;

		
		iNode = NULL;
		iter = stCreateIterator(cursor->table->colsList, &iNode);
		while (iNode) {
			column = (ColumnCursorType*) stNodeValue(iNode);
			if (column->id == colId) 
				break;
			iNode = stNext(iter);
		}
		stFreeIterator(iter);
		
		if (!iNode)
			goto freeReadRid;

		// Obtém tamanho do dado
		if (!m_Handler->getStorage()->readDataSize(&p, &colDataSize)) {
			perror("Erro ao ler o tamanho do dado!");
			goto freeReadRid;
		}
		if (!colDataSize) {
			// Aqui deveria gerar erro por registros nulos nem serem gravados, porém em caso de atualização isso pode acontecer (supostamente)
			continue;
		}

		// Achou a coluna portanto, carrega conforme seu tipo
		data = (DataCursorType*) malloc(sizeof(DataCursorType));
		data->content = NULL;
		
		// Adiciona a lista de dados dos fetch 
		stPut(rid->dataList, column->name, data);
		
		// Carga e alocação dos dados
		switch (column->type) {
			case NUMBER:
				data->allocSize = sizeof(BigNumber); // Sempre é númeral máximo tratado, no caso LONG LONG INT (64BITS)  
				data->content = (GenericPointer) malloc(data->allocSize);
				memset(data->content, 0x00, data->allocSize);
				break;
			case VARCHAR:
				// Tamanho máximo permitido até o momento
				data->allocSize = colDataSize;
				data->content = (GenericPointer) malloc(data->allocSize + 1); // Mais 1 para o Null Terminated String
				memset(data->content, 0x00, data->allocSize + 1);
				break;
			default: // TODO (Pendência): Implementar comparação de todos os tipos de dados
				perror("Tipo de campo ainda não implementado!");
				goto freeReadRid;
		}
		if (!m_Handler->getStorage()->readData(&p, data->content, colDataSize, data->allocSize, column->type)) {
			perror("Erro ao ler dado do bloco!");
			goto freeReadRid;
		}
	}
	
	ret = true;
	
freeReadRid:
	if (!ret) {
		freeRid(rid);
		rid = NULL;
	}
	if (ret)
		cursor->offset = p;

	DBUG_RETURN(rid);*/
	DBUG_RETURN(NULL);
}
