
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

ValueListRoot * vogal_manipulation::createObjectData(CursorType * cursor, char* name, char* type, BlockOffset * location) {
	DBUG_ENTER("vogal_manipulation::createObjectData");
	
	DataCursorType *data = NULL;
	ValueListRoot *dataList = vlNew(true);

	data = new DataCursorType();
	data->column = m_Handler->getDefinition()->findColumn(cursor->table, C_NAME_KEY);
	if (!data->column)
		goto freeCreateObjectData;
	data->content = (GenericPointer) name;
	data->usedSize = strlen(name);
	data->allocSize = data->usedSize + 1;
	data->contentOwner = false;
	if (!vlAdd(dataList, data))
		goto freeCreateObjectData;

	data = new DataCursorType();
	data->column = m_Handler->getDefinition()->findColumn(cursor->table, C_TYPE_KEY);
	if (!data->column)
		goto freeCreateObjectData;
	data->content = (GenericPointer) type;
	data->usedSize = strlen(type);
	data->allocSize = data->usedSize + 1;
	data->contentOwner = false;
	if (!vlAdd(dataList, data))
		goto freeCreateObjectData;

	data = new DataCursorType();
	data->column = m_Handler->getDefinition()->findColumn(cursor->table, C_LOCATION_KEY);
	if (!data->column)
		goto freeCreateObjectData;
	data->content = (GenericPointer) location;
	data->usedSize = sizeof(location);
	data->allocSize = data->usedSize;
	data->contentOwner = false;
	if (!vlAdd(dataList, data))
		goto freeCreateObjectData;

	DBUG_RETURN(dataList);
	
freeCreateObjectData:
	if (data)
		data->~DataCursorType();
	vlFree(&dataList);

	DBUG_RETURN(NULL);
}

ValueListRoot * vogal_manipulation::createColumnData(CursorType * cursor, BigNumber * tableRid, char* name, char* type, BlockOffset * location) {
	DBUG_ENTER("vogal_manipulation::createColumnData");
	
	DataCursorType * data = NULL;
	ValueListRoot * dataList = vlNew(true);

	data = new DataCursorType();
	data->column = m_Handler->getDefinition()->findColumn(cursor->table, C_TABLE_RID_KEY);
	if (!data->column)
		goto freeCreateColumnData;
	data->content = (GenericPointer) tableRid;
	data->usedSize = sizeof(tableRid);
	data->allocSize = data->usedSize;
	data->contentOwner = false;
	if (!vlAdd(dataList, data))
		goto freeCreateColumnData;

	data = new DataCursorType();
	data->column = m_Handler->getDefinition()->findColumn(cursor->table, C_NAME_KEY);
	if (!data->column)
		goto freeCreateColumnData;
	data->content = (GenericPointer) name;
	data->usedSize = strlen(name);
	data->allocSize = data->usedSize + 1;
	data->contentOwner = false;
	if (!vlAdd(dataList, data))
		goto freeCreateColumnData;

	data = new DataCursorType();
	data->column = m_Handler->getDefinition()->findColumn(cursor->table, C_TYPE_KEY);
	if (!data->column)
		goto freeCreateColumnData;
	data->content = (GenericPointer) type;
	data->usedSize = strlen(type);
	data->allocSize = data->usedSize + 1;
	data->contentOwner = false;
	if (!vlAdd(dataList, data))
		goto freeCreateColumnData;

	data = new DataCursorType();
	data->column = m_Handler->getDefinition()->findColumn(cursor->table, C_LOCATION_KEY);
	if (!data->column)
		goto freeCreateColumnData;
	data->content = (GenericPointer) location;
	data->usedSize = sizeof(location);
	data->allocSize = data->usedSize;
	data->contentOwner = false;
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
		ERROR("Impossível inserir dado com cursor fechado!");
		goto freeInsertData;
	}

	// Cria registro RID
	newRid = new RidCursorType();
	newRid->id = nextRid(cursor);
	newRid->dataList = data;
	
	if (!writeRid(cursor, newRid)) {
		ERROR("Erro ao gravar RID");
		goto freeInsertData;
	}

	DBUG_RETURN(newRid->id);
	
freeInsertData:
	if (newRid)
		newRid->~RidCursorType();

	DBUG_RETURN(0);
}

int vogal_manipulation::removeFetch(FilterCursorType * filter) {
	DBUG_ENTER("vogal_manipulation::removeFetch");

	DataCursorType * data;
	BlockCursorType * block = NULL;
	NodeType * node;

	// Atualiza dados
	for (int i = 0; i < vlCount(filter->fetch->dataList); i++) {
		data = (DataCursorType *) vlGet(filter->fetch->dataList, i);

		block = m_Handler->getStorage()->openBlock(data->blockId);
		if (!block) {
			ERROR("Erro ao abrir bloco para remover dado da coluna!");
			goto freeRemoveFetch;
		}

		if (!m_Handler->getDefinition()->parseBlock(filter->cursor, NULL, block)) {
			ERROR("Erro ao efetuar o parse ao carregar dado da coluna!");
			goto freeRemoveFetch;
		}

		node = (NodeType *) vlGet(block->nodesList, data->blockOffset);
		if (!node) {
			ERROR("Erro ao localizar nodo específico do dado da coluna!");
			goto freeRemoveFetch;
		}
		node->deleted = true;

		// Atualiza e grava
		if (!updateBlockBuffer(filter->cursor, block)) {
			ERROR("Erro ao atualizar o buffer do bloco!");
			goto freeRemoveFetch;
		}

		if (!m_Handler->getStorage()->writeBlock(block)) {
			ERROR("Erro ao gravar bloco de dados!");
			goto freeRemoveFetch;
		}
		
	}

	if (!filter->infoFetch || !filter->infoFetch->findedNode) {
		ERROR("Erro ao localizar nodo específico do RID!");
		goto freeRemoveFetch;
	}
	filter->infoFetch->findedNode->deleted = true;

	// Atualiza e grava
	if (!updateBlockBuffer(filter->cursor, filter->infoFetch->findedBlock)) {
		ERROR("Erro ao atualizar o buffer do bloco dos rids!");
		goto freeRemoveFetch;
	}

	if (!m_Handler->getStorage()->writeBlock(filter->infoFetch->findedBlock)) {
		ERROR("Erro ao gravar bloco dos rids!");
		goto freeRemoveFetch;
	}

freeRemoveFetch:
	if (block)
		block->~BlockCursorType();
	
	DBUG_RETURN(true);
}

CursorType * vogal_manipulation::openCursor(ObjectCursorType * object){
	DBUG_ENTER("vogal_manipulation::openCursor");

	int ret = false;

	CursorType *cursor = new CursorType();
	cursor->table = object;

	if (!cursor->table || !cursor->table->block) {
		ERROR("Informações incompletas para definição do cursor!");
		goto freeOpenCursor;
	}
	
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
			ERROR("Erro ao ler o rid do registro!");
			goto freeParseRecord;
		}

		// Erro!!!
		if (!node->rid->id) {
			ERROR("Rid com valor zerado!");
			goto freeParseRecord;
		}
		
		// Lê deslocamentos
		for (int i = 0; i < vlCount(cursor->table->colsList); i++) {
			DataCursorType * data = new DataCursorType();
			// o endereço do bloco (tamanho + localização)
			if (!m_Handler->getStorage()->readSizedNumber(offset, &data->blockId)) {
				ERROR("Erro ao ler dados da árvore de registros");
				data->~DataCursorType();
				goto freeParseRecord;
			}
			// o offset no bloco (tamanho é o offset)
			if (!m_Handler->getStorage()->readDataSize(offset, &data->blockOffset)) {
				ERROR("Erro ao ler dados de tamanho da árvore de registros");
				data->~DataCursorType();
				goto freeParseRecord;
			}
			data->column = (ColumnCursorType*) vlGet(cursor->table->colsList, i);
			// Adiciona informação à lista
			vlAdd(node->rid->dataList, data);
		}
	} else {
		node->data = new DataCursorType();
		node->data->column = column;
		
		// Obtém tamanho do dado
		if (!m_Handler->getStorage()->readDataSize(offset, &node->data->usedSize)) {
			ERROR("Erro ao ler o tamanho do dado!");
			goto freeParseRecord;
		}

		// Campo nulo.. ? Ainda não implementado
		if (!node->data->usedSize) {
			ERROR("Campos nulos ainda não foram implementados, como ele apareceu aí?");
			goto freeParseRecord;
		}

		// Carga e alocação dos dados
		if (node->data->column) {
			switch (node->data->column->type) {
				case NUMBER:
					node->data->allocSize = sizeof(BigNumber); // Sempre é númeral máximo tratado, no caso LONG LONG INT (64BITS)
					if (node->data->usedSize > node->data->allocSize) {
						ERROR("Informação inconsistente na base pois não pode haver númeoros maiores que 64 bits (20 dígitos)"); // Ainda...
						goto freeParseRecord;
					}
					break;
				case VARCHAR:
					// Tamanho máximo permitido até o momento
					node->data->allocSize = node->data->usedSize + 1; // Mais 1 para o Null Terminated String
					break;
				default: // TODO (Pendência): Implementar comparação de todos os tipos de dados
					ERROR("Tipo de campo ainda não implementado!");
					goto freeParseRecord;
			}
			node->data->content = vogal_cache::blankBuffer(node->data->allocSize);
			// Lê o dado
			if (!m_Handler->getStorage()->readData(offset, node->data->content, node->data->usedSize, node->data->allocSize, node->data->column->type)) {
				ERROR("Erro ao ler dado do bloco!");
				goto freeParseRecord;
			}
		} else {
			// Pula o dado
			offset += node->data->usedSize;
		}
		// Lê o número do RID
		if (!m_Handler->getStorage()->readSizedNumber(offset, &node->rid->id)) {
			ERROR("Erro ao ler o rid do dado!");
			goto freeParseRecord;
		}
	}

	DBUG_RETURN(node);

freeParseRecord:
	if (node)
		node->~NodeType();
		
	DBUG_RETURN(NULL);

}

int vogal_manipulation::comparison(SearchInfoType * info, CursorType * cursor, RidCursorType * rid2find, DataCursorType * data2find, int startingOffset) {
	DBUG_ENTER("vogal_manipulation::comparison");

	int ret = false;
	int count;
	BlockOffset * neighbor;
	
	if (!info) {
		ERROR("As informações de busca já devem ter sido inicializadas!");
		goto freeComparison;
	}

	// Inicializa (identifica quando não houver encontrado nada)
	info->findedNode = NULL;
	
continueWhileComparison:
	if (!info->findedBlock) {
		ERROR("Por qual bloco começar?!");
		goto freeComparison;
	}
	if (!info->findedBlock->buffer) {
		ERROR("Bloco não bufferizado para efetuar o parse!");
		goto freeComparison;
	}
	if (info->findedBlock->header->type == C_BLOCK_TYPE_MAIN_TAB) {
		if (!rid2find) {
			ERROR("Nenhum rid para localização!");
			goto freeComparison;
		}
	} else {
		if (!data2find) {
			ERROR("Nenhum dado para localização!");
			goto freeComparison;
		}
	}
	if (!startingOffset) 
		if (!m_Handler->getDefinition()->parseBlock(cursor, data2find?data2find->column:NULL, info->findedBlock)) {
			ERROR("Erro ao obter os registros sem complementos do block!");
			goto freeComparison;
		}

	count = vlCount(info->findedBlock->nodesList);
	if (count) {
		for (info->offset = startingOffset; info->offset < count; info->offset++) {
			info->findedNode = (NodeType *) vlGet(info->findedBlock->nodesList, info->offset);
			
			if (info->findedBlock->header->type == C_BLOCK_TYPE_MAIN_TAB) {
				info->comparison = DIFF(rid2find->id, info->findedNode->rid->id);
			} else {
				// Comparação
				switch (data2find->column->type) {
					case NUMBER:
						info->comparison = DIFF((*(BigNumber*)data2find->content), (*(BigNumber*)info->findedNode->data->content));
						break;
					case VARCHAR:
						info->comparison = strncmp((char *) data2find->content, (char *) info->findedNode->data->content, MIN(data2find->usedSize, info->findedNode->data->usedSize));
						break;
					default:
						ERROR("Comparação não preparada para o tipo!");
						goto freeComparison;
				}
			}
			
			// Se for o dato for maior que o a ser procurado e terminou a busca
			if (info->comparison <= 0) {
				if (info->comparison) {
					neighbor = (BlockOffset *) vlGet(info->findedBlock->offsetsList, info->offset);
					if (neighbor && (*neighbor)) {
						info->findedBlock = m_Handler->getStorage()->openBlock((*neighbor));
						if (!info->blocksList)
							info->blocksList = vlNew(false);
						vlAdd(info->blocksList, info->findedBlock);
						startingOffset = 0;
						goto continueWhileComparison;
					}
				}
				break;
			}
		}
		neighbor = (BlockOffset *) vlGet(info->findedBlock->offsetsList, count); // Direita!
		if (neighbor && (*neighbor)) {
			info->findedBlock = m_Handler->getStorage()->openBlock((*neighbor));
			if (!info->blocksList)
				info->blocksList = vlNew(false);
			vlAdd(info->blocksList, info->findedBlock);
			startingOffset = 0;
			goto continueWhileComparison;
		}
	} else {
		DBUG_PRINT("INFO", ("Bloco vazio!"));
	}

	ret = true;
	
freeComparison:
	DBUG_RETURN(ret);
}

SearchInfoType * vogal_manipulation::findNearest(CursorType * cursor, RidCursorType * rid2find, DataCursorType * data2find, BlockCursorType * rootBlock) {
	DBUG_ENTER("vogal_manipulation::findNearest");

	// Declara
	SearchInfoType * info = new SearchInfoType();
	info->rootBlock = rootBlock;
	info->blocksList = vlNew(true);
	info->findedBlock = rootBlock;

	if (!comparison(info, cursor, rid2find, data2find)) {
		ERROR("Erro durante o processo de comparação!");
		info->~SearchInfoType();
		DBUG_RETURN(NULL);
	}

	DBUG_RETURN(info);
}

int vogal_manipulation::updateBlockBuffer(CursorType * cursor, BlockCursorType * block) {
	DBUG_ENTER("vogal_manipulation::updateBlockBuffer");

	int ret = false;
	int count = 0;
	GenericPointer p;
	BlockOffset * neighbor;
	NodeType * node;

	if (!block) {
		ERROR("Bloco inválido para atualização!");
		goto freeUpdateBlockBuffer;
	}
	
	if (block->nodesList)
		count = vlCount(block->nodesList);

	p = block->buffer + sizeof(BlockHeaderType);
	if (!m_Handler->getStorage()->writeDataSize(&p, count)) {
		ERROR("Erro ao escrever a quantidade de dados do bloco!");
		goto freeUpdateBlockBuffer;
	}

	if (count > 0) {
		int writed = 0;
		int deleted = 0;
		for (int i = 0; i < count; i++) {
			node = (NodeType *) vlGet(block->nodesList, i);
			if (node->deleted) {
				deleted++;
				continue;
			}
			neighbor = (BlockOffset *) vlGet(block->offsetsList, i);
			if (!m_Handler->getStorage()->writeNumber((*neighbor), &p)) {
				ERROR("Erro ao escrever o ponteiro para o nodo da esqueda!");
				goto freeUpdateBlockBuffer;
			}
			if (block->header->type == C_BLOCK_TYPE_MAIN_TAB) {
				// Escreve o número do RID
				if (!m_Handler->getStorage()->writeNumber(node->rid->id, &p)) {
					ERROR("Erro ao escrever o rid no buffer da tabela!");
					goto freeUpdateBlockBuffer;
				}
				// Escreve os deslocamentos
				for (int c = 0; c < vlCount(node->rid->dataList); c++) {
					DataCursorType * data = (DataCursorType *) vlGet(node->rid->dataList, c);
					// Escreve o endereço do bloco
					if (!m_Handler->getStorage()->writeNumber(data->blockId, &p)) {
						ERROR("Erro ao escrever o endereço do bloco no buffer da tabela!");
						goto freeUpdateBlockBuffer;
					}
					// Escreve o deslocamento no bloco
					if (!m_Handler->getStorage()->writeDataSize(&p, data->blockOffset)) {
						ERROR("Erro ao escrever o deslocamento no buffer da tabela!");
						goto freeUpdateBlockBuffer;
					}
				}
			} else {
				// Escreve o dado chave da coluna
				if (!writeDataCursor(&p, node->data)) {
					ERROR("Erro ao escrever o dado chave no buffer da coluna!");
					goto freeUpdateBlockBuffer;
				}
				// Escreve o número do RID
				if (!m_Handler->getStorage()->writeNumber(node->rid->id, &p)) {
					ERROR("Erro ao escrever o rid no buffer da coluna!");
					goto freeUpdateBlockBuffer;
				}
				// Se excluiu algum antes, atualiza pais
				if (deleted) {
					// TODO: Otimizar para fazer todas as operações em memória e gravar em disco apenas uma vez
					if (!updateLocation(cursor, node, block->id, i - deleted)) {
						ERROR("Erro ao atualizar o deslocamento das colunas do bloco!");
						goto freeUpdateBlockBuffer;
					}
				}
			}
			writed++;
		}
		if (writed) {
			if (vlCount(block->offsetsList) != count + 1) {
				ERROR("A quantidade de ponteiros no nó deve ser a de registros + 1!");
				goto freeUpdateBlockBuffer;
			}
			neighbor = (BlockOffset *) vlGet(block->offsetsList, count);
			if (!m_Handler->getStorage()->writeNumber((*neighbor), &p)) {
				ERROR("Erro escrever o ponteiro para o nodo da direita!");
				goto freeUpdateBlockBuffer;
			}
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
		ERROR("Impossível gravar 'nada'!");
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
			ERROR("A coluna a com deslocamento a ser atualizado não foi encontrada!");
			goto freeUpdateLocation;
		}
		
		data->blockId = blockId;
		data->blockOffset = blockOffset;

		// TODO: CUIDADO!!! Nunca encher muito o bloco pois na atualização do deslocamento normalmente se ocupam mais ou menos blocos na
		//		hora de escrevê-lo. Portanto, nunca encher o bloco por completo
		if (!updateBlockBuffer(cursor, info->findedBlock)) {
			ERROR("Erro ao atualizar buffer da coluna com deslocamento");
			goto freeUpdateLocation;
		}

		if (!m_Handler->getStorage()->writeBlock(info->findedBlock)) {
			ERROR("Erro ao gravar bloco de dados com o deslocamento atualizado!");
			goto freeUpdateLocation;
		}

		ret = true;
	} else {
		ERROR("Impossível atualizar o deslocamento no rid!");
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
					ERROR("Erro ao atualizar o deslocamento das colunas do bloco!");
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
		ERROR("Erro ao calular espaço disponível no bloco. Fora da faixa permitida");
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
		ERROR("AINDA NÃO FEITO!!!!!!");
		goto freeWriteData;
		
	} else {
		if (!updateBlockBuffer(cursor, block)) {
			ERROR("Erro ao atualizar o buffer do bloco!");
			goto freeWriteData;
		}

		if (!m_Handler->getStorage()->writeBlock(block)) {
			ERROR("Erro ao gravar bloco de dados!");
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
		ERROR("Código RID inválido!");
		goto freeWriteRid;
	}

	// Como ainda não são permitidos campos nulos, os dados e as colunas devem estar na mesma ordem e ter a mesma quantidade de registros
	if (vlCount(rid->dataList) != vlCount(cursor->table->colsList)) {
		ERROR("A quantidade de colunas e dados não confere!");
		goto freeWriteRid;
	}

	// Grava os dados primeiro para obter sua localização
	for (int i = 0; i < vlCount(rid->dataList); i++)
		if (!writeData(cursor, rid, (DataCursorType *) vlGet(rid->dataList, i))) {
			ERROR("Erro ao gravar dado do rid!");
			goto freeWriteRid;
		}

	// Daí grava o RID com sua localização
	if (!writeData(cursor, rid, NULL)) {
		ERROR("Erro ao gravar o rid!");
		goto freeWriteRid;
	}

	ret = true;

freeWriteRid:

	DBUG_RETURN(ret);
}


int vogal_manipulation::fillDataFromLocation(CursorType * cursor, DataCursorType * data) {
	DBUG_ENTER("vogal_manipulation::fillDataFromLocation");

	int ret = false;
	BlockCursorType * block = NULL;
	NodeType * node;

	// Abre o bloco do dado
	block = m_Handler->getStorage()->openBlock(data->blockId);
	if (!block) {
		ERROR("Erro ao abrir bloco para carregar dado da coluna!");
		goto freeFillDataFromLocation;
	}

	if (!m_Handler->getDefinition()->parseBlock(cursor, data->column, block)) {
		ERROR("Erro ao efetuar o parse ao carregar dado da coluna!");
		goto freeFillDataFromLocation;
	}

	node = (NodeType *) vlGet(block->nodesList, data->blockOffset);
	if (!node) {
		ERROR("Erro ao localizar nodo específico do dado da coluna!");
		goto freeFillDataFromLocation;
	}

	// Transferencia de paternidade:
	node->dataOwner = false;

	data->content = node->data->content;
	data->usedSize = node->data->usedSize;
	data->allocSize = node->data->allocSize;
	
	ret = true;

freeFillDataFromLocation:
	if (block)
		block->~BlockCursorType();

	DBUG_RETURN(ret);
}

int vogal_manipulation::fetch(FilterCursorType * filter){
	// Aqui está acessando direto o disco - Ajustar para obter da memória 
	DBUG_ENTER("vogal_manipulation::fetch");

	int ret = false;

	if (!filter || !filter->cursor || !filter->cursor->table || !filter->cursor->table->block) {
		ERROR("Cursor de filtro inválido para efetuar o fetch!");
		goto freeFetch;
	}

	if (filter->empty) {
		DBUG_PRINT("INFO", ("Fim dos dados!"));
		goto freeFetch;
	}

	if (!filter->opened) {
		// Obtém o rid do dado a ser localizado
		filter->infoData = findNearest(filter->cursor, NULL, filter->data, filter->data->column->block);
		if (!filter->infoData || !filter->infoData->findedNode) {
			ERROR("Erro ao tentar localizar o dado!");
			goto freeFetch;
		}
		filter->opened = true;
	} else {
		// Varredura da árvore
		if (!filter->infoData->findedBlock) {
			ERROR("Nenhum bloco aberto na busca inicial. Impossível continuar!");
			goto freeFetch;
		}

		if (!comparison(filter->infoData, filter->cursor, NULL, filter->data, filter->infoData->offset+1)) {
			ERROR("Erro durante o processo de comparação dos dados para obtenção do próximo fetch!");
			goto freeFetch;
		}
	}

	// Verifica grau de identificação, no caso, tem que ser igual!
	if (!filter->infoData->findedNode || filter->infoData->comparison) {
		DBUG_PRINT("INFO", ("Dado não encontrado"));
		filter->empty = true;
		goto freeFetch;
	}

	// Obtém as localizações de todos os dados do rid localizado
	filter->infoFetch = findNearest(filter->cursor, filter->infoData->findedNode->rid, NULL, filter->cursor->table->block);
	if (!filter->infoFetch || !filter->infoFetch->findedNode) {
		ERROR("Erro ao tentar localizar o rid!");
		goto freeFetch;
	}

	filter->fetch = filter->infoFetch->findedNode->rid;
	if (!filter->fetch || !filter->fetch->dataList) {
		ERROR("Erro ao identificar RID localizado!");
		goto freeFetch;
	}

	// Por enquanto obtém todas as colunas
	// TODO: Pegar somente as colunas solicitadas
	for (int d = 0; d < vlCount(filter->fetch->dataList); d++)
		if (!fillDataFromLocation(filter->cursor, (DataCursorType*) vlGet(filter->fetch->dataList, d))) {
			ERROR("Erro ao preencher as colunas com os dados de seus deslocamentos");
			goto freeFetch;
		}

	ret = true;

freeFetch:
	DBUG_RETURN(ret);
}
