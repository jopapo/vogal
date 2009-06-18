
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
	SearchInfoType * info = findNearest(cursor, maxRid, NULL, cursor->table->blockId);
	if (info && info->findedNode && info->findedNode->rid) {
		id = info->findedNode->rid->id;
		info->~SearchInfoType();
	}
	maxRid->~RidCursorType();

	DBUG_RETURN(id + 1);
}

BigNumber vogal_manipulation::insertData(CursorType* cursor, ValueListRoot* dataList, BigNumber ridId) {
	DBUG_ENTER("vogal_manipulation::insertData");

	RidCursorType * newRid = NULL;
	
	if (!cursor) {
		ERROR("Impossível inserir dado com cursor fechado!");
		goto freeInsertData;
	}

	if (!dataList) {
		ERROR("Impossível inserir nada!");
		goto freeInsertData;
	}

	// Cria registro RID
	newRid = new RidCursorType();
	newRid->id = ridId;
	if (!newRid->id)
		newRid->id = nextRid(cursor);
	newRid->dataList = vlNew(true);
	
	fprintf(stderr, "New: %llu\n", newRid->id);
	
	// Reorganiza lista de dados para bater com a lista de colunas
	dataList->owner = false;
	for (int i = 0; i < vlCount(cursor->table->colsList); i++) {
		ColumnCursorType * column = (ColumnCursorType *) vlGet(cursor->table->colsList, i);
		DataCursorType * data = NULL;
		int d;
		for (d = 0; d < vlCount(dataList); d++) {
			data = (DataCursorType *) vlGet(dataList, d);
			if (!strcmp(column->name, data->column->name))
				break;
			data = NULL;
		}
		if (!data) {
			ERROR("Ainda não são permitidos campos nulos!");
			goto freeInsertData;
		}
		vlAdd(newRid->dataList, data);
		vlRemove(dataList, d);
	}
	
	if (vlCount(dataList) > 0) {
		ERROR("Dados de campos inválido para inclusão!");
		goto freeInsertData;
	}

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

	int ret = false;
	DataCursorType * data;
	BlockCursorType * block = NULL;
	NodeType * node;
	ValueListRoot * dataList = NULL;

	if (!filter->infoFetch || !filter->infoFetch->findedNode) {
		ERROR("Erro ao localizar nodo específico do RID!");
		goto freeRemoveFetch;
	}
	filter->infoFetch->findedNode->deleted = true;

	// Troca o dono da lista de dados
	dataList = filter->fetch->dataList;
	filter->fetch->dataList = NULL;

	// Atualiza e grava
	if (!updateBlockBuffer(filter->cursor, filter->infoFetch->findedBlock, 1)) {
		ERROR("Erro ao atualizar o buffer do bloco dos rids!");
		goto freeRemoveFetch;
	}

	if (!m_Handler->getStorage()->writeBlock(filter->infoFetch->findedBlock)) {
		ERROR("Erro ao gravar bloco dos rids!");
		goto freeRemoveFetch;
	}

	ret = true;

	// Atualiza dados
	for (int i = 0; i < vlCount(dataList); i++) {
		data = (DataCursorType *) vlGet(dataList, i);

		if (block)
			block->~BlockCursorType();
			
		block = m_Handler->getStorage()->openBlock(data->blockId);
		if (!block) {
			ERROR("Erro ao abrir bloco para remover dado da coluna!");
			goto freeRemoveFetch;
		}

		if (!m_Handler->getDefinition()->parseBlock(filter->cursor, data->column, block)) {
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
		if (!updateBlockBuffer(filter->cursor, block, 1)) {
			ERROR("Erro ao atualizar o buffer do bloco!");
			goto freeRemoveFetch;
		}

		if (!m_Handler->getStorage()->writeBlock(block)) {
			ERROR("Erro ao gravar bloco de dados!");
			goto freeRemoveFetch;
		}
		
	}

freeRemoveFetch:
	if (block)
		block->~BlockCursorType();
	
	DBUG_RETURN(ret);
}

CursorType * vogal_manipulation::openCursor(ObjectCursorType * object){
	DBUG_ENTER("vogal_manipulation::openCursor");

	int ret = false;

	CursorType *cursor = new CursorType();
	cursor->table = object;

	if (!cursor->table) {
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

		// TODO: Verificar como implementar campos nulos
		if (node->data->usedSize) {
			// Carga e alocação dos dados
			if (node->data->column) {
				switch (node->data->column->type) {
					case NUMBER:
						node->data->allocSize = sizeof(BigNumber); // Sempre é númeral máximo tratado, no caso LONG LONG INT (64BITS)
						if (node->data->usedSize > node->data->allocSize) {
							ERROR("Informação inconsistente na base pois não pode haver números maiores que 64 bits (20 dígitos)"); // Ainda...
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
				(*offset) += node->data->usedSize;
			}
		}
		
		// Lê o número do RID
		if (!m_Handler->getStorage()->readSizedNumber(offset, &node->rid->id)) {
			ERROR("Erro ao ler o rid do dado!");
			goto freeParseRecord;
		}
	}

	// Validação da leitura
	if (!node->rid->id) {
		ERROR("Rid com valor zerado!");
		goto freeParseRecord;
	}
	
	DBUG_RETURN(node);

freeParseRecord:
	if (node)
		node->~NodeType();
		
	DBUG_RETURN(NULL);

}

int vogal_manipulation::comparison(SearchInfoType * info, CursorType * cursor, RidCursorType * rid2find, DataCursorType * data2find) {
	DBUG_ENTER("vogal_manipulation::comparison");
	int ret = false;
	bool needsComparison;
	bool justGotUp = false;
	if (!info) {
		ERROR("As informações de busca já devem ter sido inicializadas!");
		goto freeComparison;
	}
	if (info->notFound) {
		DBUG_PRINT("INFO", ("Chegou ao fim das informações!"));
		ret = true;
		goto freeComparison;
	}
	// Inicializa (identifica quando não houver encontrado nada)
continueWhileComparison:
	if (!info->findedBlock) {
		ERROR("Por qual bloco começar?!");
		goto freeComparison;
	}
	if (!info->findedBlock->buffer) {
		ERROR("Bloco não bufferizado para efetuar o parse!");
		goto freeComparison;
	}
	needsComparison = ((info->findedBlock->header->type == C_BLOCK_TYPE_MAIN_TAB) && (rid2find)) ||
				      ((info->findedBlock->header->type == C_BLOCK_TYPE_MAIN_COL) && (data2find));
	if (!info->findedBlock->nodesList)
		if (!m_Handler->getDefinition()->parseBlock(cursor, data2find?data2find->column:NULL, info->findedBlock)) {
			ERROR("Erro ao obter os registros sem complementos do block!");
			goto freeComparison;
		}
	for (; info->findedBlock->searchOffset < vlCount(info->findedBlock->offsetsList); info->findedBlock->searchOffset++, info->findedBlock->navigatedSelf = false, info->findedBlock->navigatedLeft = false) {
		int comparison = -1;
		bool comparable = info->findedBlock->searchOffset < vlCount(info->findedBlock->nodesList);
		if (comparable) {
			info->findedNode = (NodeType *) vlGet(info->findedBlock->nodesList, info->findedBlock->searchOffset);
			if (needsComparison) {
				if (info->findedBlock->header->type == C_BLOCK_TYPE_MAIN_TAB) {
					comparison = DIFF(rid2find->id, info->findedNode->rid->id);
				} else {
					// Comparação
					switch (data2find->column->type) {
						case NUMBER:
							comparison = DIFF((*(BigNumber*)data2find->content), (*(BigNumber*)info->findedNode->data->content));
							break;
						case VARCHAR:
							// Não é necessário definir tamanho pois teminam em \0
							comparison = strcmp((char *) data2find->content, (char *) info->findedNode->data->content); 
							break;
						default:
							ERROR("Comparação não preparada para o tipo!");
							goto freeComparison;
					}
				}
			}
		}
		if (info->findedBlock->navigatedSelf)
			continue;
		if (info->findedBlock->navigatedLeft) {
			if (comparable)
				goto okComparison;
			break;
		}
		// Se for o dado for maior que o a ser procurado e terminou a busca
		if (comparison <= 0 || !needsComparison) {
			if (comparison || !needsComparison) {
				BlockOffset * neighbor = (BlockOffset *) vlGet(info->findedBlock->offsetsList, info->findedBlock->searchOffset);
				if (!justGotUp && neighbor && (*neighbor)) {
					info->findedBlock->navigatedLeft = true;
					if (info->findedBlock != info->rootBlock) {
						if (!info->blocksList)
							info->blocksList = vlNew(false);
						vlAdd(info->blocksList, info->findedBlock);
					}
					info->findedBlock = m_Handler->getStorage()->openBlock((*neighbor));
					//justGotUp = true;
					goto continueWhileComparison;
				}
				justGotUp = false;
			}
			if (comparable && (!needsComparison || !comparison))
				goto okComparison;
			break;
		}
	}

	// Sobe se houverem blocos (Só sobe em caso de varredura)
	if (!needsComparison && info->findedBlock->searchOffset >= vlCount(info->findedBlock->nodesList)) {
		if ((info->blocksList && vlCount(info->blocksList) > 0) || info->findedBlock != info->rootBlock) {
			if (info->blocksList && vlCount(info->blocksList) > 0) {
				info->findedBlock = (BlockCursorType *) vlGet(info->blocksList, vlCount(info->blocksList) - 1);
				vlRemove(info->blocksList, vlCount(info->blocksList) - 1); // Remove da pilha
			} else {
				info->findedBlock = info->rootBlock;
			}
			goto continueWhileComparison;
		}
	}
	info->notFound = true;
okComparison:
	info->findedBlock->navigatedSelf = true;
	ret = true;
freeComparison:
	DBUG_RETURN(ret);
}

SearchInfoType * vogal_manipulation::findNearest(CursorType * cursor, RidCursorType * rid2find, DataCursorType * data2find, BlockOffset rootBlock) {
	DBUG_ENTER("vogal_manipulation::findNearest");

	// Declara
	SearchInfoType * info = new SearchInfoType();
	info->rootBlock = m_Handler->getStorage()->openBlock(rootBlock);
	info->blocksList = vlNew(true);
	info->findedBlock = info->rootBlock;

	if (!comparison(info, cursor, rid2find, data2find)) {
		ERROR("Erro durante o processo de comparação!");
		info->~SearchInfoType();
		DBUG_RETURN(NULL);
	}

	DBUG_RETURN(info);
}

int vogal_manipulation::blockSplit(CursorType * cursor, SearchInfoType * info, BlockCursorType * block, int validValues) {
	DBUG_ENTER("vogal_manipulation::blockSplit");

	// Declara variáveis
	int ret = false;
	int middle = validValues / 2;
	int count;
	int parentDeleted = 0;
	int brotherDeleted = 0;
	bool updatingColumnBlock;
	bool parentIsUp = false;
	BlockOffset * blockIdAux;
	NodeType * nodeAux;
	RidCursorType * parentRid = NULL;
	SearchInfoType * parentInfo = NULL;
	CursorType * parentCursor = NULL;
	ColumnCursorType * locationColumn;
	DataCursorType * locationRidData;
	NodeType * locationNode;
	BlockCursorType	* locationBlock = NULL;
	BlockCursorType * splitBlockParent = NULL;
	BlockCursorType * splitBlockBrother = NULL;
	
	// Se não houver informação disponível da fila de blocos, não permite informações maiores que um bloco
	if (!info || !block || !cursor) {
		ERROR("Impossível armazenar informação que exceda um bloco sem que seja informado sua paternidade!");
		goto freeBlockSplit;
	}

	// Inicializa o que não pode ser inicializado na declaração
	count = vlCount(block->nodesList);
	updatingColumnBlock = block->header->type == C_BLOCK_TYPE_MAIN_COL;

	if (block == info->rootBlock) {
		// Abre o bloco e divide as informações
		splitBlockParent = new BlockCursorType();
		splitBlockParent->buffer = vogal_cache::blankBuffer();		
		// Cria bloco pai
		if (!m_Handler->getCache()->lockFreeBlock(&splitBlockParent->id)) {
			ERROR("Impossível obter bloco pai livre para dividir bloco sobrecarregado!");
			goto freeBlockSplit;
		}
		// Monta estrutura do bloco
		if (!m_Handler->getDefinition()->createStructure(block->header->type, splitBlockParent->buffer)) {
			ERROR("Erro ao criar estrutura do bloco pai!");
			goto freeBlockSplit;
		}
		// Adiciona o item central no bloco pai
		splitBlockParent->searchOffset = 0;
		splitBlockParent->nodesList = vlNew(true);
		splitBlockParent->offsetsList = vlNew(true);

		// Referência à esquerda só precisa se o a raiz é nova, pois se chegou aqui é pq já possui uma referência
		blockIdAux = new BlockOffset();
		(*blockIdAux) = block->id;
		vlAdd(splitBlockParent->offsetsList, blockIdAux);
		
		info->rootBlock = splitBlockParent;

		// Atualiza referência à nova raiz
		parentRid = new RidCursorType();
		// Abre cursor para a tabela mãe específica
		if (updatingColumnBlock) {
			parentRid->id = info->findedNode->data->column->ridNumber;
			parentCursor = openCursor(m_Handler->getCache()->openColumns());
			// Importante: Atualiza também na estrutura existente em memória da tabela
			info->findedNode->data->column->blockId = info->rootBlock->id;
		} else {
			parentRid->id = cursor->table->ridNumber;
			parentCursor = openCursor(m_Handler->getCache()->openObjects());
			// Importante: Atualiza também na estrutura existente em memória da tabela
			cursor->table->blockId = info->rootBlock->id;
		}
		if (!parentCursor) {
			ERROR("Erro ao abrir cursor do metadados para atualizar localização do nó raiz!");
			goto freeBlockSplit;
		}
		parentInfo = findNearest(parentCursor, parentRid, NULL, parentCursor->table->blockId);
		if (!parentInfo || parentInfo->notFound) {
			ERROR("Erro durante a busca do RID para atualização do nó raiz!");
			goto freeBlockSplit;
		}
		locationColumn = m_Handler->getDefinition()->findColumn(parentCursor->table, C_LOCATION_KEY);
		if (!locationColumn) {
			ERROR("Coluna de localização do bloco não existe na tabela!");
			goto freeBlockSplit;
		}
		locationRidData = (DataCursorType*) vlGet(parentInfo->findedNode->rid->dataList, locationColumn->getId());
		if (!locationRidData) {
			ERROR("Dado da coluna de localização do bloco não existe na tabela!");
			goto freeBlockSplit;
		}
		// Achou a posição do bicho, então busca bloco todo
		locationBlock = m_Handler->getStorage()->openBlock(locationRidData->blockId);
		if (!locationBlock) {
			ERROR("Erro ao abrir bloco onde se localiza a informação de posição dos blocos filhos!");
			goto freeBlockSplit;
		}
		// Faz o parse do bloco
		if (!m_Handler->getDefinition()->parseBlock(parentCursor, locationColumn, locationBlock)) {
			ERROR("Erro ao dividir bloco em registros para identificar localização dos blocos filhos!");
			goto freeBlockSplit;
		}
		// Obtém o dado em si
		locationNode = (NodeType*) vlGet(locationBlock->nodesList, locationRidData->blockOffset);
		if (!locationNode) {
			ERROR("Erro ao obter dado do bloco onde se localiza a informação de posição dos blocos filhos!");
			goto freeBlockSplit;
		}
		// Marca como removido
		locationNode->deleted = true;
		// Atualiza bloco
		if (!updateBlockBuffer(parentCursor, locationBlock, 1)) {
			ERROR("Erro ao preencher memória do bloco pai com dados concretos!");
			goto freeBlockSplit;
		}
		// Efetiva gravação
		if (!m_Handler->getStorage()->writeBlock(locationBlock)) {
			ERROR("Erro ao gravar no disco o bloco pai com dados concretos!");
			goto freeBlockSplit;
		}
		// Ajusta localização do bloco
		locationNode->data->usedSize = sizeof(BigNumber);
		if (!locationNode->data->content)
			locationNode->data->content = (GenericPointer) new BigNumber();
		(* (BigNumber*) locationNode->data->content ) = info->rootBlock->id;
		// Agora reinsere
		if (!writeData(parentCursor, locationNode->rid, locationNode->data)) {
			ERROR("Erro ao regravar no disco o bloco pai com dados concretos!");
			goto freeBlockSplit;
		}
		// Atualiza posição no RID pai
		locationRidData->blockId = locationNode->data->blockId;
		locationRidData->blockOffset = locationNode->data->blockOffset;
		// Atualiza bloco
		if (!updateBlockBuffer(parentCursor, parentInfo->findedBlock)) {
			ERROR("Erro ao preencher memória do bloco pai com dados concretos do RID!");
			goto freeBlockSplit;
		}
		// Efetiva gravação
		if (!m_Handler->getStorage()->writeBlock(parentInfo->findedBlock)) {
			ERROR("Erro ao gravar no disco o bloco pai com dados concretos do RID!");
			goto freeBlockSplit;
		}
		
	} else if (info->blocksList && vlCount(info->blocksList) > 0) {
		parentIsUp = true;
		splitBlockParent = (BlockCursorType*) vlGet(info->blocksList, vlCount(info->blocksList) - 1);
		vlRemove(info->blocksList, vlCount(info->blocksList) - 1, false);
	} else {
		splitBlockParent = info->rootBlock;
	}
	
	// Varredura para não pegar um registro que será excluído
	nodeAux = NULL;
	for (; middle < count; middle++) {
		nodeAux = (NodeType *) vlGet(block->nodesList, middle);
		if (nodeAux->deleted)
			continue;
		break;
	}
	if (!nodeAux) {
		ERROR("Erro ao localizar nodo central!");
		goto freeBlockSplit;
	}

	// Abre bloco irmao
	splitBlockBrother = new BlockCursorType();
	splitBlockBrother->buffer = vogal_cache::blankBuffer();
	
	// Cria bloco irmão
	if (!m_Handler->getCache()->lockFreeBlock(&splitBlockBrother->id)) {
		ERROR("Impossível obter bloco irmão livre para dividir bloco sobrecarregado!");
		goto freeBlockSplit;
	}
	// Monta estrutura do bloco
	if (!m_Handler->getDefinition()->createStructure(block->header->type, splitBlockBrother->buffer)) {
		ERROR("Erro ao criar estrutura do bloco irmão!");
		goto freeBlockSplit;
	}

	// Adiciona os demais no bloco mano
	// TODO: Remover offsets desnecessários (Na folha sempre serão zerados!)
	splitBlockBrother->nodesList = vlNew(true);
	splitBlockBrother->offsetsList = vlNew(true);
	brotherDeleted = 0;
	for (int i = count - 1; i > middle; i--) {
		nodeAux = (NodeType*) vlGet(block->nodesList, i);
		if (nodeAux->deleted) {
			brotherDeleted++;
			continue;
		}
		// Atualiza deslocamento do nó
		if (updatingColumnBlock) {
			nodeAux->data->blockId = splitBlockBrother->id;
			nodeAux->data->blockOffset = i - middle - 1 - brotherDeleted;
			if (!nodeAux->inserted)
				if (!updateBlockOffset(cursor, nodeAux)) {
					ERROR("Erro ao atualizar o deslocamento do nó irmão!");
					goto freeBlockSplit;
				}
		}
		// Move nó
		vlInsert(splitBlockBrother->nodesList, nodeAux, 0);
		vlRemove(block->nodesList, i, false);
		vlInsert(splitBlockBrother->offsetsList, vlGet(block->offsetsList, i), 0);
		vlRemove(block->offsetsList, i, false);
	}
	// Adiciona um deslocamento a mais para representar o nó a direita
	blockIdAux = new BlockOffset();
	(*blockIdAux) = 0;
	vlAdd(splitBlockBrother->offsetsList, blockIdAux);

	// Referência do pai à direita
	blockIdAux = new BlockOffset();
	(*blockIdAux) = splitBlockBrother->id;

	// Adiciona nó pai
	nodeAux = (NodeType*) vlGet(block->nodesList, middle);
	vlInsert(splitBlockParent->nodesList, nodeAux, splitBlockParent->searchOffset);
	vlInsert(splitBlockParent->offsetsList, blockIdAux, splitBlockParent->searchOffset + 1);
	vlRemove(block->nodesList, middle, false);
	vlRemove(block->offsetsList, middle); // Remove referência desnecessária
	// Atualiza deslocamento do nó só se for dado
	if (updatingColumnBlock) {
		parentDeleted = 0;
		for (int i = splitBlockParent->searchOffset; i < vlCount(splitBlockParent->nodesList); i++) {
			nodeAux = (NodeType*) vlGet(splitBlockParent->nodesList, i);
			if (nodeAux->deleted) {
				parentDeleted++;
				continue;
			}
			nodeAux->data->blockId = splitBlockParent->id;
			nodeAux->data->blockOffset = i - parentDeleted;

			if (!nodeAux->inserted)
				if (!updateBlockOffset(cursor, nodeAux)) {
					ERROR("Erro ao atualizar restante do deslocamento do nó pai!");
					goto freeBlockSplit;
				}
		}
	}

	// Atualiza buffers (somente para o info para o parent pois o brother NÃO PODE exceder)
	if (!updateBlockBuffer(cursor, splitBlockParent, parentDeleted, info) || 
		!updateBlockBuffer(cursor, splitBlockBrother, brotherDeleted)) {
		ERROR("Erro ao preencher memória do bloco com dados concretos!");
		goto freeBlockSplit;
	}

	// Se estava na lista, recoloca para não dar problema no restante da gravação
	if (parentIsUp)
		vlAdd(info->blocksList, splitBlockParent);

	// Efetiva gravação
	if (!m_Handler->getStorage()->writeBlock(splitBlockParent) ||
		!m_Handler->getStorage()->writeBlock(splitBlockBrother)) {
		ERROR("Erro ao gravar no disco o bloco com dados concretos!");
		goto freeBlockSplit;
	}

	ret = true;

freeBlockSplit:
	if (parentInfo)
		parentInfo->~SearchInfoType();
	if (parentRid)
		parentRid->~RidCursorType();
	if (parentCursor)
		parentCursor->~CursorType();
	if (splitBlockBrother)
		splitBlockBrother->~BlockCursorType();
	if (locationBlock)
		locationBlock->~BlockCursorType();
		
	DBUG_RETURN(ret);
}

BigNumber vogal_manipulation::neededSpace(BlockCursorType * block) {
	DBUG_ENTER("vogal_manipulation::neededSpace");

	#define SIZE_AVG_COUNT 2
	#define SIZE_AVG_BLOCK 6
	
	BigNumber count = vlCount(block->nodesList);
	BigNumber removed = 0;
	BigNumber space = sizeof(BlockHeaderType) + // Cabeçalho
		SIZE_AVG_COUNT; // Tamanho necessário para representar a qtd registros
	for (int i = 0; i < count; i++) {
		NodeType * node = (NodeType *) vlGet(block->nodesList, i);
		if (node->deleted) {
			removed++;
			continue;
		}
		if (block->header->type == C_BLOCK_TYPE_MAIN_TAB) {
			space += SIZE_AVG_BLOCK + // Rid
				vlCount(node->rid->dataList) * ( 6 + SIZE_AVG_COUNT ); // Blocos (6) + Deslocamentos (2)
		} else {
			space += node->data->usedSize + // Dado da chave
				SIZE_AVG_BLOCK; // Rid
		}
	}
	space +=
		SIZE_AVG_BLOCK * (count - removed); // Quantidade de ponteiros à esquerda dos nodos
	if (count - removed) // Se houver dados, um ponteiro à direita
		space += SIZE_AVG_BLOCK;

	DBUG_RETURN(space);
}

// TODO: Estudar o que fazer no caso de nós solitários, ou seja, que só cabe 1 por bloco (Tvz permitir dividí-los)
int vogal_manipulation::updateBlockBuffer(CursorType * cursor, BlockCursorType * block, int removed, SearchInfoType * info) {
	DBUG_ENTER("vogal_manipulation::updateBlockBuffer");

	int ret = false;
	int count = 0;
	GenericPointer p;
	BlockOffset * neighbor;
	NodeType * node;
	int validValues;

	if (!block) {
		ERROR("Bloco inválido para atualização!");
		goto freeUpdateBlockBuffer;
	}
	
	if (block->nodesList)
		count = vlCount(block->nodesList);

	// Estimar tamanho necessário.
	// TODO: Melhorar a estimativa de campos numéricos - Está pegando sempre máximo em relação ao tamanho da base de dados
	validValues = count - removed;

	// Se não há mais dados no bloco, estudar possibilidade de eliminá-lo
	if (!validValues) {
		// Se houver informações, verifica se o bloco não é raiz e mescla ele com seu irmão
		if (info) {
			// Por enquanto grava o bloco vazio.
		}
	}
	// Se o espaço necessário for maior que o disponível no bloco, deve separá-lo (split)
	// TODO: Testar formações de campos que sejam muito grandes, ou seja, maior que o bloco de 1024 bytes
	else if (neededSpace(block) > C_BLOCK_SIZE) {
		if (!blockSplit(cursor, info, block, validValues)) {
			ERROR("Impossível efetuar a divisão do bloco!");
			goto freeUpdateBlockBuffer;
		}
		// Recalcula pois foi dividido
		count = vlCount(block->nodesList);
		validValues = count - removed;
	}

		
	p = block->buffer + sizeof(BlockHeaderType);
	if (!m_Handler->getStorage()->writeDataSize(&p, count - removed)) {
		ERROR("Erro ao escrever a quantidade de dados do bloco!");
		goto freeUpdateBlockBuffer;
	}

	if (count > 0) {
		int writed = 0;
		int deleted = 0;
		int added = 0;
		for (int i = 0; i < count; i++) {
			node = (NodeType *) vlGet(block->nodesList, i);
			if (node->deleted) {
				deleted++;
				continue;
			}
			if (node->inserted) {
				added++;
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
				if (!m_Handler->getStorage()->writeData(&p, node->data->content, node->data->usedSize, node->data->column->type)) {
					ERROR("Erro ao escrever o dado chave no buffer da coluna!");
					goto freeUpdateBlockBuffer;
				}
				// Escreve o número do RID
				if (!m_Handler->getStorage()->writeNumber(node->rid->id, &p)) {
					ERROR("Erro ao escrever o rid no buffer da coluna!");
					goto freeUpdateBlockBuffer;
				}
				// Atualiza a informação de localização
				node->data->blockId = block->id;
				node->data->blockOffset = i - deleted;
				// Se excluiu algum antes, atualiza pais
				if (!node->inserted && (deleted || added)) {
					// TODO: Otimizar para fazer todas as operações em memória e gravar em disco apenas uma vez
					if (!updateBlockOffset(cursor, node)) {
						ERROR("Erro ao atualizar o deslocamento das colunas do bloco!");
						goto freeUpdateBlockBuffer;
					}
				}
			}
			writed++;
		}
	}
	// Sempre grava o nó da direita // Se não existir, grava vazio
	if (vlCount(block->offsetsList) < count + 1) {
		neighbor = new BlockOffset();
		(*neighbor) = 0;
	} else 
		neighbor = (BlockOffset *) vlGet(block->offsetsList, count);
	if (!m_Handler->getStorage()->writeNumber((*neighbor), &p)) {
		ERROR("Erro escrever o ponteiro para o nodo da direita!");
		goto freeUpdateBlockBuffer;
	}

	ret = true;
	
freeUpdateBlockBuffer:
	DBUG_RETURN(ret);
}

int vogal_manipulation::updateBlockOffset(CursorType * cursor, NodeType * node) {
	DBUG_ENTER("vogal_manipulation::updateBlockOffset");

	// Declarações
	int ret = false;
	SearchInfoType * info;

	// Se não achou em memória, faz a busca na base
	info = findNearest(cursor, node->rid, NULL, cursor->table->blockId);
	if (info && !info->notFound) {
		// Procura a coluna com offset atualizado
		DataCursorType * data = (DataCursorType *) vlGet(info->findedNode->rid->dataList, node->data->column->getId());
		if (!data) {
			ERROR("A coluna a com deslocamento a ser atualizado não foi encontrada!");
			goto freeUpdateBlockOffset;
		}
		
		data->blockId = node->data->blockId;
		data->blockOffset = node->data->blockOffset;

		// TODO: CUIDADO!!! Nunca encher muito o bloco pois na atualização do deslocamento normalmente se ocupam mais ou menos blocos na
		//		hora de escrevê-lo. Portanto, nunca encher o bloco por completo
		if (!updateBlockBuffer(cursor, info->findedBlock)) {
			ERROR("Erro ao atualizar buffer da coluna com deslocamento");
			goto freeUpdateBlockOffset;
		}

		if (!m_Handler->getStorage()->writeBlock(info->findedBlock)) {
			ERROR("Erro ao gravar bloco de dados com o deslocamento atualizado!");
			goto freeUpdateBlockOffset;
		}

		ret = true;
	} else {
		ERROR("Impossível atualizar o deslocamento no rid!");
		goto freeUpdateBlockOffset;
	}

freeUpdateBlockOffset:
	if (info)
		info->~SearchInfoType();

	DBUG_RETURN(ret);
}

// TODO: Tornar o índice balanceado
int vogal_manipulation::writeData(CursorType * cursor, RidCursorType * rid, DataCursorType * data) {
	DBUG_ENTER("vogal_manipulation::writeData");
	
	// Declaração das variáveis
	int ret = false;
	SearchInfoType *info = NULL;
	NodeType *node;
	BlockOffset * neighbor;

	// Procura o dado mais próximo
	info = findNearest(cursor, rid, data, data?data->column->blockId:cursor->table->blockId);
	if (!info) { 
		ERROR("Erro ao obter bloco para gravação do dado!");
		goto freeWriteData;
	}

	// Cria novo nodo e insere no local adequado
	node = new NodeType();
	node->rid = rid;
	node->data = data;
	node->inserted = true;
	
	vlInsert(info->findedBlock->nodesList, node, info->findedBlock->searchOffset);

	// Esquerda e direita apontando para o vazio!
	neighbor = new BlockOffset();
	(*neighbor) = 0;
	vlInsert(info->findedBlock->offsetsList, neighbor, info->findedBlock->searchOffset);
	// Somente insere vizinho da direita se for o primeiro rid do bloco
	if (vlCount(info->findedBlock->offsetsList) == vlCount(info->findedBlock->nodesList) == 1) {
		neighbor = new BlockOffset();
		(*neighbor) = 0;
		vlInsert(info->findedBlock->offsetsList, neighbor, info->findedBlock->searchOffset + 1);
	}

	if (!updateBlockBuffer(cursor, info->findedBlock, 0, info)) {
		ERROR("Erro ao atualizar o buffer do bloco!");
		goto freeWriteData;
	}

	if (!m_Handler->getStorage()->writeBlock(info->findedBlock)) {
		ERROR("Erro ao gravar bloco de dados!");
		goto freeWriteData;
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
	if (!filter || !filter->cursor || !filter->cursor->table) {
		ERROR("Cursor de filtro inválido para efetuar o fetch!");
		goto freeFetch;
	}
	filter->fetch = NULL;

	if (filter->notFound)
		goto fetchEmpty;

	if (filter->data) {
		if (!filter->opened) {
			// Obtém o rid do dado a ser localizado
			filter->infoData = findNearest(filter->cursor, NULL, filter->data, filter->data->column->blockId);
			if (!filter->infoData) {
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
			if (!comparison(filter->infoData, filter->cursor, NULL, filter->data)) {
				ERROR("Erro durante o processo de comparação dos dados para obtenção do próximo fetch!");
				goto freeFetch;
			}
		}
	} else {
		if (!filter->opened) {
			// Obtém o rid do dado a ser localizado
			filter->infoFetch = findNearest(filter->cursor, NULL, NULL, filter->cursor->table->blockId);
			if (!filter->infoFetch) {
				ERROR("Erro ao tentar localizar o rid!");
				goto freeFetch;
			}
			filter->opened = true;
		} else {
			// Varredura da árvore
			if (!filter->infoFetch->findedBlock) {
				ERROR("Nenhum bloco aberto na busca inicial. Impossível continuar!");
				goto freeFetch;
			}
			if (!comparison(filter->infoFetch, filter->cursor, NULL, NULL)) {
				ERROR("Erro durante o processo de comparação dos dados para obtenção do próximo fetch!");
				goto freeFetch;
			}
		}
	}

	// Verifica grau de identificação, no caso, tem que ser igual!
	if (filter->infoData) {
		if (filter->infoData->notFound)
			goto fetchEmpty;
		// Obtém as localizações de todos os dados do rid localizado
		filter->infoFetch = findNearest(filter->cursor, filter->infoData->findedNode->rid, NULL, filter->cursor->table->blockId);
	}
 	if (!filter->infoFetch) {
		ERROR("Erro ao tentar localizar o rid!");
		goto freeFetch;
	}
	if (filter->infoFetch->notFound)
		goto fetchEmpty;
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
	filter->count ++;
	ret = true;
freeFetch:
	DBUG_RETURN(ret);

fetchEmpty:
	DBUG_PRINT("INFO", ("Cursor no fim dos dados!"));
	filter->notFound = true;
	DBUG_RETURN(true);

}
