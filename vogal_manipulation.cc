
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
	data->allocSize = strlen(name);
	data->content = (GenericPointer) name;
	if (!vlAdd(dataList, data))
		goto freeCreateObjectData;

	data = new DataCursorType();
	data->column = findColumn(cursor, C_TYPE_KEY);
	if (!data->column)
		goto freeCreateObjectData;
	data->content = (GenericPointer) type;
	data->allocSize = strlen(type);
	if (!vlAdd(dataList, data))
		goto freeCreateObjectData;

	data = new DataCursorType();
	data->column = findColumn(cursor, C_LOCATION_KEY);
	if (!data->column)
		goto freeCreateObjectData;
	data->content = (GenericPointer) &location;
	data->allocSize = sizeof(location);
	if (!vlAdd(dataList, data))
		goto freeCreateObjectData;

	DBUG_RETURN(dataList);
	
freeCreateObjectData:
	if (data)
		data->~DataCursorType();
	vlFree(dataList);

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
	data->allocSize = sizeof(tableRid);
	if (!vlAdd(dataList, data))
		goto freeCreateColumnData;

	data = new DataCursorType();
	data->column = findColumn(cursor, C_NAME_KEY);
	if (!data->column)
		goto freeCreateColumnData;
	data->allocSize = strlen(name);
	data->content = (GenericPointer) name;
	if (!vlAdd(dataList, data))
		goto freeCreateColumnData;

	data = new DataCursorType();
	data->column = findColumn(cursor, C_TYPE_KEY);
	if (!data->column)
		goto freeCreateColumnData;
	data->content = (GenericPointer) type;
	data->allocSize = strlen(type);
	if (!vlAdd(dataList, data))
		goto freeCreateColumnData;

	data = new DataCursorType();
	data->column = findColumn(cursor, C_LOCATION_KEY);
	if (!data->column)
		goto freeCreateColumnData;
	data->content = (GenericPointer) &location;
	data->allocSize = sizeof(location);
	if (!vlAdd(dataList, data))
		goto freeCreateColumnData;

	DBUG_RETURN(dataList);
	
freeCreateColumnData:
	if (data)
		data->~DataCursorType();
	vlFree(dataList);

	DBUG_RETURN(NULL);
}

BigNumber vogal_manipulation::nextRid(CursorType * cursor) {
	DBUG_ENTER("vogal_manipulation::nextRid");

	BigNumber id = 0;
	
	RidCursorType * maxRid = new RidCursorType();
	maxRid->id = -1; // Não aceita negativo, portanto se tornará o maior valor possível

	// Busca o RID mais próximo
	SearchInfoType * info = findNearest(cursor, maxRid, NULL, cursor->table->block);
	if (info) {
		id = info->findedRid->id;
		info->~SearchInfoType();
	}

	DBUG_RETURN(id + 1);
}

RidCursorType * vogal_manipulation::insertData(CursorType* cursor, ValueListRoot* data){
	DBUG_ENTER("vogal_manipulation::insertData");

	int ret = false;
	GenericPointer p;

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

	ret = true;	
	
freeInsertData:
	if (!ret && newRid) {
		newRid->~RidCursorType();
		newRid = NULL;
	}

	DBUG_RETURN(newRid);
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

RidCursorType * vogal_manipulation::parseRecord(CursorType * cursor, ColumnCursorType * column, BlockCursorType * block, GenericPointer * offset, bool loadData) {
	DBUG_ENTER("vogal_manipulation::parseRecord");

	// Declara
	DataCursorType * data = NULL;
	RidCursorType * rid = NULL;

	// Inicializa
	rid = new RidCursorType();
	if (loadData || block->header->type == C_BLOCK_TYPE_MAIN_COL)
		rid->dataList = vlNew(true);
	
	if (block->header->type == C_BLOCK_TYPE_MAIN_TAB) {
		if (!m_Handler->getStorage()->readSizedNumber(offset, &rid->id)) {
			perror("Erro ao ler o rid do registro!");
			goto freeParseRecord;
		}

		// Erro!!!
		if (!rid->id) {
			perror("Rid com valor zerado!");
			goto freeParseRecord;
		}
		
		// Lê deslocamentos
		for (int i = 0; i < vlCount(cursor->table->colsList); i++) {
			if (loadData) {
				data = new DataCursorType();
				data->column = (ColumnCursorType*) vlGet(cursor->table->colsList, i);
				// o endereço do bloco (tamanho + localização)
				if (!m_Handler->getStorage()->readSizedNumber(offset, &data->blockId)) {
					perror("Erro ao ler dados da árvore de registros");
					goto freeParseRecord;
				}
				// o offset no bloco (tamanho é o offset)
				if (!m_Handler->getStorage()->readDataSize(offset, &data->blockOffset)) {
					perror("Erro ao ler dados de tamanho da árvore de registros");
					goto freeParseRecord;
				}
				// Adiciona informação à lista
				vlAdd(rid->dataList, data);
				data = NULL;

				continue;
			}
			// Pula
			if (!m_Handler->getStorage()->skipData(offset)) {
				perror("Erro ao pular dados da árvore de registros");
				goto freeParseRecord;
			}
			if (!m_Handler->getStorage()->skipDataSize(offset)) {
				perror("Erro ao pular dados de tamanho da árvore de registros");
				goto freeParseRecord;
			}
		}
	} else {
		if (!column) {
			perror("A coluna é obrigatória na leitura de blocos de colunas!");
			goto freeParseRecord;
		}

		DataCursorType * data = new DataCursorType();
		data->column = column;
		
		// Obtém tamanho do dado
		if (!m_Handler->getStorage()->readDataSize(offset, &data->usedSize)) {
			perror("Erro ao ler o tamanho do dado!");
			goto freeParseRecord;
		}

		// Campo nulo.. ? Ainda não implementado
		if (!data->usedSize) {
			perror("Campos nulos ainda não foram implementados, como ele apareceu aí?");
			goto freeParseRecord;
		}

		// Carga e alocação dos dados
		switch (data->column->type) {
			case NUMBER:
				if (!data->content) {
					data->allocSize = sizeof(BigNumber); // Sempre é númeral máximo tratado, no caso LONG LONG INT (64BITS)
					data->content = (GenericPointer) malloc(data->allocSize);
				}
				if (data->usedSize > data->allocSize) {
					perror("Informação inconsistente na base pois não pode haver númeoros maiores que 64 bits (20 dígitos)");
					goto freeParseRecord;
				}
				break;
			case VARCHAR:
				// Tamanho máximo permitido até o momento
				if (data->usedSize >= data->allocSize) {
					if (data->content)
						free(data->content);
					data->allocSize = data->usedSize + 1; // Mais 1 para o Null Terminated String
					data->content = (GenericPointer) malloc(data->allocSize);
				}
				break;
			default: // TODO (Pendência): Implementar comparação de todos os tipos de dados
				perror("Tipo de campo ainda não implementado!");
				goto freeParseRecord;
		}
		// Zera para evitar informação errônea
		memset(data->content, 0x00, data->allocSize);
		// Lê o dado
		if (!m_Handler->getStorage()->readData(offset, data->content, data->usedSize, data->allocSize, data->column->type)) {
			perror("Erro ao ler dado do bloco!");
			goto freeParseRecord;
		}

		// Lê o número do RID
		if (!m_Handler->getStorage()->readSizedNumber(offset, &rid->id)) {
			perror("Erro ao ler o rid do dado!");
			goto freeParseRecord;
		}

		// Adiciona informação à lista
		vlAdd(rid->dataList, data);
		data = NULL;
	}

	DBUG_RETURN(rid);

freeParseRecord:
	if (data)
		data->~DataCursorType();
	if (rid)
		rid->~RidCursorType();
		
	DBUG_RETURN(NULL);

}

int vogal_manipulation::parseBlock(CursorType * cursor, ColumnCursorType * column, BlockCursorType * block, bool loadData) {
	DBUG_ENTER("vogal_manipulation::parseBlock");
	
	// Declara variáveis
	int ret = false;
	ValueListRoot *list = NULL;
	RidCursorType *rid;
	BlockOffset *nighbor;
	BigNumber dataCount;

	if (!block) {
		perror("É obrigatório informar o bloco para efetuar o parse");
		goto freeParseBlock;
	}

	// Monta lista de rids e offsets do bloco
	if (block->ridsList)
		vlFree(block->ridsList);
	block->ridsList = vlNew(true);
	if (block->offsetsList)
		vlFree(block->offsetsList);
	block->offsetsList = vlNew(true);

	// Posição atual
	block->offset = block->buffer + sizeof(BlockHeaderType);

	// Lê a quantidade de registros do bloco
	if (!m_Handler->getStorage()->readDataSize(&block->offset, &dataCount)) {
		perror("Erro ao ler a quantidade de registros no block");
		goto freeParseBlock;
	}

	if (dataCount > 0) {
		for (int i = 0; i < dataCount; i++) {
			// Auxiliares
			nighbor = new BlockOffset();
			// Lê primeiro nó a esquerda
			if (!m_Handler->getStorage()->readSizedNumber(&block->offset, nighbor)) {
				perror("Erro ao ler nó a esquerda da árvore de registros");
				free(nighbor);
				goto freeParseBlock;
			}
			// Adiciona informação à lista
			vlAdd(block->offsetsList, nighbor);

			rid = new RidCursorType();
			if (!parseRecord(cursor, column, block, &block->offset, loadData)) {
				rid->~RidCursorType();
				goto freeParseBlock;
			}

			// Adiciona informação à lista
			vlAdd(block->ridsList, rid);
		}

		// Se a quantidade for maior que zero
		// Depois do último nó sempre tem o nó a direita
		nighbor = new BlockOffset();
		if (!m_Handler->getStorage()->readSizedNumber(&block->offset, nighbor)) {
			perror("Erro ao ler nó a direita da árvore de registros");
			free(nighbor);
			goto freeParseBlock;
		}
		// Adiciona informação à lista
		vlAdd(block->offsetsList, nighbor);
	}

	ret = true;
		
freeParseBlock:
	DBUG_RETURN(ret);

}

SearchInfoType * vogal_manipulation::findNearest(CursorType * cursor, RidCursorType * rid2find, DataCursorType * data2find, BlockCursorType * rootBlock) {
	DBUG_ENTER("vogal_manipulation::findNearest");

	// Inicializa variáveis
	SearchInfoType * info = new SearchInfoType();
	info->rootBlock = rootBlock;
	info->blocksList = vlNew(true);
	info->findedBlock = rootBlock;
	int count;

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
		if (!parseBlock(cursor, data2find?data2find->column:NULL, info->findedBlock, false)) {
			perror("Erro ao obter os registros sem complementos do block!");
			goto freeFindNearest;
		}
		count = vlCount(info->findedBlock->ridsList);
		if (count <= 0) {
			DBUG_PRINT("INFO", ("Bloco vazio!"));
			goto freeFindNearest;
		}

		for (int i = 0; i < vlCount(info->findedBlock->ridsList); i++) {
			BlockOffset * left = (BlockOffset *) vlGet(info->findedBlock->offsetsList, i);
			info->findedRid = (RidCursorType *) vlGet(info->findedBlock->ridsList, i);

			if (info->findedBlock->header->type == C_BLOCK_TYPE_MAIN_TAB) {
				info->comparision = rid2find->id - info->findedRid->id;
			} else {
				if (vlCount(info->findedRid->dataList) <= 0) {
					perror("Nenhum dado a ser localizado!");
					goto freeFindNearest;
				}
				DataCursorType * data = (DataCursorType *) vlGet(info->findedRid->dataList, 0);
				// Comparação
				switch (data2find->column->type) {
					case NUMBER:
						info->comparision = (BigNumber*)data2find->content - (BigNumber*)data->content;
						break;
					case VARCHAR:
						info->comparision = memcmp(data2find->content, data->content, MIN(data2find->usedSize, data->usedSize));
						break;
					default:
						perror("Comparação não preparada para o tipo!");
						goto freeFindNearest;
				}
				// Se for o dato for maior que o a ser procurado e terminou a busca
				if (info->comparision < 0 && !data->usedSize) {
					if (left) {
						info->findedBlock = m_Handler->getStorage()->openBlock((*left));
						if (!info->blocksList)
							info->blocksList = vlNew(true);
						vlAdd(info->blocksList, info->findedBlock);
						continue;
					}
					info->offset = i;
					break;
				}
			}
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

	if (!block) {
		perror("Bloco inválido para atualização!");
		goto freeUpdateBlockBuffer;
	}

	
	if (block->ridsList)
		count = vlCount(block->ridsList);

	block->offset = block->buffer + sizeof(BlockHeaderType);
	if (!m_Handler->getStorage()->writeDataSize(&block->offset, count)) {
		perror("Erro ao escrever a quantidade de dados do bloco!");
		goto freeUpdateBlockBuffer;
	}

	if (count > 0) {
		for (int i = 0; i < count; i++) {
			BlockOffset * left = (BlockOffset *) vlGet(block->offsetsList, i);
			RidCursorType * rid = (RidCursorType *) vlGet(block->ridsList, i);
			if (!m_Handler->getStorage()->writeNumber((*left), &block->offset)) {
				perror("Erro ao escrever o ponteiro para o nodo da esqueda!");
				goto freeUpdateBlockBuffer;
			}
			if (block->header->type == C_BLOCK_TYPE_MAIN_TAB) {
				// Escreve o número do RID
				if (!m_Handler->getStorage()->writeNumber(rid->id, &block->offset)) {
					perror("Erro ao escrever o rid no buffer da tabela!");
					goto freeUpdateBlockBuffer;
				}
				// Escreve os deslocamentos
				for (int c = 0; c < vlCount(rid->dataList); c++) {
					DataCursorType * data = (DataCursorType *) vlGet(rid->dataList, c);
					// Escreve o endereço do bloco
					if (!m_Handler->getStorage()->writeNumber(data->blockId, &block->offset)) {
						perror("Erro ao escrever o endereço do bloco no buffer da tabela!");
						goto freeUpdateBlockBuffer;
					}
					// Escreve o deslocamento no bloco
					if (!m_Handler->getStorage()->writeDataSize(&block->offset, data->blockOffset)) {
						perror("Erro ao escrever o deslocamento no buffer da tabela!");
						goto freeUpdateBlockBuffer;
					}
				}
			} else {
				DataCursorType * data = (DataCursorType *) vlGet(rid->dataList, 0);
				// Escreve o dado chave da coluna
				if (!writeDataCursor(&block->offset, data)) {
					perror("Erro ao escrever o dado chave no buffer da coluna!");
					goto freeUpdateBlockBuffer;
				}
				// Escreve o número do RID
				if (!m_Handler->getStorage()->writeNumber(rid->id, &block->offset)) {
					perror("Erro ao escrever o rid no buffer da coluna!");
					goto freeUpdateBlockBuffer;
				}
			}
		}
		BlockOffset * right = (BlockOffset *) vlGet(block->offsetsList, count - 1);
		if (!m_Handler->getStorage()->writeNumber((*right), &block->offset)) {
			perror("Erro escrever o ponteiro para o nodo da direita!");
			goto freeUpdateBlockBuffer;
		}
	}

	ret = true;
	
freeUpdateBlockBuffer:
	DBUG_RETURN(ret);
}

int vogal_manipulation::writeDataCursor(GenericPointer* dest, DataCursorType * data) {
	DBUG_ENTER("vogal_manipulation::writeDataCursor");
	DBUG_RETURN(m_Handler->getStorage()->writeAnyData(dest, (GenericPointer)data, data->column->type) );
}


// TODO: Tornar o índice balanceado
int vogal_manipulation::writeData(CursorType * cursor, RidCursorType * rid, DataCursorType * data) {
	DBUG_ENTER("vogal_manipulation::writeData");

	// Declaração das variáveis
	BigNumber neededSpace;
	BigNumber freeSpace;
	BlockCursorType *block;
	SearchInfoType *info = NULL;
	RidCursorType *newRid = NULL;

	// Inicialização das variáveis
	if (data) {
		block = data->column->block;
		neededSpace =
			m_Handler->getStorage()->bytesNeeded(data->usedSize) + // Dado chave
			m_Handler->getStorage()->bytesNeeded(sizeof(rid->id)) + // RID
			m_Handler->getStorage()->bytesNeeded(sizeof(BlockOffset)); // Ponteiro bloco esquerda
	} else {
		block = cursor->table->block;
		neededSpace =
			m_Handler->getStorage()->bytesNeeded(data->usedSize); // RID
		// + Deslocamentos!!!
	}

	info = findNearest(cursor, rid, data, block);

	//for (int i = vlCount(info->blockList) -1; i >= 0; i--) {
		//block = vlGet(info->blockList, 0);
		block = info->rootBlock;

		freeSpace = block->offset - block->buffer; // Posição atual no buffer - posição do início do buffer
		if (freeSpace < 0 || freeSpace > C_BLOCK_SIZE - sizeof(BlockHeaderType)) {
			perror("Erro ao calular espaço disponível no bloco. Fora da faixa permitida");
			goto freeWriteData;
		}

		if (neededSpace > freeSpace) {
			// Fudeu!
		} else {
			// Acha a posição do rid encontrado
			newRid = new RidCursorType();
			newRid->id = rid->id;
			newRid->dataList = vlNew(true);
			vlAdd(newRid->dataList, data);

			// ################################################
			// #### ATUALIZAR OFFSET DAS COLUNAS À DIREITA ####
			// ################################################
			
			vlInsert(info->findedBlock->ridsList, newRid, info->offset);

			vlInsert(info->findedBlock->offsetsList, new BlockOffset(), info->offset);
			if (vlCount(info->findedBlock->offsetsList) == 1)
				vlAdd(info->findedBlock->offsetsList, new BlockOffset());

			if (!updateBlockBuffer(info->findedBlock)) {
				perror("Erro ao atualizar o buffer do bloco!");
				goto freeWriteData;
			}

			if (!m_Handler->getStorage()->writeBlock(info->findedBlock)) {
				perror("Erro ao gravar bloco de dados!");
				goto freeWriteData;
			}
			
		}
	//}
		
	

	/*BlockCursorType * findedBlock;
	GenericPointer findedOffset;
	BigNumber neededSpace;
	BigNumber usedSpace;
	

	// TODO: Otimizar para algumas informações estarem disponíveis no bloco, por exemplo o espaço disponível
	// Contabiliza informações do índice

	// Espaço necessário
	if (block->type != C_BLOCK_TYPE_MAIN_COL) {
		perror("Tipo de bloco incorreto para gravação dos dados das colunas");
		goto freeWriteData;
	}


	// Espaço disponível
	usedSpace = sizeof(BlockHeaderType);
	while (true) {
	}*/

freeWriteData:
	if (newRid)
		newRid->~RidCursorType();
	if (info)
		info->~SearchInfoType();
		
	DBUG_RETURN(false);	
}

int vogal_manipulation::writeRid(CursorType * cursor, RidCursorType * rid){
	DBUG_ENTER("vogal_manipulation::writeRid");

	int ret = false;
	BigNumber fieldCount = 0;
	DataCursorType * data;

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
	for (int i = 0; i < vlCount(rid->dataList); i++) {
		data = (DataCursorType *) vlGet(cursor->table->colsList, i);
		if (!writeData(cursor, rid, data)) {
			perror("Erro ao gravar dado do rid!");
			goto freeWriteRid;
		}
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
			int comparision;
			
			if (!stGet(cursor->table->colsList, stNodeName(iNode), &value))
				goto freeFetch;    
			column = (ColumnCursorType *) value;
			if (!stGet(cursor->fetch->dataList, (char*) stNodeName(iNode), &value))
				goto freeFetch;    
			data = (DataCursorType *) value;

			// Comparação
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
