
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

ColumnCursorType * vogal_manipulation::findColumn(ObjectCursorType* table, char * colName) {
	DBUG_ENTER("vogal_manipulation::findColumn");
	for (int i = 0; i < vlCount(table->colsList); i++) {
		ColumnCursorType * col = (ColumnCursorType *) vlGet(table->colsList, i);
		if (!strcmp(col->name, colName))
			DBUG_RETURN(col);
	}
	DBUG_RETURN(NULL);
}

ValueListRoot * vogal_manipulation::createObjectData(char* name, char* type, BlockOffset location) {
	DBUG_ENTER("vogal_manipulation::createObjectData");
	
	DataCursorType *data = NULL;
	ValueListRoot *dataList = vlNew(true);

	data = new DataCursorType();
	data->column = findColumn(m_Handler->getCache()->getObjects(), C_NAME_KEY);
	if (!data->column)
		goto freeCreateObjectData;
	data->allocSize = strlen(name);
	data->content = (GenericPointer) name;
	if (!vlAdd(dataList, data))
		goto freeCreateObjectData;

	data = new DataCursorType();
	data->column = findColumn(m_Handler->getCache()->getObjects(), C_TYPE_KEY);
	if (!data->column)
		goto freeCreateObjectData;
	data->content = (GenericPointer) type;
	data->allocSize = strlen(type);
	if (!vlAdd(dataList, data))
		goto freeCreateObjectData;

	data = new DataCursorType();
	data->column = findColumn(m_Handler->getCache()->getObjects(), C_LOCATION_KEY);
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

ValueListRoot * vogal_manipulation::createColumnData(BigNumber tableRid, char* name, char* type, BlockOffset location) {
	DBUG_ENTER("vogal_manipulation::createColumnData");
	
	DataCursorType * data = NULL;
	ValueListRoot * dataList = vlNew(true);

	data = new DataCursorType();
	data->column = findColumn(m_Handler->getCache()->getColumns(), C_TABLE_RID_KEY);
	if (!data->column)
		goto freeCreateColumnData;
	data->content = (GenericPointer) &tableRid;
	data->allocSize = sizeof(tableRid);
	if (!vlAdd(dataList, data))
		goto freeCreateColumnData;

	data = new DataCursorType();
	data->column = findColumn(m_Handler->getCache()->getColumns(), C_NAME_KEY);
	if (!data->column)
		goto freeCreateColumnData;
	data->allocSize = strlen(name);
	data->content = (GenericPointer) name;
	if (!vlAdd(dataList, data))
		goto freeCreateColumnData;

	data = new DataCursorType();
	data->column = findColumn(m_Handler->getCache()->getColumns(), C_TYPE_KEY);
	if (!data->column)
		goto freeCreateColumnData;
	data->content = (GenericPointer) type;
	data->allocSize = strlen(type);
	if (!vlAdd(dataList, data))
		goto freeCreateColumnData;

	data = new DataCursorType();
	data->column = findColumn(m_Handler->getCache()->getColumns(), C_LOCATION_KEY);
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

	RidCursorType * maxRid = new RidCursorType();
	maxRid->id = -1; // Não aceita negativo, portanto se tornará o maior valor possível

	// Busca o RID mais próximo
	RidCursorType * nearestRid = findNearest(cursor, maxRid, cursor->table->block);

	DBUG_RETURN((nearestRid?nearestRid->id:0) + 1);
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
			goto freeparseRecord;
		}

		// Fim
		if (!rid->id) {
			goto freeparseRecord;
		}
		
		// Lê deslocamentos
		for (int i = 0; i < vlCount(cursor->table->colsList); i++) {
			if (loadData) {
				data = new DataCursorType();
				data->column = (ColumnCursorType*) vlGet(cursor->table->colsList, i);
				data->location = new DataLocationType();
				// o endereço do bloco (tamanho + localização)
				if (!m_Handler->getStorage()->readSizedNumber(offset, &data->location->id)) {
					perror("Erro ao ler dados da árvore de registros");
					goto freeparseRecord;
				}
				// o offset no bloco (tamanho é o offset)
				if (!m_Handler->getStorage()->readDataSize(offset, &data->location->offset)) {
					perror("Erro ao ler dados de tamanho da árvore de registros");
					goto freeparseRecord;
				}
				// Adiciona informação à lista
				vlAdd(rid->dataList, data);
				data = NULL;

				continue;
			}
			// Pula
			if (!m_Handler->getStorage()->skipData(offset)) {
				perror("Erro ao pular dados da árvore de registros");
				goto freeparseRecord;
			}
			if (!m_Handler->getStorage()->skipDataSize(offset)) {
				perror("Erro ao pular dados de tamanho da árvore de registros");
				goto freeparseRecord;
			}
		}
	} else {
		if (!column) {
			perror("A coluna é obrigatória na leitura de blocos de colunas!");
			goto freeparseRecord;
		}

		DataCursorType * data = new DataCursorType();
		data->column = column;
		
		// Obtém tamanho do dado
		if (!m_Handler->getStorage()->readDataSize(offset, &data->usedSize)) {
			perror("Erro ao ler o tamanho do dado!");
			goto freeparseRecord;
		}

		// Fim
		if (!data->usedSize)
			goto freeparseRecord;

		// Carga e alocação dos dados
		switch (data->column->type) {
			case NUMBER:
				if (!data->content) {
					data->allocSize = sizeof(BigNumber); // Sempre é númeral máximo tratado, no caso LONG LONG INT (64BITS)
					data->content = (GenericPointer) malloc(data->allocSize);
				}
				if (data->usedSize > data->allocSize) {
					perror("Informação inconsistente na base pois não pode haver númeoros maiores que 64 bits (20 dígitos)");
					goto freeparseRecord;
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
				goto freeparseRecord;
		}
		// Zera para evitar informação errônea
		memset(data->content, 0x00, data->allocSize);
		// Lê o dado
		if (!m_Handler->getStorage()->readData(offset, data->content, data->usedSize, data->allocSize, data->column->type)) {
			perror("Erro ao ler dado do bloco!");
			goto freeparseRecord;
		}

		// Lê o número do RID
		if (!m_Handler->getStorage()->readSizedNumber(offset, &rid->id)) {
			perror("Erro ao ler o rid do dado!");
			goto freeparseRecord;
		}

		// Adiciona informação à lista
		vlAdd(rid->dataList, data);
		data = NULL;
	}

	DBUG_RETURN(rid);

freeparseRecord:
	if (data)
		data->~DataCursorType();
	if (rid)
		rid->~RidCursorType();
		
	DBUG_RETURN(NULL);

}

ValueListRoot * vogal_manipulation::parseBlock(CursorType * cursor, ColumnCursorType * column, BlockCursorType * block, bool loadData) {
	DBUG_ENTER("vogal_manipulation::parseBlock");
	
	// Declara variáveis
	ValueListRoot * resultList = NULL;
	GenericPointer offset;

	// Inicializa
	resultList = vlNew(true);
	offset = block->buffer + sizeof(BlockHeaderType);

	while (true) {
		BlockOffset *left = NULL;
		RidCursorType *rid = NULL;

		// Auxiliares
		left = new BlockOffset();
		// Lê primeiro nó a esquerda
		if (!m_Handler->getStorage()->readSizedNumber(&offset, left)) {
			perror("Erro ao ler nó a esquerda da árvore de registros");
			free(left);
			goto freeparseBlock;
		}
		// Adiciona informação à lista
		vlAdd(resultList, left);
		left = NULL;

		rid = new RidCursorType();
		if (!parseRecord(cursor, column, block, &offset, loadData)) {
			rid->~RidCursorType();
			rid = NULL;
			break;
		}

		// Adiciona informação à lista
		vlAdd(resultList, rid);
	}
		
	DBUG_RETURN(resultList);

freeparseBlock:
	vlFree(resultList);
	
	DBUG_RETURN(NULL);

}

RidCursorType* vogal_manipulation::findNearest(CursorType * cursor, RidCursorType * rid2find, BlockCursorType * block) {
	DBUG_ENTER("vogal_manipulation::findNearest");

	// Inicializa variáveis
	RidCursorType * findedRid = NULL;
	DataCursorType * data2find = NULL;
	ValueListRoot * list = NULL;
	BlockOffset * left;

	if (block->header->type == C_BLOCK_TYPE_MAIN_COL) {
		if (vlCount(rid2find->dataList) <= 0) {
			perror("Nenhum dado para localização!");
			goto freeFindNearest;
		}
		data2find = (DataCursorType *) vlGet(rid2find->dataList, 0);
	}
	
	while (true) {
		if (!block) {
			perror("Erro ao ler bloco!");
			goto freeFindNearest;
		}
		if (list)
			vlFree(list);
		list = parseBlock(cursor, data2find?data2find->column:NULL, block, false);
		if (!list) {
			perror("Erro ao obter os registros sem complementos do block!");
			goto freeFindNearest;
		}

		for (int i = 0; i < vlCount(list) - 1; i+=2) {
			left = (BlockOffset *) vlGet(list, i++);
			findedRid = (RidCursorType *) vlGet(list, i++);

			int comparision;
			if (block->header->type == C_BLOCK_TYPE_MAIN_TAB) {
				comparision = rid2find->id - findedRid->id;
			} else {
				if (vlCount(findedRid->dataList) <= 0) {
					perror("Nenhum dado a ser localizado!");
					goto freeFindNearest;
				}
				DataCursorType * data = (DataCursorType *) vlGet(findedRid->dataList, 0);
				// Comparação
				switch (data2find->column->type) {
					case NUMBER:
						comparision = (BigNumber*)data2find->content - (BigNumber*)data->content;
						break;
					case VARCHAR:
						comparision = memcmp(data2find->content, data->content, MIN(data2find->usedSize, data->usedSize));
						break;
					default:
						perror("Comparação não preparada para o tipo!");
						goto freeFindNearest;
				}
				// Se for o dato for maior que o a ser procurado e terminou a busca
				if (comparision < 0 && !data->usedSize) {
					if (left) {
						if (block != cursor->table->block)
							block->~BlockCursorType();
						block = m_Handler->getStorage()->openBlock((*left));
					} else {
						break;
					}
				}
			}
		}
	}
	DBUG_RETURN(findedRid);

freeFindNearest:
	if (list)
		vlFree(list);
	if (findedRid)
		findedRid->~RidCursorType();
	if (block && block != cursor->table->block)
		block->~BlockCursorType();
	DBUG_RETURN(NULL);
	
}

// TODO: Tornar o índice balanceado
int vogal_manipulation::writeData(CursorType * cursor, RidCursorType * rid, DataCursorType * data) {
	DBUG_ENTER("vogal_manipulation::writeData");

	/*BlockCursorType * findedBlock;
	GenericPointer findedOffset;
	BigNumber neededSpace;
	BigNumber usedSpace;
	
	RidCursorType *findedRid = findNearestData(cursor, data, (*findedBlock), &findedOffset);

	// TODO: Otimizar para algumas informações estarem disponíveis no bloco, por exemplo o espaço disponível
	// Contabiliza informações do índice

	// Espaço necessário
	if (block->type != C_BLOCK_TYPE_MAIN_COL) {
		perror("Tipo de bloco incorreto para gravação dos dados das colunas");
		goto freeWriteData;
	}

	neededSpace =
		m_Handler->getStorage()->bytesNeeded(data->usedSize) + // Dado chave
		m_Handler->getStorage()->bytesNeeded(sizeof(rid->id)) + // RID
		m_Handler->getStorage()->bytesNeeded(sizeof(BlockOffset)); // Ponteiro bloco direita

	// Espaço disponível
	usedSpace = sizeof(BlockHeaderType);
	while (true) {
	}

freeWriteData:*/
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
