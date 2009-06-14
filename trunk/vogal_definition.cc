
#include "vogal_definition.h"
#include "FilterCursorType.h"

vogal_definition::vogal_definition(vogal_handler *handler){
	DBUG_ENTER("vogal_definition::vogal_definition");
	
	m_Handler = handler;
	
	DBUG_LEAVE;
}

vogal_definition::~vogal_definition(){
	DBUG_ENTER("vogal_definition::~vogal_definition");
	DBUG_LEAVE;
}

int vogal_definition::createDataDictionary(){
	DBUG_ENTER("vogal_definition::createDataDictionary");

	// Declaração das variáveis

	// Auxiliares (Não precisa liberar)
	int ret = false;
	GenericPointer p;
	ValueListRoot *dataList = NULL;
	
	// Controle (Devem ser liberadas)
	CursorType *objsCursor = NULL;
	CursorType *colsCursor = NULL;
	PairListRoot *objsColsList = NULL;
	PairListRoot *colsColsList = NULL;
	BigNumber objsRid;
	BigNumber colsRid;
	BigNumber ridAux;
	ColumnCursorType *colAux;
	
	// Definição
	char *colsNames[] = C_COLUMNS_NAMES;
	char *colsTypes[] = C_COLUMNS_TYPES;

	// Preenche lista de colunas da OBJS
	objsColsList = plNew(false, false);
	for (int i = 0; i < C_OBJECTS_COLS_COUNT; i++) {
		plAdd(objsColsList, colsNames[i], colsTypes[i]);
	}

	// Preenche lista de colunas da COLS
	colsColsList = plNew(false, false);
	for (int i = C_OBJECTS_COLS_COUNT; i < C_OBJECTS_COLS_COUNT + C_COLUMNS_COLS_COUNT; i++) {
		plAdd(colsColsList, colsNames[i], colsTypes[i]);
	}
	
	// ### OBJS ###
	
	// Cria objeto
	objsCursor = createTableStructure(C_OBJECTS, objsColsList);
	if (!objsCursor) {
		perror("Erro ao criar a estrutura da tabela OBJS do dicionário de dados!");
		goto freeCreateDataDictionary;
	}

	// ### COLS ###
	
	// Cria objeto
	colsCursor = createTableStructure(C_COLUMNS, colsColsList);
	if (!colsCursor) {
		perror("Erro ao criar a estrutura da tabela COLS do dicionário de dados!");
		goto freeCreateDataDictionary; 
	}

	// ### OBJS:DATA ###

	dataList = m_Handler->getManipulation()->createObjectData(objsCursor, objsCursor->table->name, C_OBJECT_TYPE_TABLE, objsCursor->table->block->id);
	if (!dataList) {
		perror("Erro ao criar estrutura de dados da tabela OBJS");
		goto freeCreateDataDictionary;
	}
	objsRid = m_Handler->getManipulation()->insertData(objsCursor, dataList);
	if (!objsRid) {
		perror("Erro ao inserir estrutura de dados da tabela OBJS");
		vlFree(&dataList); // Precisa liberar o datalist pois em caso de sucesso ele é filho do RID que é liberado posteriormente
		goto freeCreateDataDictionary;
	}

	// Não precisa liberar o dataList pois ele vira 
	dataList = m_Handler->getManipulation()->createObjectData(objsCursor, colsCursor->table->name, C_OBJECT_TYPE_TABLE, colsCursor->table->block->id);
	if (!dataList) {
		perror("Erro ao criar estrutura de dados da tabela COLS");
		vlFree(&dataList); // Precisa liberar o datalist pois em caso de sucesso ele é filho do RID que é liberado posteriormente
		goto freeCreateDataDictionary;
	}
	colsRid = m_Handler->getManipulation()->insertData(objsCursor, dataList);
	if (!colsRid) {
		perror("Erro ao inserir estrutura de dados da tabela COLS");
		goto freeCreateDataDictionary;
	}

	// ### COLS:DATA ###

	// Varre as colunas da tabela OBJS para inserção
	for (int i = 0; i < vlCount(objsCursor->table->colsList); i++) {
		colAux = (ColumnCursorType *) vlGet(objsCursor->table->colsList, i);
		dataList = m_Handler->getManipulation()->createColumnData(colsCursor, objsRid, colAux->name, vogal_utils::type2str(colAux->type), colAux->block->id);
		if (!dataList) {
			perror("Erro ao criar estrutura de dados da tabela COLS para colunas da OBJS");
			goto freeCreateDataDictionary;
		}
		ridAux = m_Handler->getManipulation()->insertData(colsCursor, dataList);
		if (!ridAux) {
			perror("Erro ao inserir estrutura de dados na tabela COLS para colunas OBJS");
			vlFree(&dataList);
			goto freeCreateDataDictionary;
		}
	}

	// Varre as colunas da tabela COLS para inserção
	for (int i = 0; i < vlCount(colsCursor->table->colsList); i++) {
		colAux = (ColumnCursorType *) vlGet(colsCursor->table->colsList, i);
		dataList = m_Handler->getManipulation()->createColumnData(colsCursor, colsRid, colAux->name, vogal_utils::type2str(colAux->type), colAux->block->id);
		if (!dataList) {
			perror("Erro ao criar estrutura de dados da tabela COLS para colunas da OBJS");
			goto freeCreateDataDictionary;
		}
		ridAux = m_Handler->getManipulation()->insertData(colsCursor, dataList);
		if (!ridAux) {
			perror("Erro ao inserir estrutura de dados na tabela COLS para colunas OBJS");
			vlFree(&dataList);
			goto freeCreateDataDictionary;
		}
	}

	// Sucesso
	ret = true;
	
freeCreateDataDictionary:
	if (objsCursor)
		objsCursor->~CursorType();
	if (colsCursor)
		colsCursor->~CursorType();
	if (objsColsList)
		plFree(&objsColsList);
	if (colsColsList)
		plFree(&colsColsList);

	DBUG_RETURN(ret);
}

// Preenche a estrutura ObjectCursorType (object) com os blocos onde forem criadas as estruturas
CursorType * vogal_definition::createTableStructure(char *name, PairListRoot *columns) {
	DBUG_ENTER("vogal_definition::createTableStructure");

	// Inicializa as variáveis

	// Auxiliares (não precisa finalizar)
	int ret = false;
	GenericPointer p;
	int dict = false;

	// Controle (devem ser finalizadas)
	CursorType *objsCursor = NULL;
	CursorType *colsCursor = NULL;
	ValueListRoot *dataList = NULL;
	BigNumber objRid = NULL;
	
	CursorType * cursor = new CursorType();

	cursor->table = new ObjectCursorType();
	cursor->table->name = name;
	cursor->table->block = NULL;
	cursor->table->colsList = NULL;

	cursor->table->block = new BlockCursorType();
	cursor->table->block->id = 0;
	cursor->table->block->buffer = vogal_cache::blankBuffer();

	// Obtém o bloco da tabela
	if (!strcmp(name, C_OBJECTS)) {
		cursor->table->block->id = C_OBJECTS_BLOCK;
		dict = true;
	} else if (!strcmp(name, C_COLUMNS)) {
		cursor->table->block->id = C_COLUMNS_BLOCK;
		dict = true;
	} else if (!m_Handler->getCache()->lockFreeBlock(&cursor->table->block->id)) {
		perror("Não existem blocos disponíveis para criação da tabela!");
		goto freeCreateTableStructure;
	}

	// Monta estrutura da tabela
	if (!createStructure(C_BLOCK_TYPE_MAIN_TAB, cursor->table->block->buffer)) {
		perror("Erro ao criar estrutura da tabela!");
		goto freeCreateTableStructure;
	}

	// Atualiza estrutura
	if (!m_Handler->getManipulation()->updateBlockBuffer(cursor->table->block)) {
		perror("Erro ao atualizar estrutura da tabla!");
		goto freeCreateTableStructure;
	}

	// Monta estrutura das colunas
	cursor->table->colsList = vlNew(true);
	if (!cursor->table->colsList) {
		perror("Erro ao criar coleção de colunas da tabela!");
		goto freeCreateTableStructure;
	}

	// Obtém os blocos das colunas
	for (int i = 0; i < plCount(columns); i++) {
		ColumnCursorType *col = new ColumnCursorType(i);
		col->name = (char *) plGetName(columns, i);
		col->type = vogal_utils::str2type((char *) plGetValue(columns, i));
		col->block = new BlockCursorType();
		col->block->buffer = vogal_cache::blankBuffer();
		if (!m_Handler->getCache()->lockFreeBlock(&col->block->id)) {
			perror("Não existem blocos disponíveis para criação das colunas da tabela!");
			col->~ColumnCursorType();
			goto freeCreateTableStructure;
		}
		// Monta estrutura da coluna
		if (!createStructure(C_BLOCK_TYPE_MAIN_COL, col->block->buffer)) {
			perror("Erro ao criar estrutura da coluna!");
			goto freeCreateTableStructure;
		}
		if (!m_Handler->getManipulation()->updateBlockBuffer(col->block)) {
			perror("Erro ao atualizar estrutura da coluna!");
			goto freeCreateTableStructure;
		}
		// Adiciona à lista de colunas
		vlAdd(cursor->table->colsList, col);
	}

	// Se não for do dicionário de dados, insere os registros
	if (!dict) {
		// Abre cursores dos objetos e coluns
		objsCursor = m_Handler->getManipulation()->openCursor(m_Handler->getCache()->openObjects());
		if (!objsCursor) {
			perror("Erro ao abrir cursor da tabela OBJS!");
			goto freeCreateTableStructure;
		}
		colsCursor = m_Handler->getManipulation()->openCursor(m_Handler->getCache()->openColumns());
		if (!colsCursor) {
			perror("Erro ao abrir cursor da tabela COLS!");
			goto freeCreateTableStructure;
		}

		// Prepara dados para inserção na tabela OBJS
		dataList = m_Handler->getManipulation()->createObjectData(objsCursor, cursor->table->name, C_OBJECT_TYPE_TABLE, cursor->table->block->id);
		if (!dataList) 
			goto freeCreateTableStructure;
		objRid = m_Handler->getManipulation()->insertData(objsCursor, dataList);
		if (!objRid)
			goto freeCreateTableStructure;
			
		// Insere colunas na COLS
		for (int i = 0; i < vlCount(cursor->table->colsList); i++) {
			BigNumber colRid;
			ColumnCursorType *col = (ColumnCursorType *) vlGet(cursor->table->colsList, i);
			dataList = m_Handler->getManipulation()->createColumnData(colsCursor, objRid, col->name, vogal_utils::type2str(col->type), col->block->id);
			if (!dataList) 
				goto freeCreateTableStructure;
			colRid = m_Handler->getManipulation()->insertData(colsCursor, dataList);
			if (!colRid)
				goto freeCreateTableStructure;
		}
	}

	// Pelo menos a estrutura dos blocos devem caber no bloco

	// Grava tabela
	if (!m_Handler->getStorage()->writeBlock(cursor->table->block)) {
		perror("Erro ao gravar fisicamente o objeto na base!");
		goto freeCreateTableStructure;
	}
	// Grava colunas
	for (int i = 0; i < vlCount(cursor->table->colsList); i++) {
		ColumnCursorType * col = (ColumnCursorType *) vlGet(cursor->table->colsList, i);
		if (!m_Handler->getStorage()->writeBlock(col->block)) {
			perror("Erro ao gravar fisicamente a coluna do objeto na base!");
			goto freeCreateTableStructure;
		}
	}

	// Sucesso
	ret = true;

freeCreateTableStructure:
	if (colsCursor)
		colsCursor->~CursorType();
	if (objsCursor)
		objsCursor->~CursorType();

	if (!ret && cursor) {
		if (cursor->table) {
			// Desbloqueia blocos travados para gravação da tabela
			if ((!dict) && (cursor->table) && (cursor->table->block) && (cursor->table->block->id > 0))
				m_Handler->getCache()->unlockBlock(cursor->table->block->id);
			if (cursor->table->colsList) {
				for (int i = 0; i < vlCount(cursor->table->colsList); i++) {
					ColumnCursorType * col = (ColumnCursorType *) vlGet(cursor->table->colsList, i);
					m_Handler->getCache()->unlockBlock(col->block->id);
				}
			}
		}
		cursor->~CursorType();
		cursor = NULL;
	}

	DBUG_RETURN(cursor);
}

int vogal_definition::createStructure(int type, GenericPointer buf){
	DBUG_ENTER("vogal_definition::createStructure");
	
	BlockHeaderType * index = (BlockHeaderType *) buf;
	
	index->valid = true;
	index->type  = type;

	DBUG_RETURN(true);
}

ColumnCursorType * vogal_definition::findColumn(ObjectCursorType* table, char * colName) {
	DBUG_ENTER("vogal_definition::findColumn");
	if (!table) {
		perror("Impossível obter uma coluna de nenhuma tabela!");
		DBUG_RETURN(NULL);
	}
	for (int i = 0; i < vlCount(table->colsList); i++) {
		ColumnCursorType * col = (ColumnCursorType *) vlGet(table->colsList, i);
		if (!strcmp(col->name, colName))
			DBUG_RETURN(col);
	}
	DBUG_RETURN(NULL);
}

ObjectCursorType * vogal_definition::openTable(char * tableName){
// TODO (Pendência): Todo este processo de busca das tabelas e colunas deve ser bufferizado para melhorar o desempenho
//   e evitar acesso a disco desnecessariamente!
	DBUG_ENTER("vogal_definition::openTable");

	// Auxiliares
	int i;
	int ret = false;
	
	CursorType * colsCursor = NULL;
	FilterCursorType * objsFilter = NULL;
	FilterCursorType * colsFilter = NULL;

	// Começa a inicializar a tabela
	ObjectCursorType * table = new ObjectCursorType();
	table->name = tableName;
	table->colsList = NULL;

	// Se está tentando abrir a tabela de colunas, a inicialização deve eser anual pois não há donde obter sua definição
	if (!strcmp(tableName,C_COLUMNS)) {
		char * cols[] = C_COLUMNS_NAMES;
		char * types[] = C_COLUMNS_TYPES;
		table->block = m_Handler->getStorage()->openBlock(C_COLUMNS_BLOCK);
		if (!table->block) {
			perror("Erro ao abrir bloco de dados da tabela COLS!");
			goto freeOpenTable;
		}
		table->colsList = vlNew(true);
		if (!table->colsList) {
			perror("Erro ao criar lista de colunas!");
			goto freeOpenTable;
		}
		for (i = 0; i < C_COLUMNS_COLS_COUNT; i++) {
			ColumnCursorType * column = new ColumnCursorType(i);
			column->name = cols[i+C_OBJECTS_COLS_COUNT];
			column->type = vogal_utils::str2type(types[i+C_OBJECTS_COLS_COUNT]);
			if (!vlAdd(table->colsList, column))
				goto freeOpenTable;
		}
		ret = true;
		goto freeOpenTable;
	}
	
	// Se está tentando abrir a tabela de objetos, a inicialização deve eser manual pois não há donde obter sua definição
	if (!strcmp(tableName,C_OBJECTS)) {
		char * cols[] = C_COLUMNS_NAMES;
		char * types[] = C_COLUMNS_TYPES;
		table->block = m_Handler->getStorage()->openBlock(C_OBJECTS_BLOCK);
		if (!table->block) {
			perror("Erro ao abrir bloco de dados da tabela OBJS");
			goto freeOpenTable;
		}
		table->colsList = vlNew(true);
		if (!table->colsList)
			goto freeOpenTable;
		for (i = 0; i < C_OBJECTS_COLS_COUNT; i++) {
			ColumnCursorType * column = new ColumnCursorType(i);
			column->name = cols[i];
			column->type = vogal_utils::str2type(types[i]);
			if (!vlAdd(table->colsList, column))
				goto freeOpenTable;
		}
		ret = true;
		goto freeOpenTable;
	}

	// Cria objeto de filtro
	objsFilter = new FilterCursorType();
	objsFilter->cursor = m_Handler->getManipulation()->openCursor(m_Handler->getCache()->openObjects());

	// Senão, deve obter através das tabelas do dicionário de dados
	if (!objsFilter->cursor || !objsFilter->cursor->table) {
		perror("Impossível abrir cursor dos objetos!");
		goto freeOpenTable;
	}

	objsFilter->column = findColumn(objsFilter->cursor->table, C_NAME_KEY);
	if (!objsFilter->column) {
		perror("Coluna do metadados correspondente ao nome da tabela não pode ser encontrada!");
		goto freeOpenTable;
	}

	objsFilter->data = new DataCursorType();
	objsFilter->data->content = (GenericPointer) tableName;
	objsFilter->data->usedSize = strlen(tableName);
	objsFilter->data->allocSize = objsFilter->data->usedSize + 1;

	// Procura a tabela	
	if (!m_Handler->getManipulation()->fetch(objsFilter)) {
		perror("Tabela não encontrada!"); 
		goto freeOpenTable;
	}
	
	// Obtém as colunas da tablea
	colsFilter = new FilterCursorType();
	colsFilter->cursor = m_Handler->getManipulation()->openCursor(m_Handler->getCache()->openColumns());

	if (!colsFilter->cursor || !colsFilter->cursor->table) {
		perror("Impossível abrir cursor das colunas!");
		goto freeOpenTable;
	}

	colsFilter->column = findColumn(colsFilter->cursor->table, C_TABLE_RID_KEY);
	if (!colsFilter->column) {
		perror("Coluna do metadados correspondente ao RID da tabela não pode ser encontrada!");
		goto freeOpenTable;
	}
	
	colsFilter->data = new DataCursorType();
	colsFilter->data->content = (GenericPointer) objsFilter->fetch->id;
	colsFilter->data->usedSize = sizeof(BigNumber);
	colsFilter->data->allocSize = colsFilter->data->usedSize;

	// Varre as colunas da tabela	
	table->colsList = vlNew(true);
	if (!table->colsList)
		goto freeOpenTable;
	while (m_Handler->getManipulation()->fetch(colsFilter)) {
		int check = 0;
		ColumnCursorType * column = new ColumnCursorType(vlCount(table->colsList));
		for (int i = 0; i < vlCount(colsFilter->fetch->dataList); i++) {
			DataCursorType * data = (DataCursorType *) vlGet(colsFilter->fetch->dataList, i);
			if (!strcmp(data->column->name, C_NAME_KEY)) {
				column->name = (char*) data->content;
				check++;
			} else if (!strcmp(data->column->name, C_TYPE_KEY)) {
				column->type = vogal_utils::str2type((char*) data->content);
				check++;
			} else if (!strcmp(data->column->name, C_LOCATION_KEY)) {
				column->block = new BlockCursorType();
				column->block->id = (*(BigNumber *) data->content );
				check++;
			}
		}
		if (check != 3) {
			perror("Erro ao obter a estrutura (colunas) da tabela!");
			goto freeOpenTable;
		}
		if (!vlAdd(table->colsList, column)) 
			goto freeOpenTable;
	}

	ret = true;
	
freeOpenTable:
	if (objsFilter)
		objsFilter->~FilterCursorType();
	if (colsFilter)
		colsFilter->~FilterCursorType();
	if (!ret) {
		if (table)
			table->~ObjectCursorType();
		table = NULL;
	}
	
	DBUG_RETURN(table);
}

int vogal_definition::newTable(char* name, PairListRoot * columns){
	DBUG_ENTER("vogal_definition::newTable");

	// Verifica se a tabela existe
    ObjectCursorType * table = openTable(name); 
	if (table) {
		table->~ObjectCursorType();
		DBUG_RETURN(false);
	}

	CursorType * cursor = createTableStructure(name, columns);
	bool tableCreated = cursor;
	cursor->~CursorType();
	
	DBUG_RETURN(tableCreated); 
}

int vogal_definition::parseBlock(CursorType * cursor, ColumnCursorType * column, BlockCursorType * block) {
	DBUG_ENTER("vogal_manipulation::parseBlock");
	
	// Declara variáveis
	int ret = false;
	ValueListRoot *list = NULL;
	NodeType *node = NULL;
	BlockOffset *neighbor;
	BigNumber dataCount;
	GenericPointer p;

	if (!block) {
		perror("É obrigatório informar o bloco para efetuar o parse");
		goto freeParseBlock;
	}

	// Monta lista de rids e offsets do bloco
	if (block->nodesList)
		vlFree(&block->nodesList);
	block->nodesList = vlNew(true);
	if (block->offsetsList)
		vlFree(&block->offsetsList);
	block->offsetsList = vlNew(true);
	
	// Posição atual
	p = block->buffer + sizeof(BlockHeaderType);

	// Lê a quantidade de registros do bloco
	if (!m_Handler->getStorage()->readDataSize(&p, &dataCount)) {
		perror("Erro ao ler a quantidade de registros no block");
		goto freeParseBlock;
	}

	if (dataCount > 0) {
		for (int i = 0; i < dataCount; i++) {
			// Auxiliares
			neighbor = new BlockOffset();
			// Lê primeiro nó a esquerda
			if (!m_Handler->getStorage()->readSizedNumber(&p, neighbor)) {
				perror("Erro ao ler nó a esquerda da árvore de registros");
				free(neighbor);
				goto freeParseBlock;
			}
			// Adiciona informação à lista
			vlAdd(block->offsetsList, neighbor);

			node = m_Handler->getManipulation()->parseRecord(cursor, column, block, &p);
			if (!node) {
				perror("Erro ao efetuar o parse do registro!");
				goto freeParseBlock;
			}

			// Adiciona informação à lista
			vlAdd(block->nodesList, node);
			node = NULL;
		}

		// Se a quantidade for maior que zero
		// Depois do último nó sempre tem o nó a direita
		neighbor = new BlockOffset();
		if (!m_Handler->getStorage()->readSizedNumber(&p, neighbor)) {
			perror("Erro ao ler nó a direita da árvore de registros");
			free(neighbor);
			goto freeParseBlock;
		}
		// Adiciona informação à lista
		vlAdd(block->offsetsList, neighbor);
	}

	block->freeSpace = C_BLOCK_SIZE - (p - block->buffer); // O deslocamento é o espaço usado no bloco!

	ret = true;
		
freeParseBlock:
	if (node)
		node->~NodeType();

	DBUG_RETURN(ret);

}
