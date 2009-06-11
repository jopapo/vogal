
#include "vogal_definition.h"

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
	int i;
	ValueListRoot *dataList = NULL;
	
	// Controle (Devem ser liberadas)
	CursorType *objsCursor = NULL;
	CursorType *colsCursor = NULL;
	PairListRoot *objsColsList = NULL;
	PairListRoot *colsColsList = NULL;
	RidCursorType *objsRid = NULL;
	RidCursorType *colsRid = NULL;
	
	// Definição
	char *colsNames[] = C_COLUMNS_NAMES;
	char *colsTypes[] = C_COLUMNS_TYPES;

	// Preenche lista de colunas da OBJS
	objsColsList = plNew(false, false);
	for (i = 0; i < C_OBJECTS_COLS_COUNT; i++)
		plAdd(objsColsList, colsNames[i], colsTypes[i]);

	// Preenche lista de colunas da COLS
	colsColsList = plNew(false, false);
	for (i = C_OBJECTS_COLS_COUNT; i < C_OBJECTS_COLS_COUNT + C_COLUMNS_COLS_COUNT; i++)
		plAdd(colsColsList, colsNames[i], colsTypes[i]);
	
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

	dataList = m_Handler->getManipulation()->createObjectData(objsCursor->table->name, C_OBJECT_TYPE_TABLE, objsCursor->table->block->id);
	if (!dataList) {
		perror("Erro ao criar estrutura de dados da tabela OBJS");
		goto freeCreateDataDictionary;
	}
	objsRid = m_Handler->getManipulation()->insertData(objsCursor, dataList);
	if (!objsRid) {
		perror("Erro ao inserir estrutura de dados da tabela OBJS");
		goto freeCreateDataDictionary;
	}

	dataList = m_Handler->getManipulation()->createObjectData(colsCursor->table->name, C_OBJECT_TYPE_TABLE, colsCursor->table->block->id);
	if (!dataList) {
		perror("Erro ao criar estrutura de dados da tabela COLS");
		goto freeCreateDataDictionary;
	}
	vlFree(dataList);
	colsRid = m_Handler->getManipulation()->insertData(colsCursor, dataList);
	if (!colsRid) {
		perror("Erro ao inserir estrutura de dados da tabela COLS");
		goto freeCreateDataDictionary;
	}

	// ### COLS:DATA ###

	// Varre as colunas da tabela OBJS para inserção
	for (int i = 0; i < vlCount(objsCursor->table->colsList); i++) {
		RidCursorType *ridAux;
		ColumnCursorType *col = (ColumnCursorType *) vlGet(objsCursor->table->colsList, i);
		dataList = m_Handler->getManipulation()->createColumnData(objsRid->id, col->name, vogal_utils::type2str(col->type), col->block->id);
		if (!dataList) {
			perror("Erro ao criar estrutura de dados da tabela COLS para colunas da OBJS");
			goto freeCreateDataDictionary;
		}
		vlFree(dataList);
		ridAux = m_Handler->getManipulation()->insertData(colsCursor, dataList);
		if (!ridAux) {
			perror("Erro ao inserir estrutura de dados na tabela COLS para colunas OBJS");
			goto freeCreateDataDictionary;
		}
		ridAux->~RidCursorType();
	}

	// Varre as colunas da tabela COLS para inserção
	for (int i = 0; i < vlCount(colsCursor->table->colsList); i++) {
		RidCursorType *ridAux;
		ColumnCursorType *col = (ColumnCursorType *) vlGet(colsCursor->table->colsList, i);
		dataList = m_Handler->getManipulation()->createColumnData(colsRid->id, col->name, vogal_utils::type2str(col->type), col->block->id);
		if (!dataList) {
			perror("Erro ao criar estrutura de dados da tabela COLS para colunas da OBJS");
			goto freeCreateDataDictionary;
		}
		ridAux = m_Handler->getManipulation()->insertData(colsCursor, dataList);
		if (!ridAux) {
			perror("Erro ao inserir estrutura de dados na tabela COLS para colunas OBJS");
			goto freeCreateDataDictionary;
		}
		ridAux->~RidCursorType();
	}

	// Sucesso
	ret = true;
	
freeCreateDataDictionary:
	if (objsCursor)
		objsCursor->~CursorType();
	if (colsCursor)
		colsCursor->~CursorType();
	plFree(objsColsList);
	plFree(colsColsList);
	if (objsRid)
		objsRid->~RidCursorType();
	if (colsRid)
		colsRid->~RidCursorType();

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
	RidCursorType *objRid = NULL;
	
	CursorType * cursor = new CursorType();

	cursor->table = new ObjectCursorType();
	cursor->table->block = NULL;
	cursor->table->colsList = NULL;

	cursor->table->block = new BlockCursorType();
	cursor->table->block->id = 0;
	cursor->table->block->buffer = NULL;

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
	cursor->table->block->buffer = vogal_cache::blankBuffer();

	// Monta estrutura da tabela
	if (!createStructure(C_BLOCK_TYPE_MAIN_TAB, cursor->table->block->buffer)) {
		perror("Erro ao criar estrutura da tabela!");
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
		ColumnCursorType *col = new ColumnCursorType();
		col->name = (char *) plGetName(columns, i);
		col->type = vogal_utils::str2type((char *) plGetValue(columns, i));
		col->block = new BlockCursorType();
		if (!m_Handler->getCache()->lockFreeBlock(&col->block->id)) {
			perror("Não existem blocos disponíveis para criação das colunas da tabela!");
			free(col->block);
			free(col);
			goto freeCreateTableStructure;
		}
		col->block->buffer = vogal_cache::blankBuffer();
		// Monta estrutura da coluna
		if (!createStructure(C_BLOCK_TYPE_MAIN_COL, col->block->buffer)) {
			perror("Erro ao criar estrutura da coluna!");
			goto freeCreateTableStructure;
		}
		vlAdd(cursor->table->colsList, col);
	}

	// Se não for do dicionário de dados, insere os registros
	if (!dict) {
		if ((!m_Handler->getCache()->getObjects()) || 
			(!m_Handler->getCache()->getColumns())) {
			perror("Dicionário de dados não inicializado corretamente!");
			goto freeCreateTableStructure;
		}

		// Abre cursores dos objetos e coluns
		objsCursor = m_Handler->getManipulation()->openCursor(m_Handler->getCache()->getObjects(), NULL);
		if (!objsCursor) {
			perror("Erro ao abrir cursor da tabela OBJS!");
			goto freeCreateTableStructure;
		}
		colsCursor = m_Handler->getManipulation()->openCursor(m_Handler->getCache()->getColumns(), NULL);
		if (!colsCursor) {
			perror("Erro ao abrir cursor da tabela COLS!");
			goto freeCreateTableStructure;
		}

		// Prepara dados para inserção na tabela OBJS
		dataList = m_Handler->getManipulation()->createObjectData(cursor->table->name, C_OBJECT_TYPE_TABLE, cursor->table->block->id);
		if (!dataList) 
			goto freeCreateTableStructure;
		objRid = m_Handler->getManipulation()->insertData(objsCursor, dataList);
		if (!objRid)
			goto freeCreateTableStructure;
			
		// Insere colunas na COLS
		for (int i = 0; i < vlCount(cursor->table->colsList); i++) {
			int inserted;
			RidCursorType *colRid;
			ColumnCursorType *col = (ColumnCursorType *) vlGet(cursor->table->colsList, i);
			dataList = m_Handler->getManipulation()->createColumnData(objRid->id, col->name, vogal_utils::type2str(col->type), col->block->id);
			if (!dataList) 
				goto freeCreateTableStructure;
			colRid = m_Handler->getManipulation()->insertData(colsCursor, dataList);
			inserted = colRid != NULL;
			colRid->~RidCursorType();
			if (!inserted)
				goto freeCreateTableStructure;
		}
	}

	// Pelo menos a estrutura dos blocos devem caber no bloco

	// Grava tabela
	if (!m_Handler->getStorage()->writeBlock(cursor->table->block))
		goto freeCreateTableStructure;
	// Grava colunas
	for (int i = 0; i < vlCount(cursor->table->colsList); i++) {
		ColumnCursorType * col = (ColumnCursorType *) vlGet(cursor->table->colsList, i);
		if (!m_Handler->getStorage()->writeBlock(col->block))
			goto freeCreateTableStructure;
	}

	// Sucesso
	ret = true;

freeCreateTableStructure:
	if (objRid)
		objRid->~RidCursorType();
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

ObjectCursorType * vogal_definition::openTable(char * tableName){
// TODO (Pendência): Todo este processo de busca das tabelas e colunas deve ser bufferizado para melhorar o desempenho
//   e evitar acesso a disco desnecessariamente!
	DBUG_ENTER("vogal_definition::openTable");

	// Auxiliares
	int i;
	int ret = false;
	
	CursorType * objsCursor = NULL;
	CursorType * colsCursor = NULL;

	// Começa a inicializar a tabela
	ObjectCursorType * table = new ObjectCursorType();
	table->name = tableName;
	table->colsList = NULL;

/*
	// Se está tentando abrir a tabela de colunas, a inicialização deve eser anual pois não há donde obter sua definição
	if (!strcmp(tableName,C_COLUMNS)) {
		char * cols[] = C_COLUMNS_NAMES;
		char * types[] = C_COLUMNS_TYPES;
		cursor->block = m_Handler->getStorage->openBlock(C_COLUMNS_BLOCK);
		if (!cursor->block) {
			perror("Erro ao abrir bloco de dados da tabela COLS!");
			goto freeOpenTable;
		}
		cursor->colsList = stNew(FALSE, TRUE);
		if (!cursor->colsList) {
			perror("Erro ao criar lista de colunas!");
			goto freeOpenTable;
		}
		for (i = 0; i < C_COLUMNS_COLS_COUNT; i++) {
			ColumnCursorType * column = new ColumnCursorType();
			column->name = cols[i+C_OBJECTS_COLS_COUNT];
			column->type = vogal_utils::dataType(types[i+C_OBJECTS_COLS_COUNT]);
			if (!stPut(cursor->colsList, column->name, column))
				goto freeOpenTable;
		}
		ret = true;
		goto freeOpenTable;
	}
	
	// Se está tentando abrir a tabela de objetos, a inicialização deve eser manual pois não há donde obter sua definição
	if (!strcmp(tableName,C_OBJECTS)) {
		char * cols[] = C_COLUMNS_NAMES;
		char * types[] = C_COLUMNS_TYPES;
		cursor->block = m_Handler->getStorage->openBlock(C_OBJECTS_BLOCK);
		if (!cursor->block) {
			perror("Erro ao abrir bloco de dados da tabela OBJS");
			goto freeOpenTable;
		}
		cursor->colsList = stNew(FALSE, TRUE);
		if (!cursor->colsList)
			goto freeOpenTable;
		for (i = 0; i < C_OBJECTS_COLS_COUNT; i++) {
			ColumnCursorType * column = new ColumnCursorType();
			column->name = cols[i];
			column->type = vogal_utils::str2type(types[i]);
			if (!stPut(cursor->colsList, column->name, column))
				goto freeOpenTable;
		}
		ret = true;
		goto freeOpenTable;
	}
	
	if (!m_Handler->getCache()->getObjects() || 
		!m_Handler->getCache()->getColumns()) {
		perror("Objetos básicos do metadados não foram inicializados!");
		goto freeOpenTable;
	} 
	
	// Senão, deve obter através das tabelas do dicionário de dados
	tree = stNew(FALSE, FALSE);
	stPut(tree, C_NAME_KEY, tableName);
	objsCursor = m_Handler->getManipulation()->openCursor(m_Handler->getCache()->getObjects(), tree);
	if (!objsCursor) {
		perror("Impossível abrir cursor dos objetos!");
		goto freeOpenTable;
	}

	// Procura a tabela	
	if (!m_Handler->getManipulation()->fetch(objsCursor)) {
		perror("Tabela não encontrada!"); 
		goto freeOpenTable;
	}
	
	// Obtém as colunas da tablea
	tree = stNew(FALSE, FALSE);
	stPut(tree, C_TABLE_RID_KEY, &objsCursor->fetch->id);
	colsCursor = m_Handler->getManipulation()->openCursor(m_Handler->getCache()->getColumns(), tree);
	if (!colsCursor) {
		perror("Impossível abrir cursor das colunas!");
		goto freeOpenTable;
	}
	// Varre as colunas da tabela	
	cursor->colsList = stNew(FALSE, TRUE);
	if (!cursor->colsList)
		goto freeOpenTable;
	while (m_Handler->getManipulation()->fetch(colsCursor)) {
		TreeNodeValue value;

		ColumnCursorType * column = new ColumnCursorType();

		if (!stGet(colsCursor->fetch->dataList, C_COLUMN_SID_KEY, &value))
			goto freeOpenTable;
		column->id = (* (BigNumber*) value );
		
		if (!stGet(colsCursor->fetch->dataList, C_NAME_KEY, &value))
			goto freeOpenTable;
		column->name = (char*) value; 
		
		if (!stGet(colsCursor->fetch->dataList, C_TYPE_KEY, &value))
			goto freeOpenTable;
		column->type = vogal_utils::dataType( (char*) value );
		
		if (!stPut(cursor->colsList, column->name, column)) 
			goto freeOpenTable;
		
	}

	ret = true;
	*/
	
freeOpenTable:
	if (objsCursor)
		objsCursor->~CursorType();
	if (colsCursor)
		colsCursor->~CursorType();
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
