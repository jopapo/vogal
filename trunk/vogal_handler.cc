
#include "vogal_handler.h"

vogal_handler::vogal_handler(){
	DBUG_ENTER("vogal_handler::vogal_handler");
	
	m_Cache = new vogal_cache(this);
	m_Storage = new vogal_storage(this);
	m_Definition = new vogal_definition(this);
	m_Manipulation = new vogal_manipulation(this);
	
	DBUG_LEAVE;
}

vogal_handler::~vogal_handler(){
	DBUG_ENTER("vogal_handler::~vogal_handler");
	
	if (m_Storage) {
		m_Storage->~vogal_storage();
		m_Storage = NULL;
	}
	if (m_Cache) {
		m_Cache->~vogal_cache();
		m_Cache = NULL;
	}
	if (m_Definition) {
		m_Definition->~vogal_definition();
		m_Definition = NULL;
	}
	if (m_Manipulation) {
		m_Manipulation->~vogal_manipulation();
		m_Manipulation = NULL;
	}
		
	DBUG_LEAVE;
}

vogal_cache* vogal_handler::getCache() {
	DBUG_ENTER("vogal_handler::getCache");
	DBUG_RETURN(m_Cache);
}

vogal_storage* vogal_handler::getStorage() {
	DBUG_ENTER("vogal_handler::getStorage");
	DBUG_RETURN(m_Storage);
}

vogal_definition* vogal_handler::getDefinition() {
	DBUG_ENTER("vogal_handler::getDefinition");
	DBUG_RETURN(m_Definition);
}

vogal_manipulation* vogal_handler::getManipulation() {
	DBUG_ENTER("vogal_handler::getManipulation");
	DBUG_RETURN(m_Manipulation);
}

int vogal_handler::ensureSanity() {
	DBUG_ENTER("vogal_handler::ensureSanity");

	bool newDatabase = false;
	
	if (!getStorage()->openDatabase()) {
		if (getStorage()->initialize()) {
			newDatabase = true;
		} else {
			perror("Erro ao criar DB");
			DBUG_RETURN(false);
		}
	}
	
	if (!getCache()->bufferize()) {
		perror("Erro ao bufferizar DB");
		DBUG_RETURN(false);
	}
	
	// Verifica se os cursores do metadados estão carregados na memória
	// a abertura ocorre antes da criação pois a criação é física e a abertura é lógica
	if (!getCache()->openDataDictionary()) {
		perror("Erro ao abrir dicionário de dados");
		DBUG_RETURN(false);
	}

	if (newDatabase)
		if (!getDefinition()->createDataDictionary()) {
			perror("Erro ao criar dicionário de dados");
			DBUG_RETURN(false);
		}
		
	DBUG_RETURN(true);
}

