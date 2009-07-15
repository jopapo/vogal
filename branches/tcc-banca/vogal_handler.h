
#ifndef VOGAL_HANDLER
#define VOGAL_HANDLER

class vogal_handler; // Necessário pelo interrelacionamento com a vogal_cache e vogal_storage

#include <my_global.h>
#include <mysql_priv.h>
#include "vogal_utils.h"
#include "vogal_cache.h"
#include "vogal_storage.h"
#include "vogal_definition.h"
#include "vogal_manipulation.h"

class vogal_handler
{
public:
	vogal_handler();
	virtual ~vogal_handler();
	
	vogal_cache* getCache();
	vogal_storage* getStorage();
	vogal_definition* getDefinition();
	vogal_manipulation* getManipulation();
	
	int ensureSanity();

private:
	vogal_cache *m_Cache;
	vogal_storage *m_Storage;
	vogal_definition *m_Definition;
	vogal_manipulation *m_Manipulation;

};

#endif
