
#ifndef VOGAL_COLUMN_CURSOR_TYPE
#define VOGAL_COLUMN_CURSOR_TYPE

#include "vogal_utils.h"

class ColumnCursorType {

public:
	ColumnCursorType(int id);
	virtual ~ColumnCursorType();

	char 			*name;
	DataTypes	     type;
	BlockOffset  	 blockId;

	// Necessário para atualização da localização quando necessário
	BigNumber		 ridNumber;

	int getId();
	
private:
	int 			 m_Id; // Sequencial na tabela para identificar interrelacionamento de dados

};

#endif
