
#include "vogal_utils.h"
#include <string.h>

vogal_utils::vogal_utils(){
	DBUG_ENTER("vogal_utils::vogal_utils");
	DBUG_LEAVE;
}

vogal_utils::~vogal_utils(){
	DBUG_ENTER("vogal_utils::~vogal_utils");
	DBUG_LEAVE;
}

DataTypes vogal_utils::str2type(char * str){
	DBUG_ENTER("vogal_utils::str2type");

	if (!strcmp(str, C_TYPE_VARCHAR))
		DBUG_RETURN(VARCHAR);
	if (!strcmp(str, C_TYPE_NUMBER))
		DBUG_RETURN(NUMBER);
	if (!strcmp(str, C_TYPE_DATE))
		DBUG_RETURN(DATE);
	if (!strcmp(str, C_TYPE_FLOAT))
		DBUG_RETURN(FLOAT);

	ERROR("Tipo de dado não identificado!");
	DBUG_RETURN(UNKNOWN);
}

char* vogal_utils::type2str(DataTypes type){
	DBUG_ENTER("vogal_utils::type2str");

	switch (type) {
		case VARCHAR: DBUG_RETURN(C_TYPE_VARCHAR);
		case NUMBER: DBUG_RETURN(C_TYPE_NUMBER);
		case DATE: DBUG_RETURN(C_TYPE_DATE);
		case FLOAT: DBUG_RETURN(C_TYPE_FLOAT);
	}

	ERROR("Tipo de dado não identificado!");
	DBUG_RETURN(C_TYPE_UNKNOWN);
}

DataTypes vogal_utils::myType2VoType(enum enum_field_types type) {
  switch (type) {
    case MYSQL_TYPE_BIT:
	case MYSQL_TYPE_NEWDECIMAL:
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_ENUM:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_TINY_BLOB:
		return NUMBER;

	case MYSQL_TYPE_BLOB:
	case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_VARCHAR:
    	return VARCHAR;
    
	case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_NEWDATE:
	case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_YEAR:
		return DATE;
    
	case MYSQL_TYPE_DOUBLE:
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_GEOMETRY:
		return FLOAT;

	//case MYSQL_TYPE_SET:
    //case MYSQL_TYPE_NULL:
    default:
		return UNKNOWN;
  }
}

char * vogal_utils::cloneString(char * from){
	DBUG_ENTER("vogal_utils::cloneString");

	char * to = new char[strlen(from) + 1];

	DBUG_RETURN(strcpy(to, from));
}
