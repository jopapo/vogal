
#ifndef VOGAL_STORAGE
#define VOGAL_STORAGE

class vogal_storage; // Necessário pelo interrelacionamento com a vogal_handler

#include "vogal_utils.h"
#include "vogal_handler.h"

class vogal_storage
{
public:
	vogal_storage(vogal_handler *handler);
	virtual ~vogal_storage();

	int openDatabase(int mode = 0);
	void closeDatabase();
	int initialize();

	int readBlock(BlockOffset block, GenericPointer buffer);
	BlockCursorType * openBlock(BlockOffset id);
	int writeOnBlock(GenericPointer buf, BlockOffset block);
	
	int writeNumber(BigNumber number, GenericPointer* where);
	int writeData(GenericPointer* dest, GenericPointer src, BigNumber dataBytes, DataTypes dataType);
	int writeDataSize(GenericPointer* dest, BigNumber size);
	int writeBlock(BlockCursorType* block);
	int writeAnyData(GenericPointer* dest, GenericPointer src, DataTypes dataType);
	int writeString(char * strValue, GenericPointer* where);
	
	int readSizedNumber(GenericPointer* src, BigNumber* number);
	int readDataSize(GenericPointer* src, BigNumber* dest);
	int readNumber(GenericPointer* src, BigNumber* number, BigNumber dataBytes);
	int readData(GenericPointer* src, GenericPointer dest, BigNumber dataBytes, BigNumber destSize, DataTypes dataType);
	
	int skipData(GenericPointer* src);
	int skipAllData(GenericPointer* src, BigNumber count);
	int skipDataSize(GenericPointer* src);

	bool isOpen();

	/*int writeBlockPointer(BlockOffset where, BlockOffset offset, GenericPointer* buf);
	int writeInt(int value, GenericPointer* dest);
	int writeIt(GenericPointer buf);
	int writeOnEmptyBlock(GenericPointer buf, BlockOffset* block);
	*/
	
private:
	vogal_handler *m_Handler;
	File m_DB;
	
};

#endif
