/***************************************************************************
 *            list.h
 *
 *  Sat Apr 11 21:19:15 2009
 *  Copyright  2009  João Paulo Poffo
 *  <jopapo@gmail.com>
 ****************************************************************************/

#ifndef VOGAL_LIST
#define VOGAL_LIST

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

// ### LinkedListType ###

typedef unsigned long long int ListDataType;

typedef struct linked_list_st {
	ListDataType key;
	unsigned char link;
	struct linked_list_st * next;
} LinkedListType;

typedef struct {
	LinkedListType *root;
} LinkedListRootType;

LinkedListRootType* llNew();
int llFree(LinkedListRootType**);
int llPush(LinkedListRootType*, ListDataType);
int llPop(LinkedListRootType*, ListDataType *);
int llRemove(LinkedListRootType*, ListDataType);
int llDump(LinkedListRootType*);


// ### ValueList ###

typedef void * ListValueType;

typedef struct {
	ListValueType *root;
	int space;
	int count;
	bool owner;
} ValueListRoot;

ValueListRoot* vlNew(bool owner);
int vlFree(ValueListRoot**);
int vlAdd(ValueListRoot*,ListValueType);
int vlInsert(ValueListRoot*,ListValueType,int);
int vlRemove(ValueListRoot*,int,bool = false);
int vlCount(ValueListRoot*);
int vlGrow(ValueListRoot*);
ListValueType* vlGet(ValueListRoot*, int);
// Simulação de pilha
int vlPush(ValueListRoot*,ListValueType);
ListValueType* vlPop(ValueListRoot*);


// ### PairList ###
typedef struct {
	ValueListRoot *a;
	ValueListRoot *b;
} PairListRoot;

PairListRoot* plNew(bool, bool);
int plFree(PairListRoot**);
int plAdd(PairListRoot*,ListValueType,ListValueType);
int plCount(PairListRoot*);
int plGet(PairListRoot*, int, ListValueType*, ListValueType*);
ListValueType * plGetName(PairListRoot*, int);
ListValueType * plGetValue(PairListRoot*, int);


// ### StringTree ###
typedef char* TreeNodeName;
typedef void* TreeNodeValue;

typedef struct string_tree_st {
	char * name;
	void * value;
	struct string_tree_st * left;
	struct string_tree_st * rigth;
} StringTreeNode;

typedef struct {
	StringTreeNode * root;
	int count;
	int nameOwner;
	int valueOwner;
} StringTreeRoot;

typedef struct {
	StringTreeRoot * root;
	ValueListRoot * stack;
} StringTreeIterator;

StringTreeRoot* stNew(int, int);
int stFree(StringTreeRoot**);
int stPut(StringTreeRoot*,TreeNodeName,TreeNodeValue);
int stCount(StringTreeRoot*);
int stGet(StringTreeRoot*,TreeNodeName,TreeNodeValue*);
StringTreeIterator* stCreateIterator(StringTreeRoot*, StringTreeNode**);
StringTreeNode* stNext(StringTreeIterator*);
int stFreeIterator(StringTreeIterator**);
TreeNodeName stNodeName(StringTreeNode*);
TreeNodeValue stNodeValue(StringTreeNode*);

#endif
