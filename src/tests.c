#include "dtree.h"

StringTreeRoot * testeArvore1() {
	// TESTE DE ÁRVORE
	ColumnCursorType * column1 = malloc(sizeof(ColumnCursorType));
	column1->id = 1;
	column1->name = C_NAME_KEY;
	column1->type = dataType(C_TYPE_VARCHAR);
	
	ColumnCursorType * column2 = malloc(sizeof(ColumnCursorType));
	column2->id = 2;
	char * teste = "TABLE_RID";
	column2->name = teste;
	column2->type = dataType(C_TYPE_NUMBER);
	
	
	StringTreeRoot * root = stNew(FALSE, FALSE);
	stPut(root, "Marcos", NULL);
	stPut(root, "Armero", NULL);
	stPut(root, "Danilo", NULL);
	stPut(root, column2->name, column2);
	stPut(root, "Maurício", NULL);
	stPut(root, "Fabinho Capixaba", NULL);
	stPut(root, "Pierre", NULL);
	stPut(root, "Sandro Silva", NULL);
	stPut(root, "Diego Souza", NULL);
	stPut(root, column1->name, column1);
	stPut(root, "Marquinhos", NULL);
	stPut(root, "Keirrison", NULL);
	stPut(root, "Williams", NULL);
	
	return root;
}

void testeArvore() {
	StringTreeRoot * root = testeArvore1();

	// TESTE DA ITERAÇÃO
	StringTreeNode* iNode;
	StringTreeIterator* iterator = stCreateIterator(root, &iNode);
	while (iNode) {
		printf("%s", stNodeName(iNode));
		ColumnCursorType * col =  stNodeValue(iNode);
		if (col) {
			printf(" - %lli,%s,%d", col->id, col->name, col->type);
		}
		printf("\n");
		iNode = stNext(iterator);
	}
	stFreeIterator(iterator);
	
	TreeNodeValue value;
	stGet(root, C_TABLE_RID_KEY, &value);
	ColumnCursorType * col = value;
	if (col) {
		printf("### %lli,%s,%d\n", col->id, col->name, col->type);
	} else {
		printf("### Nao achou!\n");
	}
	
	stFree(root);
}

void testaRangeLinkedList() {
	/* ### TESTE DA RANGE LINKED LIST ### 
	llPush(freeBuffers, 1);
	llPush(freeBuffers, 7002);
	llPush(freeBuffers, 7000);
	llPush(freeBuffers, 6999);
	llPush(freeBuffers, 7001);
	llPush(freeBuffers, 3000);
	llPush(freeBuffers, 4097);
	//llPush(freeBuffers, 2);
	llPush(freeBuffers, 4096);
	llDump(freeBuffers);
	long int v;
	llPop(freeBuffers, &v);
	printf("%li\n", v);
	llPop(freeBuffers, &v);
	printf("%li\n", v);*/
}

void tests() {
	testeArvore();
	testaRangeLinkedList();
}