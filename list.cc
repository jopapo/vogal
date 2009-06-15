/***************************************************************************
 *            list.c
 *
 *  Sat Apr 11 21:18:48 2009
 *  Copyright  2009  João Paulo Poffo
 *  <jopapo@gmail.com>
 ****************************************************************************/

#include "list.h"
#include <my_global.h>
#include "vogal_utils.h"

// ### RANGE LIST IMPLEMENATION ###

LinkedListRootType* llNew() {
	DBUG_ENTER("llNew");
	
	LinkedListRootType* root = (LinkedListRootType*) malloc(sizeof(LinkedListRootType));
	root->root = NULL;
	
	DBUG_RETURN(root);
}

int llFreeNode(LinkedListType** node) {
	DBUG_ENTER("llFreeNode");
	
	if (node && (*node)) {
		llFreeNode(&(*node)->next);
		free((*node));
		(*node) = NULL;
	}
	
	DBUG_RETURN(true);
}

int llFree(LinkedListRootType** root) {
	DBUG_ENTER("llFree");
	
	llFreeNode(&(*root)->root);
	free((*root));
	(*root) = NULL;
	
	DBUG_RETURN(true);
}

// TODO: Melhorar esse merge. Pra que criar pra remover depois?
void llMergeCheck(LinkedListType* previous, LinkedListType* node) {
	DBUG_ENTER("llMergeCheck");
	if (!node)
		DBUG_LEAVE;
	LinkedListType* next = node->next;
	// Verifica se a sequência tem algo que poderia ser incorporado
	if ((next) && (node->key - next->key == -1)) {
		if ((previous) && (previous->link)) {
			node->next = next->next;
			if (!next->link) { // Se não for faixa, mata o próximo
				node->key = next->key;
				free(next);
			} else { // Se for faixa, mata a faixa completa
				// Faz o ponteiro pular
				node->next = next->next->next;
				node->key = next->next->key;
				// Fim da faixa
				free(next->next);
				// Início da faixa
				free(next);
			}
		} else {
			if (next->link) {
				next->key = node->key;
				previous->next = next;
				free(node);
			} else 
				node->link = true;
		}
	}
	DBUG_LEAVE;
}

int llPush(LinkedListRootType* root, ListDataType key) {
	DBUG_ENTER("llPush");
	if (!root) 
		DBUG_RETURN(false);
	
	// 1o caso: Lista vazia
	if (!root->root) {
		root->root = (LinkedListType*) malloc (sizeof(LinkedListType));
		LinkedListType* node = root->root;
		node->key = key;
		node->link = 0;
		node->next = NULL;
		DBUG_RETURN(true);
	}
	
	// 2o caso: Busca do item na lista
	LinkedListType* previous = NULL;
	LinkedListType* iterator = root->root;
	while (iterator) {
		int dif = iterator->key - key;
		// É igual
		if (!dif) // dif == 0
			DBUG_RETURN(true);
		// Deve estar antes
		if (dif > 0) {
			if ((dif == 1) && (iterator->link)) {
				iterator->key = key;
				llMergeCheck(previous, iterator);
				DBUG_RETURN(true);
			} 
			LinkedListType* node = (LinkedListType*) malloc (sizeof(LinkedListType));
			node->key = key;
			node->link = false;
			node->next = iterator;
			if (previous)
				previous->next = node;
			else
				root->root = node;
			DBUG_RETURN(true);
		}
		// Deve estar depois
		if (dif < 0) {
			LinkedListType* next = iterator->next;
			if (iterator->link) {
				if ((next) && (next->key >= key))
					DBUG_RETURN(true); // Se estiver na faixa, cai fora
			} else {
				// Se for o próximo (1 de diferença)
				if (dif == -1) {
					if ((previous) && (previous->link)) {
						iterator->key = key;
						llMergeCheck(previous, iterator);
						DBUG_RETURN(true);
					}
				}
				if ((!next) || (next->key > key)) {
					// Se for o próximo (mais de 1 de diferença)
					LinkedListType* node = (LinkedListType*) malloc (sizeof(LinkedListType));
					node->key = key;
					node->link = false;
					node->next = iterator->next;
					iterator->next = node;
					if (dif == -1) 
						iterator->link = true;
					llMergeCheck(iterator, node);
					DBUG_RETURN(true);
				}
			}
		}
		// Next one
		previous = iterator;
		iterator = iterator->next;
	}
	DBUG_RETURN(false);
}

// 2o caso: Busca do item na lista
int llPop(LinkedListRootType* root, ListDataType * key) {
	DBUG_ENTER("llPop");
	if (!root) 
		DBUG_RETURN(false);
	LinkedListType* node = root->root;
	if (!node) 
		DBUG_RETURN(false);
	(*key) = node->key;
	if (node->next) {
		if (node->link) {
			node->key += 1;
			if (node->key < node->next->key)
				DBUG_RETURN(true);
		} 
		root->root = node->next;
		free(node);
	} else {
		free(root->root);
		root->root = NULL;
	}
	
	DBUG_RETURN(true);
}

void llRemoveMerge(LinkedListRootType* root, LinkedListType* previous, LinkedListType* node) {
	DBUG_ENTER("llRemoveMerge");
	if (previous) {
		previous->link = false;
		previous->next = node->next;
	} else {
		root->root = node->next;
	}
	free(&node);
	DBUG_LEAVE;
}

int llRemove(LinkedListRootType* root, ListDataType key) {
	DBUG_ENTER("llRemove");
	if (!root) 
		DBUG_RETURN(false);
	
	// 2o caso: Busca do item na lista
	LinkedListType* previous = NULL;
	LinkedListType* node = root->root;
	
	while ((node) && (key < node->key)) {
		// Nas bordas
		if (key == node->key) {
			if (node->link) {
				node->key++;
				if (node->key == node->next->key) {
					llRemoveMerge(root, previous, node);
				}
				DBUG_RETURN(true);				
			} else {
				if ((previous) && (previous->link)) {
					node->key--;
					if (node->key == previous->key) {
						llRemoveMerge(root, previous, node);
					}
				} else {
					llRemoveMerge(root, previous, node);
				}
				DBUG_RETURN(true);
			}
		// No meio de uma faixa
		} else if ((previous) && 
				   (key > previous->key) && 
				   (key < node->key)) {
			int difp = key - previous->key,
					 difn = node->key - key;
			if (difp == 1 && difn == 1) {
			// Remove o item 3 tendo uma faixa de 2 a 4
				previous->link = false;
			} else if (difp == 1) {
			// Remove o item 3 tendo uma faixa 2 a 100
				LinkedListType* newnode = (LinkedListType*) malloc (sizeof(LinkedListType));
				newnode->key = key + 1;
				newnode->link = true;
				newnode->next = node;
				previous->link = false;
				previous->next = newnode;
			} else if (difn == 1) {
			// Remove o item 3 tendo uma faixa 1 a 4
				LinkedListType* newnode = (LinkedListType*) malloc (sizeof(LinkedListType));
				newnode->key = key - 1;
				newnode->link = false;
				newnode->next = node;
				previous->link = true;
				previous->next = newnode;
			} else {
			// Remove o item 3 tendo uma faixa 1 a 100
				LinkedListType* newnextnode = (LinkedListType*) malloc (sizeof(LinkedListType));
				newnextnode->key = key + 1;
				newnextnode->link = true;
				newnextnode->next = node;

				LinkedListType* newnode = (LinkedListType*) malloc (sizeof(LinkedListType));
				newnode->key = key - 1;
				newnode->link = false;
				newnode->next = newnextnode;
				
				previous->next = newnode;
			}
			DBUG_RETURN(true);
		}
		previous = node;
		node = node->next;
	}
	
	DBUG_RETURN(false);
}

int llDump(LinkedListRootType* root) {
	DBUG_ENTER("llDump");
	if (!root)
		DBUG_RETURN(false);
	printf("<RangeLinkedList>\n");
	LinkedListType* iterator = root->root;
	while (iterator) {
		printf("\t<key startValue=%lli", iterator->key);
		if (iterator->link) {
			iterator = iterator->next;
			printf(" endValue=%lli", iterator->key);
		};
		printf(" \\>\n");
		iterator = iterator->next;
	}
	printf("</RangeLinkedList>\n");
	DBUG_RETURN(true);
}

// ### ValueList ###

#define C_VL_GROW_FACTOR 10

ValueListRoot* vlNew(bool owner) {
	DBUG_ENTER("vlNew");
	ValueListRoot * root = new ValueListRoot();
	root->root = NULL;
	root->space = 0;
	root->count = 0;
	root->owner = owner;
	DBUG_RETURN(root);
}

int vlFree(ValueListRoot** root) {
	DBUG_ENTER("vlFree");
	if (!(*root))
		DBUG_RETURN(true);
	if ((*root)->root) {
		if ((*root)->owner)
			for (int i = 0; i < (*root)->count; i++)
				free((*root)->root[i]);
		free((*root)->root);
	}
	free((*root));
	(*root) = NULL;
	DBUG_RETURN(true);
}

int vlGrow(ValueListRoot* root) {
	DBUG_ENTER("vlGrow");
	// Se é necessário crescer
	if (root->count >= root->space) {
		root->space += C_VL_GROW_FACTOR;
		if (!root->root)
			root->root = (ListValueType*) malloc(root->space * sizeof(ListValueType*));
		else
			root->root = (ListValueType*) realloc(root->root, root->space * sizeof(ListValueType*));
	}
	DBUG_RETURN(true);	
}

int vlAdd(ValueListRoot* root, ListValueType value) {
	DBUG_ENTER("vlAdd");
	if (!vlGrow(root))
		DBUG_RETURN(false);
	root->root[root->count] = value;
	root->count++;
	DBUG_RETURN(true);
}

int vlInsert(ValueListRoot* root, ListValueType value, int index) {
	DBUG_ENTER("vlInsert");
	if (!root)
		DBUG_RETURN(false);
	if (index < 0 || index > root->count) {
		ERROR("Index inválido!");
		DBUG_RETURN(false);
	}
	if (!vlGrow(root))
		DBUG_RETURN(false);
	for (int i = root->count; i > index; i--)
		root->root[i] = root->root[i-1];
	root->root[index] = value;
	root->count++;
	DBUG_RETURN(true);
}

int vlRemove(ValueListRoot* root, int index) {
	DBUG_ENTER("vlRemove");
	if (!root)
		DBUG_RETURN(false);
	if (index < 0 || index >= root->count) {
		ERROR("Index inválido!");
		DBUG_RETURN(false);
	}
	// Grava ponteiro do nó
	ListValueType removed = root->root[index];
	if (root->owner) // Remove se necessário
		free(removed);
	// Reorganiza lista
	for (int i = index; i < root->count - 1; i++)
		root->root[i] = root->root[i+1];
	root->count--;
	DBUG_RETURN(true);
}

int vlCount(ValueListRoot* root) {
	DBUG_ENTER("vlCount");
	if (!root) {
		DBUG_PRINT("WARN", ("Lista não existe! Retornando zero na contagem."));
		DBUG_RETURN(0);
	}
	DBUG_RETURN(root->count);	
}

ListValueType * vlGet(ValueListRoot* root, int index) {
	DBUG_ENTER("vlGet");
	if ((index < 0) ||
	    (index >= root->count)) {
		ERROR("Fora dos limites da lista!"); 
		DBUG_RETURN(NULL);
	}
	DBUG_RETURN( (ListValueType *) root->root[index] );
}

int vlPush(ValueListRoot* root,ListValueType value) {
	DBUG_ENTER("vlPush");
	DBUG_RETURN(vlAdd(root, value));
}

ListValueType* vlPop(ValueListRoot* root) {
	DBUG_ENTER("vlPop");
	int last = vlCount(root) - 1;
	ListValueType* value = vlGet(root, last);
	root->count--; // Remove da lista o último elemento
	DBUG_RETURN(value); 
}

// ### PairList ###

PairListRoot* plNew(bool nameOwner, bool valueOwner) {
	DBUG_ENTER("plNew");
	PairListRoot * root = (PairListRoot *) malloc(sizeof(PairListRoot));
	root->a = vlNew(nameOwner);
	root->b = vlNew(valueOwner);
	DBUG_RETURN(root); 
}

int plFree(PairListRoot** root) {
	DBUG_ENTER("plFree");
	if (!(*root))
		DBUG_RETURN(true);
	if (!vlFree(&(*root)->a))
		DBUG_RETURN(false);
	if (!vlFree(&(*root)->b))
		DBUG_RETURN(false);
	free((*root));
	(*root) = NULL;
	DBUG_RETURN(true);
}

int plAdd(PairListRoot* root, ListValueType a, ListValueType b) {
	DBUG_ENTER("plAdd");
	if (!root)
		DBUG_RETURN(false);
	if (!vlAdd(root->a, a))
		DBUG_RETURN(false);
	if (!vlAdd(root->b, b))
		DBUG_RETURN(false);
	DBUG_RETURN(true);
}

int plCount(PairListRoot* root) {
	DBUG_ENTER("plCount");
	DBUG_RETURN( vlCount(root->a) );
}

int plGet(PairListRoot* root, int index, ListValueType* a, ListValueType* b) {
	DBUG_ENTER("plGet");
	(*a) = vlGet(root->a, index);
	(*b) = vlGet(root->b, index);
	DBUG_RETURN(true);
}

ListValueType * plGetName(PairListRoot* root, int index) {
	DBUG_ENTER("plGetName");
	DBUG_RETURN( vlGet(root->a, index) );
}

ListValueType * plGetValue(PairListRoot* root, int index) {
	DBUG_ENTER("plGetValue");
	DBUG_RETURN( vlGet(root->b, index) );
}


// ### StringTree ###

StringTreeRoot* stNew(int nameOwner, int valueOwner) {
	DBUG_ENTER("stNew");
	StringTreeRoot * root = (StringTreeRoot*) malloc(sizeof(StringTreeRoot));
	root->count = 0;
	root->root = NULL;
	// Owner significa que, se for true, libera a memória ao terminar.
	root->nameOwner = nameOwner;
	root->valueOwner = valueOwner;
	DBUG_RETURN(root);
}

int stFreeNode(StringTreeRoot* root, StringTreeNode** node) {
	DBUG_ENTER("stFreeNode");
	if ((*node)) {
		if (!stFreeNode(root, &(*node)->left))
			DBUG_RETURN(false);
		if (!stFreeNode(root, &(*node)->rigth))
			DBUG_RETURN(false);
		if (root->nameOwner)
			free((*node)->name);
		if (root->valueOwner)
			free((*node)->value);
		free((*node));
		(*node) = NULL;
	}
	DBUG_RETURN(true);
}

int stFree(StringTreeRoot** root) {
	DBUG_ENTER("stFree");
	if (!(*root))
		DBUG_RETURN(true);
	if (!stFreeNode((*root), &(*root)->root))
		DBUG_RETURN(false);
	free((*root));
	(*root) = NULL;
	DBUG_RETURN(true);
}

StringTreeNode ** stFind(StringTreeNode ** node, TreeNodeName name, int * diff) {
	DBUG_ENTER("stFind");
	if ( (*node) ) {
		(*diff) = strcmp((*node)->name, name);
		if ((*diff)) {
			if ((*diff) > 0)
				DBUG_RETURN( stFind(&(*node)->left, name, diff) );
			DBUG_RETURN( stFind(&(*node)->rigth, name, diff) );
		}
	}
	DBUG_RETURN(node);
}

int stPut(StringTreeRoot* root, TreeNodeName name, TreeNodeValue value) {
	DBUG_ENTER("stPut");
	if (!name)
		DBUG_RETURN(false);
	int diff = 1;
	StringTreeNode ** node = stFind(&root->root, name, &diff);
	if (!diff) {
		(*node)->value = value;
	} else {
		if ((*node)) {
			ERROR("Erro ao inserir na ramificação da árvore. Varredura incompleta!");
			DBUG_RETURN(false);
		}
		(*node) = (StringTreeNode*) malloc(sizeof(StringTreeNode));
		(*node)->name = name;
		(*node)->value = value;
		(*node)->left = NULL;
		(*node)->rigth = NULL;
		root->count++;
	}
	DBUG_RETURN(true);
}

int stCount(StringTreeRoot* root) {
	DBUG_ENTER("stCount");
	DBUG_RETURN(root->count);
}

int stGet(StringTreeRoot* root, TreeNodeName name, TreeNodeValue* value) {
	DBUG_ENTER("stGet");
	int diff = 1;
	StringTreeNode ** node = stFind(&root->root, name, &diff);
	if (!diff) {
		(*value) = (*node)->value;
		DBUG_RETURN(true);
	}
	DBUG_RETURN(false);
}

int stInternalNextPush(StringTreeIterator* iterator, StringTreeNode* node) {
	DBUG_ENTER("stInternalNextPush");
	if (!node) 
		DBUG_RETURN(false);
	// Empilha todos os lefts...
	for (; (node) && (node->left); node = node->left)
		vlPush(iterator->stack, node);
	// Empilha o último nodo a esquerda (não tem mais esquerdas)
	if (node)
		vlPush(iterator->stack, node);

	DBUG_RETURN(true);
}

StringTreeIterator* stCreateIterator(StringTreeRoot* root, StringTreeNode** first) {
	DBUG_ENTER("stCreateIterator");
	StringTreeIterator* iterator = (StringTreeIterator*) malloc(sizeof(StringTreeIterator));
	iterator->root = root;
	iterator->stack = vlNew(false);
	stInternalNextPush(iterator, iterator->root->root);
	(*first) = stNext(iterator);
	DBUG_RETURN(iterator);
}

int stFreeIterator(StringTreeIterator** iterator) {
	DBUG_ENTER("stFreeIterator");
	if (!(*iterator))
		DBUG_RETURN(true);
	vlFree(&(*iterator)->stack);
	free((*iterator));
	(*iterator) = NULL;
	DBUG_RETURN(true);
}

StringTreeNode* stNext(StringTreeIterator* iterator) {
	DBUG_ENTER("stNext");
	if (!iterator->stack->count)
		DBUG_RETURN(NULL);
	StringTreeNode* node = (StringTreeNode*) vlPop(iterator->stack);
	if (node) {
		stInternalNextPush(iterator, node->rigth);
		DBUG_RETURN(node); 
	}
	DBUG_RETURN(NULL);
}

TreeNodeName stNodeName(StringTreeNode* node) {
	DBUG_ENTER("stNodeName");
	if (!node)
		DBUG_RETURN(NULL);
	DBUG_RETURN(node->name);
}

TreeNodeValue stNodeValue(StringTreeNode* node) {
	DBUG_ENTER("stNodeValue");
	if (!node)
		DBUG_RETURN(NULL);
	DBUG_RETURN(node->value);
}
