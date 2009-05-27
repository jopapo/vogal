/***************************************************************************
 *            list.c
 *
 *  Sat Apr 11 21:18:48 2009
 *  Copyright  2009  João Paulo Poffo
 *  <jopapo@gmail.com>
 ****************************************************************************/

#include "list.h"

// ### RANGE LIST IMPLEMENATION ###

LinkedListRootType* llNew() {
	LinkedListRootType* root = (LinkedListRootType*) malloc(sizeof(LinkedListRootType));
	root->root = NULL;
	return root;
}

int llFreeNode(LinkedListType* node) {
	if (node) {
		llFreeNode(node->next);
		free(node);
	}
	return TRUE;
}

int llFree(LinkedListRootType* root) {
	llFreeNode(root->root);
	free(root);
	return TRUE;
}

void llMergeCheck(LinkedListType* previous, LinkedListType* node) {
	if (!node)
		return;
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
			node->link = TRUE;
		}
	}
}

int llPush(LinkedListRootType* root, ListDataType key) {
	if (!root) 
		return FALSE;
	
	// 1o caso: Lista vazia
	if (!root->root) {
		root->root = (LinkedListType*) malloc (sizeof(LinkedListType));
		LinkedListType* node = root->root;
		node->key = key;
		node->link = 0;
		node->next = NULL;
		return TRUE;
	}
	
	// 2o caso: Busca do item na lista
	LinkedListType* previous = NULL;
	LinkedListType* iterator = root->root;
	while (iterator) {
		int dif = iterator->key - key;
		// É igual
		if (!dif) // dif == 0
			return TRUE;
		// Deve estar antes
		if (dif > 0) {
			if ((dif == 1) && (iterator->link)) {
				iterator->key = key;
				llMergeCheck(previous, iterator);
				return TRUE;
			} 
			LinkedListType* node = (LinkedListType*) malloc (sizeof(LinkedListType));
			node->key = key;
			node->link = FALSE;
			node->next = iterator;
			if (previous)
				previous->next = node;
			else
				root->root = node;
			return TRUE;
		}
		// Deve estar depois
		if (dif < 0) {
			LinkedListType* next = iterator->next;
			if (iterator->link) {
				if ((next) && (next->key >= key))
					return TRUE; // Se estiver na faixa, cai fora
			} else {
				// Se for o próximo (1 de diferença)
				if (dif == -1) {
					if ((previous) && (previous->link)) {
						iterator->key = key;
						llMergeCheck(previous, iterator);
						return TRUE;
					}
				} 
				if ((!next) || (next->key > key)) {
					// Se for o próximo (mais de 1 de diferença)
					LinkedListType* node = (LinkedListType*) malloc (sizeof(LinkedListType));
					node->key = key;
					node->link = FALSE;
					node->next = iterator->next;
					iterator->next = node;
					if (dif == -1) 
						iterator->link = TRUE;
					llMergeCheck(iterator, node);
					return TRUE;
				}
			}
		}
		// Next one
		previous = iterator;
		iterator = iterator->next;
	}
	return FALSE;
}

int llPop(LinkedListRootType* root, ListDataType * key) {
	if (!root) 
		return FALSE;
	
	// 2o caso: Busca do item na lista
	LinkedListType* node = root->root;
	(*key) = node->key;
	if (node->next) {
		if (node->link) {
			node->key += 1;
			if (node->key < node->next->key)
				return TRUE;
		} 
		root->root = node->next;
		free(node);
	} else {
		free(root->root);
		root->root = NULL;
	}
	
	return TRUE;
}

void llRemoveMerge(LinkedListRootType* root, LinkedListType* previous, LinkedListType* node) {
	if (previous) {
		previous->link = FALSE;
		previous->next = node->next;
	} else {
		root->root = node->next;
	}
	free(node);
}

int llRemove(LinkedListRootType* root, ListDataType key) {
	if (!root) 
		return FALSE;
	
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
				return TRUE;				
			} else {
				if ((previous) && (previous->link)) {
					node->key--;
					if (node->key == previous->key) {
						llRemoveMerge(root, previous, node);
					}
				} else {
					llRemoveMerge(root, previous, node);
				}
				return TRUE;
			}
		// No meio de uma faixa
		} else if ((previous) && 
				   (key > previous->key) && 
				   (key < node->key)) {
			int difp = key - previous->key,
					 difn = node->key - key;
			if (difp == 1 && difn == 1) {
			// Remove o item 3 tendo uma faixa de 2 a 4
				previous->link = FALSE;
			} else if (difp == 1) {
			// Remove o item 3 tendo uma faixa 2 a 100
				LinkedListType* newnode = (LinkedListType*) malloc (sizeof(LinkedListType));
				newnode->key = key + 1;
				newnode->link = TRUE;
				newnode->next = node;
				previous->link = FALSE;
				previous->next = newnode;
			} else if (difn == 1) {
			// Remove o item 3 tendo uma faixa 1 a 4
				LinkedListType* newnode = (LinkedListType*) malloc (sizeof(LinkedListType));
				newnode->key = key - 1;
				newnode->link = FALSE;
				newnode->next = node;
				previous->link = TRUE;
				previous->next = newnode;
			} else {
			// Remove o item 3 tendo uma faixa 1 a 100
				LinkedListType* newnextnode = (LinkedListType*) malloc (sizeof(LinkedListType));
				newnextnode->key = key + 1;
				newnextnode->link = TRUE;
				newnextnode->next = node;

				LinkedListType* newnode = (LinkedListType*) malloc (sizeof(LinkedListType));
				newnode->key = key - 1;
				newnode->link = FALSE;
				newnode->next = newnextnode;
				
				previous->next = newnode;
			}
			return TRUE;
		}
		previous = node;
		node = node->next;
	}
	
	return FALSE;
}

int llDump(LinkedListRootType* root) {
	if (!root)
		return FALSE;
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
	return TRUE;
}

/* ### TREE IMPLEMENTATION ###
 
 
int create(treeRootNode_t *root) {
	root = (treeRootNode_t) malloc(sizeof(treeRootNode_t));
	root.root = NULL;
	return 1;
}

void free_node(treeNode_t **node) {
	if (node) {
		free_node(node->leftLeaf);
		free_node(node->rigthLeaf);
		free(node);
	}
}

void destroy(treeRootNode_t *root) {
	free_node(root.root);
}

int add(treeRootNode_t **root, ListDataType key) {
	treeNode_t *parent = (treeNode_t) malloc(sizeof(treeNode_t));
	if (find(root, parent))
		return 0;
	
	treeNode_t *node = (treeNode_t) malloc(sizeof(treeNode_t));
	node.key = key;
	node.leftLeaf = NULL;
	node.rightLeaf = NULL;
	
	if (parent) {
		if (key > parent->key) {
			node->leftLeaf = parent->leftLeaf;
			parent->leftLeaf = node;
		} else {
			node->rightLeaf = parent->rightLeaf;
			parent->rightLeaf = node;
		}
	} else {
		root.root = node;
	}
	
}

int remove(treeRootNode_t **root, ListDataType key) {
	treeNode_t*
		parent = (treeNode_t) malloc(sizeof(treeNode_t)),
		node = (treeNode_t) malloc(sizeof(treeNode_t));
	if (find(root, parent, node)) {
		parent.
	}
	return 0;
}

treeNode_t **find(treeRootNode_t **root, ListDataType value) {
	treeNote_t *parent = NULL;
	treeNote_t *node = root.root;
	if (internal_find(&parent, value, &node))
		return node;
	return NULL;
}

int internal_find(treeNode_t *parent, ListDataType value, treeNode_t *node) {
	if (node) {
		if (node.value == value)
			return 1;
		if (node.value < value) {
			if (internal_find(node, node.leftLeaf))
				return 1;
			return 0;
		} 
		if (node.value > value) {
			if (internal_find(node, node.rigthLeaf))
				return 1;
			return 0;
		} 
	}
	return 0;
}
*/

#define C_VL_GROW_FACTOR 10

ValueListRoot* vlNew() {
	ValueListRoot * root = malloc(sizeof(ValueListRoot));
	root->root = malloc(C_VL_GROW_FACTOR * sizeof(ListValueType*));
	root->space = C_VL_GROW_FACTOR;
	root->count = 0;
	return root;
}

int vlFree(ValueListRoot* root) {
	free(root->root);
	root->root = NULL;
	root->space = 0;
	root->count = 0;
	free(root);
	return TRUE;
}

int vlAdd(ValueListRoot* root, ListValueType value) {
	// Se é necessário crescer
	if (root->count >= root->space) {
		root->root = realloc(root->root, sizeof(root->root) + (C_VL_GROW_FACTOR * sizeof(ListValueType*)));
		root->space += C_VL_GROW_FACTOR;
	}
	root->root[root->count] = value;
	root->count++;
	return TRUE;
}

int vlCount(ValueListRoot* root) {
	return root->count;	
}

ListValueType * vlGet(ValueListRoot* root, int index) {
	if ((index < 0) ||
	    (index >= root->count)) {
		perror("Fora dos limites da lista!"); 
		return NULL;
	}
	return root->root[index];
}

int vlPush(ValueListRoot* root,ListValueType value) {
	return vlAdd(root, value);
}

ListValueType* vlPop(ValueListRoot* root) {
	int last = vlCount(root) - 1;
	ListValueType* value = vlGet(root, last);
	root->count--; // Remove da lista o último elemento
	return value; 
}

// ### PairList ###

PairListRoot* plNew() {
	PairListRoot * root = malloc(sizeof(PairListRoot));
	root->a = vlNew();
	root->b = vlNew();
	return root; 
}

int plFree(PairListRoot* root) {
	if (!root)
		return TRUE;
	if (!vlFree(root->a))
		return FALSE;
	if (!vlFree(root->b))
		return FALSE;
	free(root);
	return TRUE;
}

int plAdd(PairListRoot* root, ListValueType a, ListValueType b) {
	if (!root)
		return FALSE;
	if (!vlAdd(root->a, a))
		return FALSE;
	if (!vlAdd(root->b, b))
		return FALSE;
	return TRUE;
}

int plCount(PairListRoot* root) {
	return vlCount(root->a);
}

int plGet(PairListRoot* root, int index, ListValueType* a, ListValueType* b) {
	(*a) = vlGet(root->a, index);
	(*b) = vlGet(root->b, index);
	return TRUE;
}


// ### StringTree ###

StringTreeRoot* stNew(int nameOwner, int valueOwner) {
	StringTreeRoot * root = malloc(sizeof(StringTreeRoot));
	root->count = 0;
	root->root = NULL;
	// Owner significa que, se for TRUE, libera a memória ao terminar.
	root->nameOwner = nameOwner;
	root->valueOwner = valueOwner;
	return root; 
}

int stFreeNode(StringTreeRoot* root, StringTreeNode* node) {
	if (node) {
		if (!stFreeNode(root, node->left))
			return FALSE;
		if (!stFreeNode(root, node->rigth))
			return FALSE;
		if (root->nameOwner)
			free(node->name);
		if (root->valueOwner)
			free(node->value);
		free(node);
	}
	return TRUE;
}

int stFree(StringTreeRoot* root) {
	if (!stFreeNode(root, root->root))
		return FALSE;
	root->root = NULL;
	root->count = 0;
	return TRUE;
}

StringTreeNode ** stFind(StringTreeNode ** node, TreeNodeName name, int * diff) {
	if ( (*node) ) {
		(*diff) = strcmp((*node)->name, name);
		if ((*diff)) {
			if ((*diff) > 0)
				return stFind(&(*node)->left, name, diff);
			return stFind(&(*node)->rigth, name, diff);
		}
	}
	return node;
}

int stPut(StringTreeRoot* root, TreeNodeName name, TreeNodeValue value) {
	if (!name)
		return FALSE;
	int diff = 1;
	StringTreeNode ** node = stFind(&root->root, name, &diff);
	if (!diff) {
		(*node)->value = value;
	} else {
		if ((*node)) {
			perror("Erro ao inserir na ramificação da árvore. Varredura incompleta!");
			return FALSE;
		}
		(*node) = malloc(sizeof(StringTreeNode));
		(*node)->name = name;
		(*node)->value = value;
		(*node)->left = NULL;
		(*node)->rigth = NULL;
		root->count++;
	}
	return TRUE;
}

int stCount(StringTreeRoot* root) {
	return root->count;
}

int stGet(StringTreeRoot* root, TreeNodeName name, TreeNodeValue* value) {
	int diff = 1;
	StringTreeNode ** node = stFind(&root->root, name, &diff);
	if (!diff) {
		(*value) = (*node)->value;
		return TRUE;
	}
	return FALSE;
}

int stInternalNextPush(StringTreeIterator* iterator, StringTreeNode* node) {
	if (!node) 
		return FALSE;
	// Empilha todos os lefts...
	for (; (node) && (node->left); node = node->left)
		vlPush(iterator->stack, node);
	// Empilha o último nodo a esquerda (não tem mais esquerdas)
	if (node)
		vlPush(iterator->stack, node);

	return TRUE;
}

StringTreeIterator* stCreateIterator(StringTreeRoot* root, StringTreeNode** first) {
	StringTreeIterator* iterator = malloc(sizeof(StringTreeIterator));
	iterator->root = root;
	iterator->stack = vlNew();
	stInternalNextPush(iterator, iterator->root->root);
	(*first) = stNext(iterator);
	return iterator;
}

int stFreeIterator(StringTreeIterator* iterator) {
	vlFree(iterator->stack);
	free(iterator);
	return TRUE;
}

StringTreeNode* stNext(StringTreeIterator* iterator) {
	if (!iterator->stack->count)
		return NULL;
	StringTreeNode* node = (StringTreeNode*) vlPop(iterator->stack);
	if (node) {
		stInternalNextPush(iterator, node->rigth);
		return node; 
	}
	return NULL;
}

TreeNodeName stNodeName(StringTreeNode* node) {
	if (!node)
		return NULL;
	return node->name;
}

TreeNodeValue stNodeValue(StringTreeNode* node) {
	if (!node)
		return NULL;
	return node->value;
}
