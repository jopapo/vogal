/***************************************************************************
 *            list.c
 *
 *  Sat Apr 11 21:18:48 2009
 *  Copyright  2009  João Paulo Poffo
 *  <jopapo@gmail.com>
 ****************************************************************************/

#include "list.h"

// ### RANGE LIST IMPLEMENATION ###

struct linkedList_st {
	long int key;
	unsigned char link;
	struct linkedList_st *next;
};
typedef struct linkedList_st * linkedList_p;

linkedListRoot_p llNew() {
	linkedListRoot_p root = (linkedListRoot_p) malloc(sizeof(struct linkedListRoot_st));
	root->root = NULL;
	return root;
}

int llFreeNode(linkedList_p node) {
	if (node) {
		llFreeNode(node->next);
		free(node);
	}
	return TRUE;
}

int llFree(linkedListRoot_p root) {
	llFreeNode(root->root);
	free(root);
	return TRUE;
}

void llMergeCheck(linkedList_p previous, linkedList_p node) {
	if (!node)
		return;
	linkedList_p next = node->next;
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

int llPush(linkedListRoot_p root, long int key) {
	if (!root) 
		return FALSE;
	
	// 1o caso: Lista vazia
	if (!root->root) {
		root->root = (linkedList_p) malloc (sizeof(struct linkedList_st));
		linkedList_p node = root->root;
		node->key = key;
		node->link = 0;
		node->next = NULL;
		return TRUE;
	}
	
	// 2o caso: Busca do item na lista
	linkedList_p previous = NULL;
	linkedList_p iterator = root->root;
	while (iterator) {
		long int dif = iterator->key - key;
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
			linkedList_p node = (linkedList_p) malloc (sizeof(struct linkedList_st));
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
			linkedList_p next = iterator->next;
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
					linkedList_p node = (linkedList_p) malloc (sizeof(struct linkedList_st));
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

int llPop(linkedListRoot_p root, long int * key) {
	if (!root) 
		return FALSE;
	
	// 2o caso: Busca do item na lista
	linkedList_p node = root->root;
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

void llRemoveMerge(linkedListRoot_p root, linkedList_p previous, linkedList_p node) {
	if (previous) {
		previous->link = FALSE;
		previous->next = node->next;
	} else {
		root->root = node->next;
	}
	free(node);
}

int llRemove(linkedListRoot_p root, long int key) {
	if (!root) 
		return FALSE;
	
	// 2o caso: Busca do item na lista
	linkedList_p
		previous = NULL,
		node = root->root;
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
			long int difp = key - previous->key,
					 difn = node->key - key;
			if (difp == 1 && difn == 1) {
			// Remove o item 3 tendo uma faixa de 2 a 4
				previous->link = FALSE;
			} else if (difp == 1) {
			// Remove o item 3 tendo uma faixa 2 a 100
				linkedList_p newnode = (linkedList_p) malloc (sizeof(struct linkedList_st));
				newnode->key = key + 1;
				newnode->link = TRUE;
				newnode->next = node;
				previous->link = FALSE;
				previous->next = newnode;
			} else if (difn == 1) {
			// Remove o item 3 tendo uma faixa 1 a 4
				linkedList_p newnode = (linkedList_p) malloc (sizeof(struct linkedList_st));
				newnode->key = key - 1;
				newnode->link = FALSE;
				newnode->next = node;
				previous->link = TRUE;
				previous->next = newnode;
			} else {
			// Remove o item 3 tendo uma faixa 1 a 100
				linkedList_p newnextnode = (linkedList_p) malloc (sizeof(struct linkedList_st));
				newnextnode->key = key + 1;
				newnextnode->link = TRUE;
				newnextnode->next = node;

				linkedList_p newnode = (linkedList_p) malloc (sizeof(struct linkedList_st));
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

int llDump(linkedListRoot_p root) {
	if (!root)
		return FALSE;
	printf("<Range Linked List Dump>\n");
	linkedList_p iterator = root->root;
	while (iterator) {
		printf("	key: %li", iterator->key);
		if (iterator->link) {
			iterator = iterator->next;
			printf(" até %li", iterator->key);
		};
		printf("\n");
		iterator = iterator->next;
	}
	printf("</Range Linked List Dump>\n");
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

int add(treeRootNode_t **root, long int key) {
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

int remove(treeRootNode_t **root, long int key) {
	treeNode_t*
		parent = (treeNode_t) malloc(sizeof(treeNode_t)),
		node = (treeNode_t) malloc(sizeof(treeNode_t));
	if (find(root, parent, node)) {
		parent.
	}
	return 0;
}

treeNode_t **find(treeRootNode_t **root, long int value) {
	treeNote_t *parent = NULL;
	treeNote_t *node = root.root;
	if (internal_find(&parent, value, &node))
		return node;
	return NULL;
}

int internal_find(treeNode_t *parent, long int value, treeNode_t *node) {
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