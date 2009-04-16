/***************************************************************************
 *            list.h
 *
 *  Sat Apr 11 21:19:15 2009
 *  Copyright  2009  João Paulo Poffo
 *  <jopapo@gmail.com>
 ****************************************************************************/

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "bool.h"

// ### RANGE LIST IMPLEMENATION ###
struct linkedListRoot_st {
	struct linkedList_st *root;
};
typedef struct linkedListRoot_st * linkedListRoot_p;

linkedListRoot_p llNew();
int llFree(linkedListRoot_p);
int llPush(linkedListRoot_p, long int);
int llPop(linkedListRoot_p, long int *);
int llRemove(linkedListRoot_p, long int);
int llDump(linkedListRoot_p);


/* ### TREE IMPLEMENTATION ###

struct treeNode_t {
	long int key;
	void* leftLeaf;
	void* rightLeaf;
}

struct treeRootNode_t {
	treeNode_t* root;
}

int create(treeRootNode_t *);
void destroy(treeRootNode_t *);
int add(treeRootNode_t **, long int);
int remove(treeRootNode_t **, long int);
int find(treeRootNode_t **, long int, treeNode_t **);

*/