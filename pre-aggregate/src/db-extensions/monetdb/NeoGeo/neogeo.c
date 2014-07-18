/*
 * Created by Dennis Pallett (dennis@pallett.nl)
 * July, 2014
 */

/* monetdb_config.h must be the first include in each .c file */
#include "monetdb_config.h"
#include "neogeo.h"

#define MYALLOC(X)  malloc(X)
#define MYFREE(X)   free(X)

#include "pa_grid.template"

char *
compute_pa_grid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	pa_grid* grid;
	BAT *gkey;
	BAT *pakey;
	char *query;
	int *rgkey = (int *) getArgReference(stk, pci, 0);
	int *rpakey = (int *) getArgReference(stk, pci, 1);

	// retrieve grid query argument
	switch (getArgType(mb, pci, 2)) {
	case TYPE_str:
		query = stk->stk[getArg(pci, 2)].val.sval;
		break;
	default:
		throw(ILLARG, "udf.pa_grid", "illegal argument: must be a valid grid query!");
	}

	// create the PA grid
	grid = create_pa_grid(query);

	// create new BAT to hold grid keys
	gkey = BATnew(TYPE_void, TYPE_lng, 1 << 16);
	if (gkey == NULL)
		throw(SQL, "udf.pa_grid", MAL_MALLOC_FAIL);

	// create new BAT to hold PA keys
	pakey = BATnew(TYPE_void, TYPE_lng, 1 << 16);
	if (pakey == NULL)
		throw(SQL, "udf.pa_grid_cell", MAL_MALLOC_FAIL);

	BATseqbase(gkey, 0);
	BATseqbase(pakey, 0);

	// insert PA keys into BATs
	while(next_pa_grid(grid)) {
		/*
		TODO: future optimization: check whether pakey is 
			  even an option (by referencing index of PA table) before inserting into BAT 
		*/
		
		BUNappend(gkey, &grid->gridKey, FALSE);
		BUNappend(pakey, &grid->cellKey, FALSE);
	}

	// free the grid
	free_pa_grid(grid); 

	// return a reference to the new BATs containing the gkey and pakey
	*rgkey = gkey->batCacheid;
	*rpakey = pakey->batCacheid;
	BBPkeepref(*rgkey);	
	BBPkeepref(*rpakey);	

	return MAL_SUCCEED;
}

char *
compute_pa_grid_cell(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	pa_grid* grid;
	BAT *pakey;
	char *query;
	int *rpakey = (int *) getArgReference(stk, pci, 0);

	// retrieve grid query argument
	switch (getArgType(mb, pci, 1)) {
	case TYPE_str:
		query = stk->stk[getArg(pci, 1)].val.sval;
		break;
	default:
		throw(ILLARG, "udf.pa_grid_cell", "illegal argument: must be a valid grid query!");
	}

	// create the PA grid
	grid = create_pa_grid(query);

	// create new BAT to hold PA keys
	pakey = BATnew(TYPE_void, TYPE_lng, 1 << 16);
	if (pakey == NULL)
		throw(SQL, "udf.pa_grid_cell", MAL_MALLOC_FAIL);

	BATseqbase(pakey, 0);

	// insert PA keys into BAT
	while(next_pa_grid(grid)) {
		/*
		TODO: future optimization: check whether key is 
			  even an option (by referencing index of PA table) before inserting into BAT 
		*/

		BUNappend(pakey, &grid->cellKey, FALSE);
	}

	// free the grid
	free_pa_grid(grid); 

	// return a reference to the new BAT containing the pakeys
	*rpakey = pakey->batCacheid;
	BBPkeepref(*rpakey);	

	return MAL_SUCCEED;
}


