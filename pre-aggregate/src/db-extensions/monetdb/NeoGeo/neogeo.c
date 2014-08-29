/*
 * Created by Dennis Pallett (dennis@pallett.nl)
 * July, 2014
 */

/* monetdb_config.h must be the first include in each .c file */
#include "monetdb_config.h"
#include "neogeo.h"

#define MYALLOC(X)  malloc(X)
#define MYFREE(X)   free(X)

#include "pa_grid.template.c"

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
		throw(ILLARG, "neogeo.pa_grid", "illegal argument: must be a valid grid query!");
	}

	// create the PA grid
	grid = create_pa_grid(query);

	// create new BAT to hold grid keys
	gkey = BATnew(TYPE_void, TYPE_lng, 1 << 16);
	if (gkey == NULL)
		throw(SQL, "neogeo.pa_grid", MAL_MALLOC_FAIL);

	// create new BAT to hold PA keys
	pakey = BATnew(TYPE_void, TYPE_lng, 1 << 16);
	if (pakey == NULL)
		throw(SQL, "neogeo.pa_grid_cell", MAL_MALLOC_FAIL);

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
compute_pa_grid_enhanced(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	pa_grid* grid;
	int i;

	// how many aggregate values are requests?
	// we have 6 standard args and each requested aggregates add 2 arguments
	int aggrCount = (pci->argc - 6) / 2;

	// variables to hold new result BATs
	BAT *gkey;
	BAT *pakey;
	BAT *aggr_ret[aggrCount];

	// variables to hold existing BATs
	BAT *ckey_bat;
	BAT *aggr_bat[aggrCount];
	BATiter bat_iter[aggrCount];

	char *query;
	sql_schema *s;
	sql_table *t;
	sql_column *c;
	sql_column *aggr1_col;

	mvc *m = NULL;
	str msg;

	int *rgkey = (int *) getArgReference(stk, pci, 0);
	int *rpakey = (int *) getArgReference(stk, pci, 1);

	str *sch = (str *) getArgReference(stk, pci, aggrCount + 3);
	str *tbl = (str *) getArgReference(stk, pci, aggrCount + 4);
	str *col = (str *) getArgReference(stk, pci, aggrCount + 5);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, *sch);
	if (s == NULL)
		throw(SQL, "neogeo.pa_grid_enhanced", "3F000!Schema missing");
	t = mvc_bind_table(m, s, *tbl);
	if (t == NULL)
		throw(SQL, "neogeo.pa_grid_enhanced", "42S02!Table missing");
	c = mvc_bind_column(m, t, *col);
	if (c == NULL)
		throw(SQL, "neogeo.pa_grid_enhanced", "42S22!No such column");

	for (i = 0; i < aggrCount; i++) {
		// skip first parameters to get correct arg
		str *aggr_name = (str *) getArgReference(stk, pci, aggrCount + i + 6);
		sql_column *aggr_col = mvc_bind_column(m, t, *aggr_name);
		if (aggr_col == NULL)
			throw(SQL, "neogeo.pa_grid_enhanced", "42S22!No such column");

		aggr_bat[i] = store_funcs.bind_col(m->session->tr, aggr_col, RDONLY);
		bat_iter[i] = bat_iterator(aggr_bat[i]);	
	}

	ckey_bat = store_funcs.bind_col(m->session->tr, c, 0);

	// retrieve grid query argument
	switch (getArgType(mb, pci, aggrCount + 2)) {
	case TYPE_str:
		query = stk->stk[getArg(pci, aggrCount + 2)].val.sval;
		break;
	default:
		throw(ILLARG, "neogeo.pa_grid_enhanced", "illegal argument: must be a valid grid query!");
	}

	// create the PA grid
	grid = create_pa_grid(query);

	// create new BAT to hold grid keys
	gkey = BATnew(TYPE_void, TYPE_lng, 1 << 16);
	if (gkey == NULL)
		throw(SQL, "neogeo.pa_grid", MAL_MALLOC_FAIL);

	// create new BAT to hold PA keys
	pakey = BATnew(TYPE_void, TYPE_lng, 1 << 16);
	if (pakey == NULL)
		throw(SQL, "neogeo.pa_grid_cell", MAL_MALLOC_FAIL);

	BATseqbase(gkey, 0);
	BATseqbase(pakey, 0);

	// create new BATs to hold return values
	for (i = 0; i < aggrCount; i++) {
		aggr_ret[i] = BATnew(TYPE_void, TYPE_lng, 1 << 16);	
		BATseqbase(aggr_ret[i], 0);
	}

	// insert PA keys into BATs
	while(next_pa_grid(grid)) {
		BUN b;
		if (grid->q.keyFlag == KD_BYTE_STRING) {
			b = BUNfnd(BATmirror(ckey_bat), (ptr) grid->cellByteKey);
		} else {
			b = BUNfnd(BATmirror(ckey_bat), &grid->cellKey);
		}

		if (b != BUN_NONE) {
			BUNappend(gkey, &grid->gridKey, FALSE);
			BUNappend(pakey, &grid->cellKey, FALSE);

			for (i = 0; i < aggrCount; i++) {
				BUNappend(aggr_ret[i], BUNtail(bat_iter[i], b), FALSE);
			}
		}

	}

	// free the grid
	free_pa_grid(grid); 

	// clean-up
	BBPreleaseref(ckey_bat->batCacheid);
	for(i =0; i < aggrCount; i++) {
		BBPreleaseref(aggr_bat[i]->batCacheid);
	}

	// return a reference to the new BATs containing the gkey and pakey
	*rgkey = gkey->batCacheid;
	*rpakey = pakey->batCacheid;

	BBPkeepref(*rgkey);	
	BBPkeepref(*rpakey);	

	for (i = 0; i < aggrCount; i++) {
		int id = aggr_ret[i]->batCacheid;

		*(int *) getArgReference(stk, pci, i + 2) = id;
		BBPkeepref(id);
	}

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
		throw(ILLARG, "neogeo.pa_grid_cell", "illegal argument: must be a valid grid query!");
	}

	// create the PA grid
	grid = create_pa_grid(query);

	// create new BAT to hold PA keys
	pakey = BATnew(TYPE_void, TYPE_lng, 1 << 16);
	if (pakey == NULL)
		throw(SQL, "neogeo.pa_grid_cell", MAL_MALLOC_FAIL);

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


