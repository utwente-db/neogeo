#include "postgres.h"
#include <string.h>
#include "fmgr.h"
#include "funcapi.h"
#include "utils/geo_decls.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define MYALLOC(X)  palloc(X)
#define MYFREE(X)   pfree(X)

#include "pa_grid.template"

PG_FUNCTION_INFO_V1(compute_pa_grid);

Datum
compute_pa_grid(PG_FUNCTION_ARGS)
{
    FuncCallContext     *funcctx;
    TupleDesc            tupdesc;
    AttInMetadata       *attinmeta;
    pa_grid*		 grid;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext   oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /* switch to memory context appropriate for multiple function calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* Build a tuple descriptor for our result type */
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("function returning record called in context "
                            "that cannot accept type record")));

        /*
         * generate attribute metadata needed later to produce tuples from raw
         * C strings
         */
        attinmeta = TupleDescGetAttInMetadata(tupdesc);
        funcctx->attinmeta = attinmeta;

	/* 
	 * WARNING: there stil is a small change that memory is alllocated
	 *	    during the next calls. This could cause problems.
	 * My user context stuff, allocate all memory expected in this call.
	 * Other memory allocated outside this context will be thrown away
	 * after the first call.
	 */ 
	text  *arg1 = PG_GETARG_TEXT_P(0);
	// FILE* f = fopen("/tmp/LOG","a");
	// fprintf(f,"KEY=[%s]\n",VARDATA(arg1));
	// fclose(f);
	funcctx->user_fctx = create_pa_grid(VARDATA(arg1));
	grid = (pa_grid*)funcctx->user_fctx;

        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    grid = (pa_grid*)funcctx->user_fctx;

    attinmeta = funcctx->attinmeta;

    if ( next_pa_grid(grid) )
    {
        HeapTuple    tuple;
        Datum        result;

        snprintf(grid->values[0], 24, "%ld", grid->gridKey);
        snprintf(grid->values[1], 24, "%ld", grid->cellKey);

        /* build a tuple */
        tuple = BuildTupleFromCStrings(attinmeta, grid->values);

        /* make the tuple into a datum */
        result = HeapTupleGetDatum(tuple);

        SRF_RETURN_NEXT(funcctx, result);
    }
    else    /* do when there is no more left */
    {
        free_pa_grid(grid); // free the grid
	funcctx->user_fctx = NULL;
	//
        SRF_RETURN_DONE(funcctx);
    }
}

PG_FUNCTION_INFO_V1(compute_pa_grid_cell);

Datum
compute_pa_grid_cell(PG_FUNCTION_ARGS)
{
    FuncCallContext     *funcctx;
    TupleDesc            tupdesc;
    pa_grid*		 grid;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext   oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /* switch to memory context appropriate for multiple function calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* Build a tuple descriptor for our result type */
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_SCALAR)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("function returning record called in context "
                            "that cannot accept type record")));

	/* 
	 * WARNING: there stil is a small change that memory is alllocated
	 *	    during the next calls. This could cause problems.
	 * My user context stuff, allocate all memory expected in this call.
	 * Other memory allocated outside this context will be thrown away
	 * after the first call.
	 */ 
	text  *arg1 = PG_GETARG_TEXT_P(0);
	funcctx->user_fctx = create_pa_grid(VARDATA(arg1));
	grid = (pa_grid*)funcctx->user_fctx;

        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    grid = (pa_grid*)funcctx->user_fctx;

    if ( next_pa_grid(grid) )
    {
	Datum long_result = Int64GetDatum( grid->cellKey );

        SRF_RETURN_NEXT(funcctx, long_result);
    }
    else    /* do when there is no more left */
    {
        free_pa_grid(grid); // free the grid
	funcctx->user_fctx = NULL;
	//
        SRF_RETURN_DONE(funcctx);
    }

}
