#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "fmgr.h"
#include "catalog/pg_type.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/tuplesort.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(median_finalfn_numeric);
PG_FUNCTION_INFO_V1(median_finalfn_double);
PG_FUNCTION_INFO_V1(median_numeric_transfn);
PG_FUNCTION_INFO_V1(median_int8_transfn);
PG_FUNCTION_INFO_V1(median_int4_transfn);
PG_FUNCTION_INFO_V1(median_int2_transfn);
PG_FUNCTION_INFO_V1(median_double_transfn);
PG_FUNCTION_INFO_V1(median_float_transfn);

/*
 * used as type of state variable median's function. It uses a 
 * tuplesort as safe and inteligent storage. But we cannot to use
 * a tuplesort under window aggregate context. 
 */
typedef struct
{
	int	nelems;		/* number of valid entries */
	Tuplesortstate *sortstate;
	bool	sorted;	/* true if the entries have been sorted */
	bool	free_tuplesort;	/* true if the tuplesort should be freed */
	FmgrInfo	cast_func_finfo;
	bool	datumTypByVal;
} MedianAggState;

static void
freeDatum(Datum value, bool typByVal, bool isNull)
{
	if (!typByVal && !isNull)
	{
		Pointer	s = DatumGetPointer(value);

		pfree(s);
	}
}

static MedianAggState *
makeMedianAggState(FunctionCallInfo fcinfo, Oid valtype, Oid targettype)
{
	MemoryContext oldctx;
	MemoryContext aggcontext;
	MedianAggState *aggstate;
	Oid	sortop,
			castfunc;
	CoercionPathType		pathtype;
	int16	typlen;
	bool		typbyval;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "median_transfn called in non-aggregate context");
	}

	oldctx = MemoryContextSwitchTo(aggcontext);

	aggstate = (MedianAggState *) palloc0(sizeof(MedianAggState));

	valtype = get_fn_expr_argtype(fcinfo->flinfo, 1);
	get_sort_group_operators(valtype,
						    true, false, false,
						    &sortop, NULL, NULL, NULL);

	/* lookup necessary attributies of the datum type, used for datumFree */
	get_typlenbyval(valtype, &typlen, &typbyval);
	aggstate->datumTypByVal = typbyval;

	/* initialize a tuplesort */
	aggstate->sortstate = tuplesort_begin_datum(valtype,
							sortop,
							SORTBY_NULLS_DEFAULT,
							false,
							work_mem, true);
	aggstate->sorted = false;
	aggstate->free_tuplesort = false; /* set in transfn, if necessary */

	MemoryContextSwitchTo(oldctx);

	if (valtype != targettype)
	{
		/* find a cast function */
		pathtype = find_coercion_pathway(targettype, valtype,
									COERCION_EXPLICIT,
									&castfunc);
		if (pathtype == COERCION_PATH_FUNC)
		{
			Assert(OidIsValid(castfunc));
			fmgr_info_cxt(castfunc, &aggstate->cast_func_finfo,
									    aggcontext);
		} 
		else if (pathtype == COERCION_PATH_RELABELTYPE)
		{
			aggstate->cast_func_finfo.fn_oid = InvalidOid;
		}
		else 
			elog(ERROR, "no conversion function from %s %s",
					 format_type_be(valtype),
					 format_type_be(targettype));
	}

	return aggstate;
}

/*
 *  append a non NULL value to tuplesort
 */
static Datum
common_median_transfn(FunctionCallInfo fcinfo, Oid typoid, Oid targetoid)
{
	MedianAggState *aggstate;

	aggstate = PG_ARGISNULL(0) ? NULL : (MedianAggState *) PG_GETARG_POINTER(0);

	if (!PG_ARGISNULL(1))
	{
		bool	is_running_median = false;

		if (aggstate == NULL)
			aggstate = makeMedianAggState(fcinfo, typoid, targetoid);

		if (aggstate->sorted)
		{
			/*
			 * We have already done a sort and computed a median value, so
			 * this must be a running median computation, over a WINDOW with
			 * an ORDER BY clause.
			 *
			 * Transfer all the data to a new tuplesort, so that we can sort
			 * it again in the final function. This is very inefficient, and
			 * should probably be replaced with a better algorithm one day.
			 *
			 * For now, this is the easiest solution. We might as well try
			 * to keep the new tuplesort in memory, since it will use up
			 * memory so slowly that it's unlikely to ever be a problem.
			 */
			MemoryContext aggcontext;
			MemoryContext oldctx;
			Oid		valtype;
			Oid		sortop;
			Tuplesortstate *sortstate;
			Datum	value;
			bool	isNull;

			if (!AggCheckCallContext(fcinfo, &aggcontext))
			{
				/* cannot be called directly because of internal-type argument */
				elog(ERROR, "median_transfn called in non-aggregate context");
			}

			oldctx = MemoryContextSwitchTo(aggcontext);

			valtype = get_fn_expr_argtype(fcinfo->flinfo, 1);
			get_sort_group_operators(valtype,
									 true, false, false,
									 &sortop, NULL, NULL, NULL);

			sortstate = tuplesort_begin_datum(valtype,
											  sortop,
											  SORTBY_NULLS_DEFAULT,
											  false,
											  2000000, true);

			MemoryContextSwitchTo(oldctx);

			tuplesort_rescan(aggstate->sortstate);
			while (tuplesort_getdatum(aggstate->sortstate, true, &value, &isNull))
			{
				tuplesort_putdatum(sortstate, value, isNull);
				freeDatum(value, aggstate->datumTypByVal, isNull);
			}

			tuplesort_end(aggstate->sortstate);

			aggstate->sortstate = sortstate;
			aggstate->sorted = false;
			is_running_median = true;
		}

		/*
		 * The final function should free the tuplesort unless this is a
		 * running median computation. If this is the first valid value,
		 * we don't actually know if it will be a running median, but it's
		 * safe to not free in that case anyway, since the tuplesort won't
		 * have used a temp file.
		 */
		aggstate->free_tuplesort = aggstate->nelems > 0 && !is_running_median;

		tuplesort_putdatum(aggstate->sortstate, PG_GETARG_DATUM(1), false);
		aggstate->nelems++;
	}

	PG_RETURN_POINTER(aggstate);
}


/*
 * just wrappers to be opr sanity checks happy
 */

Datum
median_numeric_transfn(PG_FUNCTION_ARGS)
{
	return common_median_transfn(fcinfo, 
						NUMERICOID, NUMERICOID);
}


Datum
median_int8_transfn(PG_FUNCTION_ARGS)
{
	return common_median_transfn(fcinfo, 
						INT8OID, NUMERICOID);
}


Datum
median_int4_transfn(PG_FUNCTION_ARGS)
{
	return common_median_transfn(fcinfo, 
						INT4OID, NUMERICOID);
}


Datum
median_int2_transfn(PG_FUNCTION_ARGS)
{
	return common_median_transfn(fcinfo,
						INT2OID, NUMERICOID);
}


Datum
median_double_transfn(PG_FUNCTION_ARGS)
{
	return common_median_transfn(fcinfo,
						FLOAT8OID, FLOAT8OID);
}


Datum
median_float_transfn(PG_FUNCTION_ARGS)
{
	return common_median_transfn(fcinfo,
						FLOAT4OID, FLOAT8OID);
}

static double 
to_double(Datum value, FmgrInfo *cast_func_finfo)
{
	if (cast_func_finfo->fn_oid != InvalidOid)
	{
		return DatumGetFloat8(FunctionCall1(cast_func_finfo, value));
	}
	else
		return DatumGetFloat8(value);
}

/*
 * Used as final function for median when result is double.
 */

Datum
median_finalfn_double(PG_FUNCTION_ARGS)
{
	MedianAggState *aggstate;

	Assert(AggCheckCallContext(fcinfo, NULL));

	aggstate = PG_ARGISNULL(0) ? NULL : (MedianAggState *) PG_GETARG_POINTER(0);

	if (aggstate != NULL)
	{
		int	lidx;
		int	hidx;
		Datum	   value;
		bool	isNull;
		int		i = 1;
		double	result = 0;

		hidx = aggstate->nelems / 2 + 1;
		lidx = (aggstate->nelems + 1) / 2;

		tuplesort_performsort(aggstate->sortstate);
		aggstate->sorted = true;

		while (tuplesort_getdatum(aggstate->sortstate, true, &value, &isNull))
		{
			if (i++ == lidx)
			{
				result = to_double(value, &aggstate->cast_func_finfo);
				freeDatum(value, aggstate->datumTypByVal, isNull);

				if (lidx != hidx)
				{
					tuplesort_getdatum(aggstate->sortstate, true, &value, &isNull);
					result = (result + to_double(value, &aggstate->cast_func_finfo)) / 2.0;
					freeDatum(value, aggstate->datumTypByVal, isNull);
				}
				break;
			}
		}

		if (aggstate->free_tuplesort)
			tuplesort_end(aggstate->sortstate);

		PG_RETURN_FLOAT8(result);
	}

	PG_RETURN_NULL();
}

/*
 * Used for reading values from tuplesort. The value has to be
 * Numeric or cast function is defined (and used).
 */
static Numeric
to_numeric(Datum value, FmgrInfo *cast_func_finfo)
{
	/* when valtype is same as target type, returns directly */
	if (cast_func_finfo->fn_oid == InvalidOid)
		return DatumGetNumeric(value);

	return DatumGetNumeric(FunctionCall1(cast_func_finfo, value));
}

/*
 * Used as final function for median when result is numeric.
 */

Datum
median_finalfn_numeric(PG_FUNCTION_ARGS)
{
	MedianAggState *aggstate;

	Assert(AggCheckCallContext(fcinfo, NULL));

	aggstate = PG_ARGISNULL(0) ? NULL : (MedianAggState *) PG_GETARG_POINTER(0);

	if (aggstate != NULL)
	{
		int	lidx;
		int	hidx;
		Datum	   a_value;
		bool	a_isNull;
		int		i = 1;
		Numeric result = NULL;		/* be compiler quiet */

		hidx = aggstate->nelems / 2 + 1;
		lidx = (aggstate->nelems + 1) / 2;

		tuplesort_performsort(aggstate->sortstate);
		aggstate->sorted = true;

		while (tuplesort_getdatum(aggstate->sortstate, true, &a_value, &a_isNull))
		{
			if (i++ == lidx)
			{
				result = to_numeric(a_value, &aggstate->cast_func_finfo);

				if (lidx != hidx)
				{
					Datum	b_value;
					bool	b_isNull;
					Numeric stack;

					tuplesort_getdatum(aggstate->sortstate, true, &b_value, &b_isNull);

					stack = to_numeric(b_value, &aggstate->cast_func_finfo);

					stack = DatumGetNumeric(DirectFunctionCall2(numeric_add,
												NumericGetDatum(stack),
												NumericGetDatum(result)));
					result = DatumGetNumeric(DirectFunctionCall2(numeric_div,
												NumericGetDatum(stack),
												DirectFunctionCall1(float4_numeric,
																    Float4GetDatum(2.0))));
					freeDatum(b_value, aggstate->datumTypByVal, b_isNull);
				}
				break;
			}
			else 
				freeDatum(a_value, aggstate->datumTypByVal, a_isNull);
		}

		if (aggstate->free_tuplesort)
			tuplesort_end(aggstate->sortstate);

		PG_RETURN_NUMERIC(result);
	}

	PG_RETURN_NULL();
}