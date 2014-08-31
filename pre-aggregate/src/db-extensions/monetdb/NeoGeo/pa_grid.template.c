/*
 *
 * "C" template for implementation of pre aggregated cells
 *
 */

#define MYERROR(M)	{ fprintf(stderr,"%s\n",M); }

const char* HEXVAL = "0123456789ABCDEF";


/*
 *
 *
 * The Vector section
 *
 *
 */

#define VECTOR_DFLT_SIZE 8192

static void * epalloc(int size) {
    void* res;

    res = palloc(size);
    if ( ! res ) {
    	MYERROR("epalloc(): out of memory\n");
    }
    return res;
}

#define VECTORNAME VectorLong
#define VECTORTYPE long
#define VFUN(X)       X ## Long
#include "vector.template"

#undef VECTORNAME
#define VECTORNAME VectorVector
#undef VECTORTYPE
#define VECTORTYPE VectorLong*
#undef VFUN
#define VFUN(X)       X ## Vector
#include "vector.template"

#undef VECTORNAME
#define VECTORNAME VectorCharArray
#undef VECTORTYPE
#define VECTORTYPE char*
#undef VFUN
#define VFUN(X)       X ## CharArray
#include "vector.template"

/*
 * Some utility fun
 */

static long my_abs(long l) {
  return (l<0) ? -l : l;
}

static long my_log2(long num) {
  int cnt = 0;
  int v = 1;

  while ( v < num ) {
    cnt++;
    v *= 2;
  }
  return cnt;
}

static long my_pow(long base, long exp) {
  if ( exp < 1 )
    return 1;
  else {
    int i;
    long res = base;

    for(i=1; i<exp; i++) {
      res *= base;
    }
    return res;
  }
}

/*
 * the main data structures
 */

#define MAX_DIMENSION 6

typedef struct pmut {
    int dimensions;
    int range_low[MAX_DIMENSION];
    int range_high[MAX_DIMENSION];
    //
    int curDim;
    int permutation[MAX_DIMENSION];
} pmut;
    
static void pmut_setRange(pmut* pmut, int dim, int low, int high) {
    if ( low > high )
    	MYERROR("RANGE ERROR");
    pmut->range_low[dim]	= low;
    pmut->range_high[dim]	= high;
}

static void pmut_reset(pmut* pmut) {
    int i;

    for(i=0; i<pmut->dimensions; i++)
    	pmut_setRange(pmut, i,-1,-1);
}

static void pmut_init(pmut* pmut, int dimensions) {
    pmut->dimensions = dimensions;
    pmut_reset(pmut);
}
    
    
static void pmut_start(pmut* pmut) {
   int i;

   for(i=0; i<pmut->dimensions; i++) {
   	pmut->permutation[i] = pmut->range_low[i];
   }
   pmut->curDim = pmut->dimensions - 1;
   pmut->permutation[pmut->curDim]--;
}
    
static int pmut_next(pmut* pmut) {
    if ( ++pmut->permutation[pmut->curDim] >= pmut->range_high[pmut->curDim] ) {
    	while ( pmut->curDim >0 ) {
    	  pmut->permutation[pmut->curDim] = pmut->range_low[pmut->curDim];
    	  --pmut->curDim;
    	  if ( ++pmut->permutation[pmut->curDim] < pmut->range_high[pmut->curDim] ) {
    	    pmut->curDim = pmut->dimensions - 1;
    	    return 1;
    	  }
      } 
      pmut->curDim = -1;
      return 0;
    }
    return 1;
}
    
static int pmut_permutation(pmut* pmut, int i) {
    return pmut->permutation[i];
}
    
static int pmut_cardinality(pmut* pmut) {
    int i, res;

    res  = 1;
    for(i=0; i < pmut->dimensions; i++)
    	res *= (pmut->range_high[i] - pmut->range_low[i] );
    return res;
}

/*
 *
 */

typedef struct Ktype {
    int l[MAX_DIMENSION];
    int i[MAX_DIMENSION];
} Ktype;

// the data for every dimension
typedef struct pa_grid_def {
  int	N;
  int	bits;
  int   bytes;
  long	maxRange;
  //
  int start;
  int cellwidth;
  int gridsize;
} pa_grid_def;

#define MAX_NAME 64

// the complete query block
typedef struct pa_query {
  char         keyFlag;
  int          levelbits;
  int	       levelBytes;
  int		   totalBytes;
  int          dimensions; // current number of dimensions
  pa_grid_def  d[MAX_DIMENSION];
  char         table_name[MAX_NAME];
  char         btree_name[MAX_NAME];
} pa_query;

static void initQuery(pa_query* q, char keyFlag) {
    q->dimensions   = 0;
    q->keyFlag      = keyFlag;
    q->levelbits    = -1; // by default no subindexing
    q->levelBytes   = -1;
	q->totalBytes	= 0;
}

static void addQueryDimension(pa_query* q, int N, int bits, int start, int cellwidth, int gridsize) {
    pa_grid_def* d    = & q->d[q->dimensions++];
    d->N            = N;
    d->bits         = bits;
    d->maxRange	    = my_pow(2, bits) - 2;
    d->start        = start;
    d->cellwidth    = cellwidth;
    d->gridsize     = gridsize;

    // calculate nr. of bytes based on nr. of bits
    
    if (bits <= 8) {
		d->bytes = 1;
    } else if (bits <= 16) {
        d->bytes = 2; 
    } else if (bits <= 24) {
        d->bytes = 3; 
    } else if (bits <= 32) {
        d->bytes = 4; 
    } else if (bits <= 64) {
        d->bytes = 8; 
    } else {
        fprintf(stdout,"#! Dimension has too many bits!\n");
    }

	q->totalBytes = q->totalBytes + d->bytes + q->levelBytes;

    //fprintf(stdout, "#! Dimension bytes %d\n", d->bytes);
    

}

typedef struct pa_grid {
  pa_query    q; // a copy of the complete query block
  //
  int      end_of_grid;
  int      current_D[MAX_DIMENSION];
  int     current_stw_res;
  //
  VectorLong* stw_res;
  VectorCharArray* stw_res_byte;
  //
  VectorVector* stairs;
  //
  long      gridKey;
  long    cellKey;
  char*	  cellByteKey;
  //
  char       **values; // needed for printing
} pa_grid;


/*
 *
 * The lxy_...() section
 *
 */

#define indexMask    0x00FFFFFFFFFFFFFFL
#define levStart     56

static short li_l(long key) {
    return (short)(key >>levStart);
}
    
static int li_i(long key) {
    return (int)(key & indexMask);
}
    
static long li_key(short l, int i) {
    long key = (((long)l)<<levStart) | ((long)i);
        
    if ( l != li_l(key) )
        fprintf(stderr,"li_key, l mismatch");
    if ( i != li_i(key) )
        fprintf(stderr,"li_key, i mismatch");
    return key;
}

static char* li_toString(long key) {
    static char buff[512];
    sprintf(buff,"li_key[l=%d,i=%d]",li_l(key),li_i(key));
    return buff;
}
  
/*
 * Main stairwalk section
 */
static VectorLong* stairwalk(VectorLong *v, int from, int to, int N) {
    VectorLong *res;
    int level = 0;
    int step  = 1;
    int nowAt = from;

    if ( v ) {
      res = v;
      resetVectorLong(res);
    } else {
      if ( !(res = createVectorLong(VECTOR_DFLT_SIZE) ) )
      	return NULL;
    }
#ifdef MYDEBUG
    if (QVERBOSE)
      fprintf(stdout,"#! do stairwalk(from=%ld, to=%ld, N=%ld)\n",from,to,N);
#endif
    // first walk up the stairs
    while ((nowAt+step) <= to) {
      if ( (nowAt % (step*N) == 0) && ((nowAt+step*N)<=to) ) {
        // I can make a step up
        level++;
        step *= N;
      } else {
        long newKey = li_key(level,nowAt/step);
        add2VectorLong(res,newKey);
        nowAt += step;
#ifdef MYDEBUG
        if (QVERBOSE)
          fprintf(stdout,"+ @up(%s)\n",li_toString(newKey));
#endif
      }
    }
    level--; // go one level down
    step /= N; // decrease stepsize
    while ( (level>=0) && nowAt < to) {
      if ( (nowAt + step) >= (to+1)) {
        // step down;
        level--;
        step /= N;
      } else {
        // make a step
        long newKey = li_key(level,nowAt/step);
        add2VectorLong(res,newKey);
        nowAt += step;
#ifdef MYDEBUG
        if (QVERBOSE)
          fprintf(stdout,"+ @down(%s)\n",li_toString(newKey));
#endif
      }
    }
    return res;
}

static VectorLong* bounds_check_stairwalk(pa_grid* grid, int dim, VectorLong *v, long from, long to, long N) {
    VectorLong *res;

    // fprintf(stdout,"#! Doing bounds_check_stairwalk[D=%d](%ld,%ld,%ld)\n",dim,from,to,N);
    if ( from > to ) {
       MYERROR("from > to");
      // fprintf(stdout,"* bounds skip, from > to\n");
    }
    if ( from > grid->q.d[dim].maxRange ) {
      // cell totally out of bounds, code 2^n-1
      // fprintf(stdout,"* bounds skip: from(%d) > max(%d) \n",from,grid->q.d[dim].maxRange);
      return v;
    }
    if ( from < 0 ) {
      if ( to < 0 ) {
        // cell totally out of bounds, code 0
        // fprintf(stdout,"* bounds skip: [from,to] < 0\n");
	return v;
      }
      // adjust from lo lowest poss value
      // fprintf(stdout,"* adjust from %d -> %d\n",from,0);
      from = 0;
    }
    if ( to > grid->q.d[dim].maxRange ) {
      // from is valid here, so also make to valid
      // fprintf(stdout,"* adjust to %d -> %d\n",to,grid->q.d[dim].maxRange);
      to = grid->q.d[dim].maxRange;
    }
    if ( !(res = stairwalk(v,from,to,N)))
    	return NULL;
    return res;
}

/*
 *
 *
 */


#define LSWAP(X,Y) { tmp = *(X); *(X) = *(Y); *(Y) = tmp; }

#define PIVOT(I,J) ((I+J) /2)
 
static void l_quicksort(long list[],int m,int n)
{
   long tmp, key;
   int i,j,k;
   if( m < n)
   {
      k = PIVOT(m,n);
      LSWAP(&list[m],&list[k]);
      key = list[m];
      i = m+1;
      j = n;
      while(i <= j)
      {
         while((i <= n) && (list[i] <= key))                 
                i++;          
         while((j >= m) && (list[j] > key))
                j--;
         if( i < j)
                LSWAP(&list[i],&list[j]);
      }
      // swap two elements
      LSWAP(&list[m],&list[j]);
      // recursively sort the lesser list
      l_quicksort(list,m,j-1);
      l_quicksort(list,j+1,n);
   }
}

/*
 *
 *
 *
 */

static void resetK(Ktype* K) {
    int i;

    for(i=0; i<MAX_DIMENSION; i++) {
        K->l[i] = 0;
        K->i[i] = 0;
    }
}

static void copyK(Ktype* Kto, Ktype* Kfrom) {
    int i;

    for(i=0; i<MAX_DIMENSION; i++) {
        Kto->l[i] = Kfrom->l[i];
        Kto->i[i] = Kfrom->i[i];
    }
}

static void printK(Ktype* K) {
    int i;

    fprintf(stdout,"K[");
    for(i=0; i<MAX_DIMENSION; i++) {
    	fprintf(stdout,"l[%d]=%d ", K->l[i],K->i[i]);
    }
    fprintf(stdout,"]");
}

#define KD_REGULAR_LONG        'L'
#define KD_BYTE_STRING         'B'
#define KD_CROSSPRODUCT_LONG   'X'

static long regularLongKey(pa_grid* grid, Ktype *K) {
    int i;

    long res = 0;

    for(i=0; i<grid->q.dimensions; i++)
        // res = ( (res << kd.dimBits[i]) + i(i) );
        res = (res << grid->q.d[i].bits) + K->i[i];
    return res;
}

static long crossProductLongKey(pa_grid* grid, Ktype *K) {
    int i;
    long res = 0;

    for(i=0; i<grid->q.dimensions; i++) {
        res = (res << grid->q.levelbits) + K->l[i];
        res = (res << grid->q.d[i].bits) + ( K->i[i] + 1); //INCOMPLETE
    }
    return res;
}

static char* toHex(char* src, int len) {
	char* ret;

	ret = (char*)MYALLOC(len * 2 * sizeof(char));

	int i;
	for(i=0; i < len; i++) {
		ret[i*2] = HEXVAL[((src[i] >> 4) & 0xF)];
		ret[(i*2) + 1] = HEXVAL[(src[i]) & 0x0F];
	}

	ret[(len*2)] = '\0';
	ret[(len*2)+1] = '\0';

	return ret;
}

static char* byteStringKey(pa_grid* grid, Ktype *K) {
	char *key;

	//key =(char*)MYALLOC(grid->q.totalBytes * 2 * sizeof(char));
	key = (char*)MYALLOC(grid->q.totalBytes * sizeof(char));

	int idx = 0;
	int i;
	for(i=0; i<grid->q.dimensions; i++) {
		int level = K->l[i];
		int index = K->i[i];

		//fprintf(stdout, "i%d = %d, ", i, index);

		if (grid->q.levelBytes == 1) {
			key[idx++] = (char) level;
		} else if (grid->q.levelBytes == 2) {
			key[idx++] = (char)(level >> 8);
            key[idx++] = (char)(level);
		}

		if (grid->q.d[i].bytes == 4) {
			key[idx++] = (char)(index >> 24);
		}
		
		if (grid->q.d[i].bytes >= 3) {
			key[idx++] = (char)(index >> 16);
		}
		
		if (grid->q.d[i].bytes >= 2) {
			key[idx++] = (char)(index >> 8);
		}
		
		key[idx++] = (char)(index);
	}
	
	char *hexKey = toHex(key, grid->q.totalBytes);

	//fprintf(stdout, "%s\n", hexKey);

    return hexKey;
}

static void release_K(pa_grid* grid, Ktype *K) {
    long keyValue = 0;

    switch ( grid->q.keyFlag ) {
     case KD_REGULAR_LONG:
       // fprintf(stdout,"#! NOT IMPLEMENTED KD_REGULAR_LONG\n");
       keyValue = regularLongKey(grid,K);
       break;
     case KD_CROSSPRODUCT_LONG:
       keyValue = crossProductLongKey(grid,K);
       break;
     case KD_BYTE_STRING:
	   add2VectorCharArray(grid->stw_res_byte, byteStringKey(grid, K));
       break;
     default:
       fprintf(stdout,"#! UNEXPECTED keyFlag\n");
    }

    // printK(K); fprintf(stdout," = %ld\n",keyValue);
	if (keyValue != 0) {
		add2VectorLong(grid->stw_res, keyValue);
	}    
}

/*
 *
 *
 */

static int pacells_n(pa_grid* grid, int current_D[]) {
  int i;
  long lk;
  pmut PG;
  VectorLong *steps;
  Ktype K;

  if (grid->q.keyFlag == KD_BYTE_STRING) {
	  resetVectorCharArray(grid->stw_res_byte);
  } else {
	  resetVectorLong(grid->stw_res);
  }

  //
  pmut_init(&PG,grid->q.dimensions);
  for(i=0; i<grid->q.dimensions; i++) {
      int gp = current_D[i];
      steps = vector_getVector(grid->stairs,i);
      if ( !bounds_check_stairwalk(
          grid,
	  i,
          steps,
    	  grid->q.d[i].start + (gp * grid->q.d[i].cellwidth),
    	  grid->q.d[i].start + ((gp+1) * grid->q.d[i].cellwidth),
    	  grid->q.d[i].N) )
	  return 0;

      if ( vector_sizeLong(steps) > 0 )
      	pmut_setRange(&PG,i,0,vector_sizeLong(steps));
      else 
        return 1;
  }

  //
  pmut_start(&PG);
  int counter = 0;
  while ( pmut_next(&PG) ) {
    resetK(&K);
    for(i=0; i<grid->q.dimensions; i++) {
      steps = vector_getVector(grid->stairs,i);
      lk = vector_getLong(steps, pmut_permutation(&PG,i));
      K.l[i] = li_l(lk);
      K.i[i] = li_i(lk);
    }
    release_K(grid,&K);
	counter++;
  }

  fprintf(stdout, "Found %d keys\n", counter);

  if ( 0 ) { // takes 20ms for 95.000 keys
    // fprintf(stdout,"#! doing a quicksort!");
    l_quicksort(grid->stw_res->values, 0, grid->stw_res->size - 1);
  }

  return 1;
}

static int loadcell_pa_grid(pa_grid* grid, int current_D[]) {
  int i;

  grid->gridKey = 0;
  for(i=grid->q.dimensions-1; i>=0; i--) {	
      grid->gridKey <<= my_log2(grid->q.d[i].gridsize);
      grid->gridKey += current_D[i];
      // fprintf(stdout,"#### y_log2({%d} %ld, %ld)[%ld]\n",my_log2(grid->q.d[i].gridsize),current_D[i],grid->q.d[i].gridsize,grid->gridKey);
  }

  // fprintf(stdout,"#### gridKey = %ld\n",grid->gridKey);
  if (pacells_n(grid, current_D) == 0)
  	return 0;

  grid->current_stw_res = -1;
  return 1;
}

static int next_pa_grid(pa_grid* grid) {
  if ( !grid->end_of_grid ) {
	int resSize;

	if (grid->q.keyFlag == KD_BYTE_STRING) {
		resSize = vector_sizeCharArray(grid->stw_res_byte);
	} else {
		resSize = vector_sizeLong(grid->stw_res);
	}

    if ( ++grid->current_stw_res >= resSize ) {
      grid->cellKey = -1;

      // load the next cell
      if ( ++grid->current_D[0] >= grid->q.d[0].gridsize) {
        // end of row, take next
        if ( ++grid->current_D[1] >= grid->q.d[1].gridsize) {
			grid->end_of_grid = 1;
			return !grid->end_of_grid;
		} else {
			grid->current_D[0] = 0;
		}
      }

      if ( !loadcell_pa_grid(grid,grid->current_D) ) {
      	return -1;
	  }

      return next_pa_grid(grid);
    } else {
	  if (grid->q.keyFlag == KD_BYTE_STRING) {
		  grid->cellByteKey = vector_getCharArray(grid->stw_res_byte, grid->current_stw_res);
	  } else {
	      grid->cellKey = vector_getLong(grid->stw_res, grid->current_stw_res);
	  }
	}
  }

  return !grid->end_of_grid;
}

static int decode_paGridQuery(char* p, pa_query *q) {
    char *pn;
    int i, dimensions, ssize;

    if ( *p++ != '#' )
        return 0;

    if ( *p != 'G' ) // grid mode
        return 0;

    p = strchr((const char*)p,(int)'|') + 1;
    initQuery(q, *p);

    p = strchr((const char*)p,(int)'|') + 1;
    q->levelbits = atoi(p); 
    
    
    // calculate size of level in bytes
    if (q->levelbits <= 8) {
	q->levelBytes = 1;
    } else if (q->levelbits <= 16) {
	q->levelBytes = 2;
    }   

    p = strchr((const char*)p,(int)'|') + 1;
    dimensions = atoi(p);

    p = strchr((const char*)p,(int)'|') + 1;
    for(i=0; i<dimensions; i++) {
      int N, bits, start, width, gsize;
      N = atoi(p);p = strchr((const char*)p,(int)',') + 1;
      bits = atoi(p);p = strchr((const char*)p,(int)',') + 1;
      start = atoi(p);p = strchr((const char*)p,(int)',') + 1;
      width = atoi(p);p = strchr((const char*)p,(int)',') + 1;
      gsize = atoi(p);
      addQueryDimension(q,N,bits,start,width,gsize);
      //
      p = strchr((const char*)p,(int)'|') + 1;
    };

    pn = strchr((const char*)p,(int)'|');
    ssize = (pn - p);
    strncpy(q->table_name,p,ssize);
    q->table_name[ssize] = 0;

    p = pn + 1;
    pn = strchr((const char*)p,(int)'|');
    ssize = (pn - p);
    strncpy(q->btree_name,p,ssize);
    q->btree_name[ssize] = 0;

    return 1;
}

static pa_grid* create_pa_grid(char *p) {
  int i;
  pa_grid* grid        = (pa_grid*)MYALLOC(sizeof(pa_grid));

  if ( !grid )
  	return NULL;
  decode_paGridQuery(p, &grid->q);

  
  if (grid->q.keyFlag == KD_BYTE_STRING) {
	  if ( !(grid->stw_res_byte    = createVectorCharArray(VECTOR_DFLT_SIZE))) {
		  return NULL;
	  }
  } else {
	  if ( !(grid->stw_res    = createVectorLong(VECTOR_DFLT_SIZE))) {
		  return NULL;
	  } 
  }	

  if ( !(grid->stairs = createVectorVector(MAX_DIMENSION)))
  	return NULL;

  for(i=0; i<grid->q.dimensions; i++) {
        VectorLong* vl;

	if ( !(vl=createVectorLong(VECTOR_DFLT_SIZE)))
		return NULL;
        add2VectorVector(grid->stairs, vl);
  }

  if ( (grid->q.d[0].gridsize > 0) && (grid->q.d[1].gridsize > 0) ) {
    grid->end_of_grid = 0;
    for(i=0; i<grid->q.dimensions; i++)
        grid->current_D[i] = 0;
    loadcell_pa_grid(grid,grid->current_D);
  } else {
    grid->end_of_grid = 1; // terminate
  }

  grid->values = (char **) MYALLOC(2 * sizeof(char *));
  grid->values[0] = (char *) MYALLOC(64 * sizeof(char));
  grid->values[1] = (char *) MYALLOC(64 * sizeof(char));
  return grid;
}

static void free_pa_grid(pa_grid* grid) {
  int i;

  MYFREE(grid->values[0]);
  MYFREE(grid->values[1]);
  MYFREE(grid->values);

  if (grid->q.keyFlag == KD_BYTE_STRING) {
	  if ( grid->stw_res_byte ) {
		  freeVectorCharArray(grid->stw_res_byte);
	  }
  } else {
	  if ( grid->stw_res )
		freeVectorLong( grid->stw_res );
  }

  if ( grid->stairs ) {
    for(i=0; i<vector_sizeVector(grid->stairs); i++) {
        freeVectorLong( vector_getVector(grid->stairs,i));
    }
    freeVectorVector( grid->stairs );
  }
  MYFREE(grid);
}
