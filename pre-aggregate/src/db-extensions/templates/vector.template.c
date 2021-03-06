typedef struct VECTORNAME {
  int size;
  int max_size;
  VECTORTYPE* values;
} VECTORNAME;

static VECTORNAME* VFUN(createVector)(int max_size) {
  VECTORNAME* v = (VECTORNAME*)MYALLOC(sizeof(VECTORNAME));

  if ( !v )
  	return NULL;
  v->size = 0;
  v->max_size = max_size;
  v->values  = (VECTORTYPE*)MYALLOC(v->max_size*sizeof(VECTORTYPE));
  return v;
}

static void VFUN(resetVector)(VECTORNAME* v) {
  v->size = 0;
}

static void VFUN(freeVector)(VECTORNAME* v) {
  MYFREE(v->values);
  MYFREE(v);
}

static void VFUN(add2Vector)(VECTORNAME* v, VECTORTYPE val) {
  if ( (v->size+1) >= v->max_size ) {
    int i;
    VECTORTYPE *new_values;

    v->max_size *= 2;
    new_values = (VECTORTYPE*)MYALLOC(v->max_size*sizeof(VECTORTYPE));
    for(i=0; i<v->size; i++)
  	new_values[i] = v->values[i];
    MYFREE(v->values);
    v->values = new_values;
  }
  v->values[v->size++] = val;
}

static VECTORTYPE VFUN(vector_get)(VECTORNAME* v, int i) {
  return v->values[i];
}

static int VFUN(vector_size)(VECTORNAME* v) {
  return v->size;
}
