#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// #define MYALLOC(X)  malloc(X);fprintf(stdout,"MALLOC()\n") 
#define MYALLOC(X)  malloc(X)
// #define MYFREE(X)   free(X);fprintf(stdout,"FREE()\n");
#define MYFREE(X)   free(X)

#define MYDEBUG 1

#ifdef MYDEBUG
#define QVERBOSE 0
#endif

#include "pa_grid.template"

int main() {
  int i, count, testmulti;

  fprintf(stdout,"stairwalker 2 C translation experiment\n");
  
  if ( 0 ) {
    pmut PR;
    
    pmut_init(&PR,3);
    pmut_setRange(&PR,0,0,5);
    pmut_setRange(&PR,1,11,14);
    pmut_setRange(&PR,2,101,104);
    pmut_start(&PR);
    while ( pmut_next(&PR) ) {
      fprintf(stdout,"PERM(%d,%d,%d)\n",pmut_permutation(&PR,0),pmut_permutation(&PR,1),pmut_permutation(&PR,2));
    }
    exit(0);
  }

  testmulti = 1;
  for(i=0; i<testmulti; i++) {
    pa_grid* grid = create_pa_grid(
	"#G|X|3|2|4,10,61,153,10|4,9,154,108,10|public.london_hav_neogeo|public.london_hav_neogeo_btree|"
	// "#G|X|3|2|4,10,-180,40,20|4,9,-127,30,20|public.london_hav_neogeo|public.london_hav_neogeo_btree|"
	);
    fprintf(stdout,"+ GRID Result={\n");
    count = 0;
    while ( next_pa_grid(grid) ) {
      count++;
      // fprintf(stdout,"\t[x=%d,y=%d](%ld,\t%ld)\n",(int)(grid->gkey%5),(int)(grid->gkey/5),grid->gkey,grid->akey);
      // fprintf(stdout,"\t(%ld,\t%ld)\n",grid->gridKey,grid->cellKey);
      // fprintf(stdout,"%ld\n",grid->cellKey);
    }
    fprintf(stdout,"}[count=%d]\n",count);
    free_pa_grid(grid);
  }
  return(0);
}
