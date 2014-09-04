/*
 * Created by Dennis Pallett (dennis@pallett.nl)
 * July, 2014
 */

#ifndef _SQL_NEOGEO_H_
#define _SQL_NEOGEO_H_
#include "sql.h"
#include <string.h>

#ifdef WIN32
#ifndef LIBNEOGEO
#define neogeo_export extern __declspec(dllimport)
#else
#define neogeo_export extern __declspec(dllexport)
#endif
#else
#define neogeo_export extern
#endif

/* export MAL wrapper functions */

neogeo_export char * compute_pa_grid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
neogeo_export char * compute_pa_grid_enhanced(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
neogeo_export char * compute_pa_grid_cell(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

neogeo_export char * byte_to_hex_bigendian(char **ret, const int *num);
neogeo_export char * short_to_hex_bigendian(char **ret, const int *num);
neogeo_export char * int24_to_hex_bigendian(char **ret, const int *num);
neogeo_export char * int_to_hex_bigendian(char **ret, const int *num);

#endif /* _SQL_NEOGEO_H_ */
