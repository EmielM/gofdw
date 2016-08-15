#include "postgres.h"

#include "gofdw.h"

PG_MODULE_MAGIC;

extern Datum gofdw_handler(PG_FUNCTION_ARGS);
extern Datum gofdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(gofdw_handler);
Datum gofdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine* fdw;

	FdwInit();

	fdw = makeNode(FdwRoutine);
	fdw->GetForeignRelSize = FdwGetRelSize;
	fdw->GetForeignPaths = FdwGetPaths;
	fdw->GetForeignPlan = FdwGetPlan;
	fdw->BeginForeignScan = FdwBegin;
	fdw->IterateForeignScan = FdwIterate;
	fdw->ReScanForeignScan = FdwReScan;
	fdw->EndForeignScan = FdwEnd;

	PG_RETURN_POINTER(fdw);
}

PG_FUNCTION_INFO_V1(gofdw_validator);
Datum gofdw_validator(PG_FUNCTION_ARGS)
{
	FdwValidator(untransformRelOptions(PG_GETARG_DATUM(0)), PG_GETARG_OID(1));
	PG_RETURN_VOID();
}


