/*-------------------------------------------------------------------------
 *
 * nodeSeqscan.c
 *	  Support routines for sequential scans of relations.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeSeqscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecSeqScan				sequentially scans a relation.
 *		ExecSeqNext				retrieve next tuple in sequential order.
 *		ExecInitSeqScan			creates and initializes a seqscan node.
 *		ExecEndSeqScan			releases any storage allocated.
 *		ExecReScanSeqScan		rescans the relation
 *
 *		ExecSeqScanEstimate		estimates DSM space needed for parallel scan
 *		ExecSeqScanInitializeDSM initialize DSM for parallel scan
 *		ExecSeqScanReInitializeDSM reinitialize DSM for fresh parallel scan
 *		ExecSeqScanInitializeWorker attach to DSM info in parallel worker
 */
#include "postgres.h"

#include "access/relscan.h"
#include "access/tableam.h"
#include "executor/execdebug.h"
#include "executor/nodeSeqscan.h"
#include "utils/rel.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "postmaster/bgworker.h"
#include "executor/spi.h"
#include "utils/lsyscache.h"
#include "catalog/pg_type.h"
#include "catalog/pg_class.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/interrupt.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
#include "access/xact.h"
#include "utils/snapmgr.h"
#include "optimizer/optimizer.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_am.h"
#include "access/nbtree.h"
#include <math.h>

#define MAX_AUTOINDEX_STATS 100
#define MAX_AUTOINDEX_DBS 10

static AutoIndexStat *autoIndexStats = NULL;
static AutoIndexDB *autoIndexDBs = NULL;

static bool
is_column_indexed(Relation rel, AttrNumber attno)
{
	List *indexoidlist;
	ListCell *lc;
	bool found = false;

	indexoidlist = RelationGetIndexList(rel);

	foreach(lc, indexoidlist)
	{
		Oid indexoid = lfirst_oid(lc);
		Relation indexRel = index_open(indexoid, AccessShareLock);
		
		if (indexRel->rd_rel->relam == BTREE_AM_OID)
		{
			for (int i = 0; i < indexRel->rd_index->indnatts; i++)
			{
				if (indexRel->rd_index->indkey.values[i] == attno)
				{
					found = true;
					break;
				}
			}
		}
		index_close(indexRel, AccessShareLock);
		if (found) break;
	}
	return found;
}

Size AutoIndexShmemSize(void) {
	Size size = 0;
	size = add_size(size, mul_size(MAX_AUTOINDEX_STATS, sizeof(AutoIndexStat)));
	size = add_size(size, mul_size(MAX_AUTOINDEX_DBS, sizeof(AutoIndexDB)));
	return size;
}

void AutoIndexShmemInit(void) {
	bool found1, found2;
	autoIndexStats = (AutoIndexStat *) ShmemInitStruct("AutoIndex Stats", mul_size(MAX_AUTOINDEX_STATS, sizeof(AutoIndexStat)), &found1);
	autoIndexDBs = (AutoIndexDB *) ShmemInitStruct("AutoIndex DBs", mul_size(MAX_AUTOINDEX_DBS, sizeof(AutoIndexDB)), &found2);
	
	if (!found1) {
		MemSet(autoIndexStats, 0, mul_size(MAX_AUTOINDEX_STATS, sizeof(AutoIndexStat)));
	}
	if (!found2) {
		MemSet(autoIndexDBs, 0, mul_size(MAX_AUTOINDEX_DBS, sizeof(AutoIndexDB)));
	}
}

void AutoIndexMain(Datum main_arg) {
	Oid dbid = DatumGetObjectId(main_arg);

	BackgroundWorkerInitializeConnectionByOid(dbid, InvalidOid, 0);

	while (!ShutdownRequestPending) {
		WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 5000L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
		for (int i = 0; i < MAX_AUTOINDEX_STATS; i++) {
			if (autoIndexStats[i].relid != InvalidOid) {
					Oid relid = autoIndexStats[i].relid;
					AttrNumber attno = autoIndexStats[i].attno;
					double total_benefit = autoIndexStats[i].total_benefit;
					int query_count = autoIndexStats[i].query_count;
					int write_count = autoIndexStats[i].write_count;
					double rel_pages = autoIndexStats[i].rel_pages;

					double write_penalty;
					double maintenance_cost;
					
					if (rel_pages <= 0.0) rel_pages = 10.0;
					write_penalty = log(rel_pages + 1) / log(50.0);
					if (write_penalty < 1.0) write_penalty = 1.0;
					maintenance_cost = write_count * write_penalty;

					if (total_benefit > maintenance_cost) {
						elog(LOG, "AutoIndex: Creating index on rel=%u att=%d", relid, attno);
						autoIndexStats[i].query_count = 0;
						autoIndexStats[i].write_count = 0;
						autoIndexStats[i].total_benefit = 0.0;

						LWLockRelease(AddinShmemInitLock);

						PG_TRY();
						{
							SetCurrentStatementStartTimestamp();
							StartTransactionCommand();
							SPI_connect();
							PushActiveSnapshot(GetTransactionSnapshot());

							{
								char *relname = get_rel_name(relid);
								char *attname = get_attname(relid, attno, false);

								if (relname && attname) {
									char query[256];
									snprintf(query, sizeof(query), "CREATE INDEX IF NOT EXISTS auto_idx_%s_%s ON %s (%s)", relname, attname, relname, attname);
									pgstat_report_activity(STATE_RUNNING, query);
									SPI_execute(query, false, 0);
								}
							}

							SPI_finish();
							PopActiveSnapshot();
							CommitTransactionCommand();
							pgstat_report_activity(STATE_IDLE, NULL);
						}
						PG_CATCH();
						{
							/* Cleanup on error */
							EmitErrorReport();
							FlushErrorState();
							SPI_finish();
							PopActiveSnapshot();
							AbortCurrentTransaction();
						}
						PG_END_TRY();

						LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
				}
			}
		}
		LWLockRelease(AddinShmemInitLock);
	}
	proc_exit(0);
}

static TupleTableSlot *SeqNext(SeqScanState *node);

/* ----------------------------------------------------------------
 *		track_autoindex_write
 * ----------------------------------------------------------------
 */
void track_autoindex_write(Oid relid) {
	if (!autoIndexStats) return;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	for (int i = 0; i < MAX_AUTOINDEX_STATS; i++) {
		if (autoIndexStats[i].relid == relid) {
			autoIndexStats[i].write_count++;
		}
	}
	LWLockRelease(AddinShmemInitLock);
}

/* ----------------------------------------------------------------
 *		SeqNext
 *
 *		This is a workhorse for ExecSeqScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
SeqNext(SeqScanState *node)
{
	TableScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;

	/*
	 * get information from the estate and scan state
	 */
	scandesc = node->ss.ss_currentScanDesc;
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	slot = node->ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		/*
		 * We reach here if the scan is not parallel, or if we're serially
		 * executing a scan that was planned to be parallel.
		 */
		scandesc = table_beginscan(node->ss.ss_currentRelation,
								   estate->es_snapshot,
								   0, NULL);
		node->ss.ss_currentScanDesc = scandesc;
	}

	/*
	 * get the next tuple from the table
	 */
	if (table_scan_getnextslot(scandesc, direction, slot))
		return slot;
	return NULL;
}

/*
 * SeqRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
SeqRecheck(SeqScanState *node, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, SeqScan never use keys in heap_beginscan
	 * (and this is very bad) - so, here we do not check are keys ok or not.
	 */
	return true;
}

/* ----------------------------------------------------------------
 *		ExecSeqScan(node)
 *
 *		Scans the relation sequentially and returns the next qualifying
 *		tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecSeqScan(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) SeqNext,
					(ExecScanRecheckMtd) SeqRecheck);
}


/* ----------------------------------------------------------------
 *		ExecInitSeqScan
 * ----------------------------------------------------------------
 */
SeqScanState *
ExecInitSeqScan(SeqScan *node, EState *estate, int eflags)
{
	SeqScanState *scanstate;

	/*
	 * Once upon a time it was possible to have an outerPlan of a SeqScan, but
	 * not any more.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	scanstate = makeNode(SeqScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;
	scanstate->ss.ps.ExecProcNode = ExecSeqScan;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/*
	 * open the scan relation
	 */
	scanstate->ss.ss_currentRelation =
		ExecOpenScanRelation(estate,
							 node->scan.scanrelid,
							 eflags);

	/* and create slot with the appropriate rowtype */
	ExecInitScanTupleSlot(estate, &scanstate->ss,
						  RelationGetDescr(scanstate->ss.ss_currentRelation),
						  table_slot_callbacks(scanstate->ss.ss_currentRelation));

	/*
	 * Initialize result type and projection.
	 */
	ExecInitResultTypeTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.qual =
		ExecInitQual(node->scan.plan.qual, (PlanState *) scanstate);

	return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndSeqScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndSeqScan(SeqScanState *node)
{
	TableScanDesc scanDesc;

	/*
	 * get information from node
	 */
	scanDesc = node->ss.ss_currentScanDesc;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the tuple table
	 */
	if (node->ss.ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * close heap scan
	 */
	if (scanDesc != NULL)
		table_endscan(scanDesc);

	/* AutoIndex Tracking */
	if (node->ss.ps.plan && node->ss.ps.plan->qual) {
		ListCell *lc;
		foreach(lc, node->ss.ps.plan->qual) {
			Expr *expr = (Expr *) lfirst(lc);
			if (IsA(expr, OpExpr)) {
				OpExpr *op = (OpExpr *) expr;
				char *opname = get_opname(op->opno);
				if (opname && strcmp(opname, "=") == 0 && list_length(op->args) == 2) {
					Expr *arg1 = linitial(op->args);
					Expr *arg2 = lsecond(op->args);
					Var *var = NULL;
					if (IsA(arg1, Var) && IsA(arg2, Const)) {
						var = (Var *) arg1;
					} else if (IsA(arg2, Var) && IsA(arg1, Const)) {
						var = (Var *) arg2;
					}
					if (var && autoIndexStats && autoIndexDBs) {
						Relation rel = node->ss.ss_currentRelation;
						Oid relid = rel->rd_id;
						
						/* Skip system catalogs */
						if (rel->rd_rel->relnamespace == PG_CATALOG_NAMESPACE)
							continue;

						AttrNumber attno = var->varattno;
						
						double seq_scan_cost = node->ss.ps.plan->total_cost;
						double index_scan_cost = 0.0;
						double benefit = 0.0;
						double reltuples = node->ss.ss_currentRelation->rd_rel->reltuples;
						double plan_rows = node->ss.ps.plan->plan_rows;
						double indexTotalCost, descentCost, cpu_cost, heap_fetch_cost;
						int i;
						bool db_found = false;
						bool stat_found = false;

						if (reltuples <= 0.0) reltuples = 1000.0;
						if (plan_rows <= 0.0) plan_rows = 1.0;

						/* 1. Skip if already indexed */
						if (is_column_indexed(rel, attno))
							continue;

						/* 2. Selectivity Filter (Reject if > 5%) */
						if ((plan_rows / reltuples) > 0.05)
							continue;

						/* 
						 * Mathematical simulation of planner's B-Tree cost model:
						 * We use plan_rows to estimate how many tuples the index will fetch.
						 */
						
						/* 1. Disk Cost (estimated index pages touched, ~100 entries per page) */
						indexTotalCost = ceil(plan_rows / 100.0) * random_page_cost;

						/* 2. Index Descent CPU Cost: roughly log50(N) operator comparisons */
						descentCost = ceil(log(reltuples) / log(50.0)) * cpu_operator_cost;

						/* 3. Tuple Processing CPU Cost */
						cpu_cost = plan_rows * (cpu_index_tuple_cost + cpu_operator_cost);

						/* 4. Heap Fetch Cost (Random I/O for each fetched tuple) */
						heap_fetch_cost = plan_rows * random_page_cost + plan_rows * cpu_tuple_cost;

						/* Final estimated index cost */
						index_scan_cost = indexTotalCost + descentCost + cpu_cost + heap_fetch_cost;
						
						benefit = seq_scan_cost - index_scan_cost;
						elog(LOG, "AutoIndex EXEC: rel=%u att=%d benefit=%f rows=%f",
						     relid, attno, benefit, plan_rows);
						if (benefit < 0.0) benefit = 0.0;

						LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
						
						/* Ensure dynamic worker is launched once per DB */
						for (i = 0; i < MAX_AUTOINDEX_DBS; i++) {
							if (autoIndexDBs[i].dbid == MyDatabaseId) {
								db_found = true;
								break;
							}
						}
						if (!db_found) {
							for (i = 0; i < MAX_AUTOINDEX_DBS; i++) {
								if (autoIndexDBs[i].dbid == InvalidOid) {
									BackgroundWorker worker;
									autoIndexDBs[i].dbid = MyDatabaseId;
									autoIndexDBs[i].worker_launched = true;
									
									memset(&worker, 0, sizeof(worker));
									worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
									worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
									worker.bgw_restart_time = 5; /* Restart after 5s */
									sprintf(worker.bgw_library_name, "postgres");
									sprintf(worker.bgw_function_name, "AutoIndexMain");
									snprintf(worker.bgw_name, BGW_MAXLEN, "auto_indexer_worker");
									snprintf(worker.bgw_type, BGW_MAXLEN, "auto_indexer_worker");
									worker.bgw_notify_pid = 0;
									worker.bgw_main_arg = ObjectIdGetDatum(MyDatabaseId);

									RegisterDynamicBackgroundWorker(&worker, NULL);
									break;
								}
							}
						}

						for (i = 0; i < MAX_AUTOINDEX_STATS; i++) {
							if (autoIndexStats[i].relid == relid && autoIndexStats[i].attno == attno) {
								autoIndexStats[i].query_count++;
								autoIndexStats[i].total_benefit += benefit;
								autoIndexStats[i].rel_pages = node->ss.ss_currentRelation->rd_rel->relpages;
								stat_found = true;
								break;
							}
						}
						if (!stat_found) {
							for (i = 0; i < MAX_AUTOINDEX_STATS; i++) {
								if (autoIndexStats[i].relid == InvalidOid) {
									autoIndexStats[i].relid = relid;
									autoIndexStats[i].attno = attno;
									autoIndexStats[i].query_count = 1;
									autoIndexStats[i].total_benefit = benefit;
									autoIndexStats[i].rel_pages = node->ss.ss_currentRelation->rd_rel->relpages;
									break;
								}
							}
						}
						LWLockRelease(AddinShmemInitLock);
						/* Support multi-column: removed break */
					}
				}
			}
		}
	}
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecReScanSeqScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanSeqScan(SeqScanState *node)
{
	TableScanDesc scan;

	scan = node->ss.ss_currentScanDesc;

	if (scan != NULL)
		table_rescan(scan,		/* scan desc */
					 NULL);		/* new scan keys */

	ExecScanReScan((ScanState *) node);
}

/* ----------------------------------------------------------------
 *						Parallel Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecSeqScanEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanEstimate(SeqScanState *node,
					ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;

	node->pscan_len = table_parallelscan_estimate(node->ss.ss_currentRelation,
												  estate->es_snapshot);
	shm_toc_estimate_chunk(&pcxt->estimator, node->pscan_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanInitializeDSM
 *
 *		Set up a parallel heap scan descriptor.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanInitializeDSM(SeqScanState *node,
						 ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;
	ParallelTableScanDesc pscan;

	pscan = shm_toc_allocate(pcxt->toc, node->pscan_len);
	table_parallelscan_initialize(node->ss.ss_currentRelation,
								  pscan,
								  estate->es_snapshot);
	shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, pscan);
	node->ss.ss_currentScanDesc =
		table_beginscan_parallel(node->ss.ss_currentRelation, pscan);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanReInitializeDSM(SeqScanState *node,
						   ParallelContext *pcxt)
{
	ParallelTableScanDesc pscan;

	pscan = node->ss.ss_currentScanDesc->rs_parallel;
	table_parallelscan_reinitialize(node->ss.ss_currentRelation, pscan);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanInitializeWorker(SeqScanState *node,
							ParallelWorkerContext *pwcxt)
{
	ParallelTableScanDesc pscan;

	pscan = shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, false);
	node->ss.ss_currentScanDesc =
		table_beginscan_parallel(node->ss.ss_currentRelation, pscan);
}
