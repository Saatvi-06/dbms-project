/*-------------------------------------------------------------------------
 *
 * nodeSeqscan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeSeqscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODESEQSCAN_H
#define NODESEQSCAN_H

#include "access/parallel.h"
#include "nodes/execnodes.h"

extern SeqScanState *ExecInitSeqScan(SeqScan *node, EState *estate, int eflags);
extern void ExecEndSeqScan(SeqScanState *node);
extern void ExecReScanSeqScan(SeqScanState *node);

/* parallel scan support */
extern void ExecSeqScanEstimate(SeqScanState *node, ParallelContext *pcxt);
extern void ExecSeqScanInitializeDSM(SeqScanState *node, ParallelContext *pcxt);
extern void ExecSeqScanReInitializeDSM(SeqScanState *node, ParallelContext *pcxt);
extern void ExecSeqScanInitializeWorker(SeqScanState *node,
										ParallelWorkerContext *pwcxt);

/* Automatic index creation tracker */
typedef struct {
	Oid relid;
	AttrNumber attno;
	double total_benefit;
	int query_count;
	int write_count;
	double rel_pages;
} AutoIndexStat;

typedef struct {
	Oid dbid;
	bool worker_launched;
} AutoIndexDB;

extern void AutoIndexShmemInit(void);
extern Size AutoIndexShmemSize(void);

extern void track_autoindex_write(Oid relid);

extern void AutoIndexMain(Datum main_arg) pg_attribute_noreturn();

#endif							/* NODESEQSCAN_H */
