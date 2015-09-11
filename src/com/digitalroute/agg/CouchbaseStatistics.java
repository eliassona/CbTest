package com.digitalroute.agg;


public interface CouchbaseStatistics {
    public void countTotalNrOfCreates();
    public void countTotalNrOfReads();
    public void countTotalNrOfUpdates();
    public void countTotalNrOfDeletes();
    public void countTotalNrOfFailedCreates();
    public void countTotalNrOfFailedReads();
    public void countTotalNrOfFailedUpdates();
    public void countTotalNrOfFailedDeletes();
    public void countSessionTimeout();
    public void countAttemptedTimeout();
    public void countSessionRemove();
    public void updateSlowestCreate(final long time);
    public void updateSlowestRead(final long time);
    public void updateSlowestUpdate(final long time);
    public void updateSlowestDelete(final long time);
    public void countSessionCreate();
    public void measureTimeoutLatency(final long latency);
    public void measureMirrorLatency(final long latency);
    public void countMirrorFound();
    public void countMirrorNotFound();
    public void countMirrorError();
    public void countMirrorAttempt();
    public void incNrOfOutstandingConsumes();
    public void decNrOfOutstandingConsumes();
    public long getOutstandingConsumes();
    public void reset();
}
