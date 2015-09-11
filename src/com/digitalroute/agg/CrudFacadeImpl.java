package com.digitalroute.agg;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import rx.Observable;

import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.view.AsyncViewResult;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewQuery;

public final class CrudFacadeImpl implements CrudFacade {
	private final CouchbaseStatistics statistics;
	private final AsyncBucket bucket;
	private final int nrOfRetries;
	private final long sleepBtwRetries;
	private final PersistTo persistTo;
	private final ReplicateTo replicateTo;
	private final String viewName;
	private final Stale stale;
	private final Executor threadpool;
	private final Tuple2<Long, TimeUnit> operationTimeout;
	
	public CrudFacadeImpl(final Bucket bucket, 
						  final CouchbaseStatistics statistics, 
						  final int nrOfRetries, 
						  final long sleepBtwRetries, 
						  final int persistTo, 
						  final int replicateTo, 
						  final String viewName, 
						  final Stale stale, 
						  final Executor threadpool, 
						  final Tuple2<Long, TimeUnit> operationTimeout) {
		this.bucket = bucket.async();
		this.statistics = statistics;
		this.nrOfRetries = nrOfRetries;
		this.sleepBtwRetries = sleepBtwRetries;
		this.persistTo = persistToOf(persistTo);
		this.replicateTo = replicateToOf(replicateTo);
		this.viewName = viewName;
		this.stale = stale;
		this.threadpool = threadpool;
		this.operationTimeout = operationTimeout;
	}

	@Override
	public int nrOfRetries() {
		return nrOfRetries; 
	}

	@Override
	public long sleepBtwRetries() {
		return sleepBtwRetries;
	}

	@Override
	public CouchbaseStatistics statistics() {
		return statistics;
	}

	@Override
	public long currentTimeInMs() {
		return System.currentTimeMillis();
	}
	@Override
	public PersistTo persistTo() {
		return persistTo;
	}
	@Override
	public ReplicateTo replicateTo() {
		return replicateTo;
	}
	@Override
	public String viewName() {
		return viewName;
	}
	@Override
	public Stale stale() {
		return stale;
	}
	@Override
	public boolean useThreadPool() {
		return threadpool != null;
	}
	@Override
	public void execute(final Runnable task) {
		threadpool.execute(task);
	}
	@Override
	public Tuple2<Long, TimeUnit> operationTimeout() {
		return operationTimeout;
	}
	
	public static PersistTo persistToOf(final int persistTo) {
		switch (persistTo) {
		case 0: return PersistTo.NONE;
		case 1: return PersistTo.ONE;
		case 2: return PersistTo.TWO;
		case 3: return PersistTo.THREE;
		default: return PersistTo.FOUR;
		}
	}



	public static ReplicateTo replicateToOf(final int replicateTo) {
		switch (replicateTo) {
		case 0: return ReplicateTo.NONE;
		case 1: return ReplicateTo.ONE;
		case 2: return ReplicateTo.TWO;
		default: return ReplicateTo.THREE;
		}
	}
	@Override
	public Observable<AsyncViewResult> query(final ViewQuery query) {
		return bucket.query(query);
	}

	@Override
	public Observable<RawJsonDocument> insert(final RawJsonDocument doc, final PersistTo persistTo, final ReplicateTo replicateTo) {
		return bucket.insert(doc, persistTo, replicateTo);
	}

	@Override
	public Observable<RawJsonDocument> getAndLock(final RawJsonDocument doc, final int lockTimeout) {
		return bucket.getAndLock(doc, lockTimeout);
	}

	@Override
	public Observable<RawJsonDocument> get(final RawJsonDocument doc) {
		return bucket.get(doc);
	}

	@Override
	public Observable<RawJsonDocument> replace(final RawJsonDocument doc, final PersistTo persistTo, final ReplicateTo replicateTo) {
		return bucket.replace(doc, persistTo, replicateTo);
	}

	@Override
	public Observable<RawJsonDocument> remove(final RawJsonDocument doc) {
		return bucket.remove(doc);
	}

	@Override
	public Observable<Boolean> unlock(final String id, final long cas) {
		return bucket.unlock(id, cas);
	}
}


