package com.digitalroute.agg;

import java.util.concurrent.TimeUnit;

import rx.Observable;

import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.view.AsyncViewResult;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewQuery;

/**
 * Facade that the CrudOps functions use.
 * @author anderse
 *
 */
public interface CrudFacade {
//	Bucket bucket();
	int nrOfRetries();
	long sleepBtwRetries();
	CouchbaseStatistics statistics();
	long currentTimeInMs();
	PersistTo persistTo();
	ReplicateTo replicateTo();
	String viewName();
	Stale stale();
	boolean useThreadPool();
	void execute(Runnable task);
	Tuple2<Long, TimeUnit> operationTimeout();
	Observable<AsyncViewResult> query(ViewQuery query);
	Observable<RawJsonDocument> insert(RawJsonDocument doc, PersistTo persistTo, ReplicateTo replicateTo);
	Observable<RawJsonDocument> getAndLock(RawJsonDocument doc, int lockTimeout);
	Observable<RawJsonDocument> get(RawJsonDocument doc);
	Observable<RawJsonDocument> replace(RawJsonDocument doc, PersistTo persistTo, ReplicateTo replicateTo);
	Observable<RawJsonDocument> remove(RawJsonDocument doc);
	Observable<Boolean> unlock(String id, long cas);

}
