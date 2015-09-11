package com.digitalroute.agg;

import rx.Observable;

import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.view.AsyncViewResult;

/**
 * Facade to couchbase
 * @author anderse
 *
 */
public interface CbFacade {
	Observable<RawJsonDocument> create(RawJsonDocument doc);
	Observable<RawJsonDocument> readAndLock(String id);
	Observable<RawJsonDocument> read(String id);
	
	Observable<RawJsonDocument> update(RawJsonDocument session);
	
	Observable<RawJsonDocument> delete(RawJsonDocument session);
	
	Observable<Boolean> unlock(String id, long cas);

	Observable<AsyncViewResult> queryTimeoutView(long cutOffTime);

	boolean isReadOnly();


}
