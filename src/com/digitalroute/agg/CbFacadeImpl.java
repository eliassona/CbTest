package com.digitalroute.agg;

import rx.Observable;

import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.view.AsyncViewResult;
import com.digitalroute.agg.CrudOps.AsyncQueryOp;
import com.digitalroute.agg.CrudOps.CreateOp;
import com.digitalroute.agg.CrudOps.DeleteOp;
import com.digitalroute.agg.CrudOps.ReadAndLockOp;
import com.digitalroute.agg.CrudOps.ReadOp;
import com.digitalroute.agg.CrudOps.UnlockOp;
import com.digitalroute.agg.CrudOps.UpdateOp;

public class CbFacadeImpl implements CbFacade {
	private final boolean readOnly;
	private final CrudFacade crudFacade;

	public CbFacadeImpl(final boolean readOnly, final CrudFacade crudFacade) {
		this.readOnly = readOnly;
		this.crudFacade = crudFacade;
		
	}
	
	@Override
	public boolean isReadOnly() {
		return readOnly;
	}

	@Override
	public Observable<RawJsonDocument> update(final RawJsonDocument session) {
		if (readOnly) {
			return Observable.just(session);
		}
		return UpdateOp.update(session, crudFacade);
	}

	@Override
	public Observable<RawJsonDocument> delete(final RawJsonDocument session) {
		if (readOnly) {
			return Observable.just(session);
		}
		return DeleteOp.delete(session, crudFacade);
	}

	@Override
	public Observable<RawJsonDocument> create(final RawJsonDocument session) {
		if (readOnly) {
			return Observable.just(session);
		}
		return CreateOp.create(session, crudFacade);
	}

	@Override
	public Observable<AsyncViewResult> queryTimeoutView(final long cutOffTime) {
		return AsyncQueryOp.query(cutOffTime, crudFacade);
	}

	@Override
	public Observable<RawJsonDocument> readAndLock(String id) {
		return ReadAndLockOp.readAndLock(id, 30, crudFacade);//TODO timeout
	}
	
	
	@Override
	public Observable<RawJsonDocument> read(String id) {
		return ReadOp.read(id, crudFacade);
	}

	

	@Override
	public Observable<Boolean> unlock(String id, long cas) {
		if (readOnly) {
			return Observable.just(true);
		}
		return UnlockOp.delete(id, cas, crudFacade);
	}

}
