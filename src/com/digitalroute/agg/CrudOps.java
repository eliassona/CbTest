package com.digitalroute.agg;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.lang.Tuple;
import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.CouchbaseOutOfMemoryException;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.RequestTooBigException;
import com.couchbase.client.java.error.ViewDoesNotExistException;
import com.couchbase.client.java.view.AsyncViewResult;
import com.couchbase.client.java.view.ViewQuery;


/**
 * This class is just a namespace for the inner static classes. 
 * The inner classes provide a layer on top of the Couchbase API
 * It adds some error handling and retry logic.
 * @author anderse
 *
 */
public class CrudOps {

	
	private static final Logger logger = Logger.getLogger(CrudOps.class.getName());
	
	
	public static class AsyncQueryOp extends CrudOp<AsyncViewResult> {

		final long cutOffTime;

		public static Observable<AsyncViewResult> query(final long cutOffTime, final CrudFacade facade) {
			return new AsyncQueryOp(cutOffTime, facade).execute();
		}
		
		public AsyncQueryOp(final long cutOffTime, final CrudFacade facade) {
			super(facade);
			this.cutOffTime = cutOffTime;
		}

		@Override
		void incFailed() {
		}

		@Override
		void updateSlowest(final long t) {
		}

		@Override
		void incSuccessful() {
		}

		@Override
		public Observable<AsyncViewResult> execute() {
			return _async();
		}
		@Override
		Observable<AsyncViewResult> _async() {
			return facade.query(ViewQuery.from(facade.viewName(), facade.viewName()).endKey(cutOffTime + "").stale(facade.stale()));
		}

		@Override
		boolean shouldThrowImediately(final Throwable e) {
			return isInstanceOf(e, ViewDoesNotExistException.class, CouchbaseException.class);
		}

	}

	
	public static class CreateOp extends CrudOp<RawJsonDocument> {

		final RawJsonDocument doc;

		public static Observable<RawJsonDocument> create(final RawJsonDocument doc, final CrudFacade facade) {
			return new CreateOp(doc, facade).execute();
		}
		
		public CreateOp(final RawJsonDocument doc, final CrudFacade facade) {
			super(facade);
			this.doc = doc;
		}

		@Override
		void incFailed() {
			facade.statistics().countTotalNrOfFailedCreates();
		}

		@Override
		void updateSlowest(final long t) {
			facade.statistics().updateSlowestCreate(t);
		}

		@Override
		void incSuccessful() {
			facade.statistics().countSessionCreate();
			facade.statistics().countTotalNrOfCreates();
		}

		@Override
		Observable<RawJsonDocument> _async() {
			return facade.insert(doc, facade.persistTo(), facade.replicateTo());
		}


		@Override
		boolean shouldThrowImediately(final Throwable e) {
			return isInstanceOf(e, 
					DocumentAlreadyExistsException.class,
					RequestTooBigException.class,
					CouchbaseOutOfMemoryException.class);
		}
		
		@Override
		void incFailedMim(final Throwable e) {
			if (e instanceof DocumentAlreadyExistsException) {
				incSuccessful();
			} else {
				incFailed();
			}
		}
		
	}

	public static class ReadAndLockOp extends CrudOp<RawJsonDocument> {

		final String id;
		final int lockTimeout;

		public static Observable<RawJsonDocument> readAndLock(final String id, final int lockTimeout, final CrudFacade facade) {
			return new ReadAndLockOp(id, lockTimeout, facade).execute();
		}
		
		
		public ReadAndLockOp(final String id, final int lockTimeout, final CrudFacade facade) {
			super(facade);
			this.id = id;
			this.lockTimeout = lockTimeout;
		}

		@Override
		void incFailed() {
			facade.statistics().countTotalNrOfFailedReads();
		}

		@Override
		void updateSlowest(final long t) {
			facade.statistics().updateSlowestRead(t);
		}

		@Override
		void incSuccessful() {
			facade.statistics().countTotalNrOfReads();
		}

		@Override
		Observable<RawJsonDocument> _async() {
			return facade.getAndLock(RawJsonDocument.create(id), lockTimeout).defaultIfEmpty(null);
		}


		@Override
		boolean shouldThrowImediately(final Throwable e) {
			return isInstanceOf(e, NoSuchElementException.class, CouchbaseOutOfMemoryException.class, DocumentDoesNotExistException.class);
		}


	}
	public static class ReadOp extends CrudOp<RawJsonDocument> {

		final String id;

		public static Observable<RawJsonDocument> read(final String id, final CrudFacade facade) {
			return new ReadOp(id, facade).execute();
		}
		
		
		public ReadOp(final String id, final CrudFacade facade) {
			super(facade);
			this.id = id;
		}

		@Override
		void incFailed() {
			facade.statistics().countTotalNrOfFailedReads();
		}

		@Override
		void updateSlowest(final long t) {
			facade.statistics().updateSlowestRead(t);
		}

		@Override
		void incSuccessful() {
			facade.statistics().countTotalNrOfReads();
		}

		@Override
		Observable<RawJsonDocument> _async() {
			return facade.get(RawJsonDocument.create(id)).defaultIfEmpty(null);
		}


		@Override
		boolean shouldThrowImediately(final Throwable e) {
			return isInstanceOf(e, CouchbaseOutOfMemoryException.class, CouchbaseException.class);
		}

	}

	public static class UpdateOp extends CrudOp<RawJsonDocument> {

		final RawJsonDocument doc;

		public static Observable<RawJsonDocument> update(final RawJsonDocument doc, final CrudFacade facade) {
			return new UpdateOp(doc, facade).execute();
		}
		public UpdateOp(final RawJsonDocument doc, final CrudFacade facade) {
			super(facade);
			this.doc = doc;
		}

		@Override
		void incFailed() {
			facade.statistics().countTotalNrOfFailedUpdates();
		}
		
		/**
		 * Don't increment failed for CAS mismatch
		 * @param e
		 */
		@Override void incFailedMim(final Throwable e) {
			if (e instanceof CASMismatchException) {
				incSuccessful();
			} else {
				incFailed();
			}
		}
		
		@Override
		void updateSlowest(final long t) {
			facade.statistics().updateSlowestUpdate(t);
		}

		@Override
		void incSuccessful() {
			facade.statistics().countTotalNrOfUpdates();
		}


		@Override
		Observable<RawJsonDocument> _async() {
			return facade.replace(doc, facade.persistTo(), facade.replicateTo()).defaultIfEmpty(null);
		}
		
		@Override
		boolean shouldThrowImediately(final Throwable e) {
			return isInstanceOf(e, CASMismatchException.class, RequestTooBigException.class, CouchbaseOutOfMemoryException.class, CouchbaseException.class);
		}

	}


	public static class DeleteOp extends CrudOp<RawJsonDocument> {

		final RawJsonDocument doc;

		public static Observable<RawJsonDocument> delete(final RawJsonDocument doc, final CrudFacade facade) {
			return new DeleteOp(doc, facade).execute();
		}

		public DeleteOp(final RawJsonDocument doc, final CrudFacade facade) {
			super(facade);
			this.doc = doc;
		}

		@Override
		void incFailed() {
			facade.statistics().countTotalNrOfFailedDeletes();
		}

		@Override
		void updateSlowest(final long t) {
			facade.statistics().updateSlowestDelete(t);
		}

		@Override
		void incSuccessful() {
			facade.statistics().countSessionRemove();
			facade.statistics().countTotalNrOfDeletes();
		}

		@Override
		Observable<RawJsonDocument> _async() {
			return facade.remove(doc).defaultIfEmpty(null);
		}
		
		@Override
		boolean shouldThrowImediately(final Throwable e) {
			return isInstanceOf(e, CASMismatchException.class, CouchbaseOutOfMemoryException.class, CouchbaseException.class);
		}

	}
	
	
	public static class UnlockOp extends CrudOp<Boolean> {

		final String id;
		final long cas;

		public static Observable<Boolean> delete(final String id, final long cas, final CrudFacade facade) {
			return new UnlockOp(id, cas, facade).execute();
		}

		
		public UnlockOp(final String id, final long cas, final CrudFacade facade) {
			super(facade);
			this.id = id;
			this.cas = cas;
		}

		@Override
		void incFailed() {
			facade.statistics().countTotalNrOfFailedUpdates();
		}

		@Override
		void updateSlowest(final long t) {
			facade.statistics().updateSlowestUpdate(t);
		}

		@Override
		void incSuccessful() {
			facade.statistics().countTotalNrOfUpdates();
		}

		@Override
		Observable<Boolean> _async() {
			return facade.unlock(id, cas);
		}
		
		@Override
		boolean shouldThrowImediately(final Throwable e) {
			return isInstanceOf(e, /*TemporaryLockFailureException.class, */RequestTooBigException.class, CouchbaseOutOfMemoryException.class, CouchbaseException.class);
		}

		
	}

	public static abstract class CrudOp<T> {
		final CrudFacade facade;
		public CrudOp(final CrudFacade facade) {
			this.facade = facade;
		}
		public Observable<T> execute() {
			final long startTime = facade.currentTimeInMs();
			if (facade.nrOfRetries() <= 0) {
				return _execute(startTime);
			} else {
				return _execute(startTime).
						retryWhen(retryFn());
			}
		}
		private Observable<T> _execute(final long startTime) {
			return Observable.defer(new Func0<Observable<T>>() {
				@Override
				public Observable<T> call() {
					return _async().
							timeout(facade.operationTimeout().value1(), facade.operationTimeout().value2()).
							map(new Func1<T, T>() {
								@Override
								public T call(T t1) {
									updateMims(startTime);
									return t1;
								}
							});
				}
			});
		}
		
		private Func1<? super Observable<? extends Throwable>, ? extends Observable<?>> retryFn() {
			return new Func1<Observable<? extends Throwable>, Observable<?>>() {

				@Override
				public Observable<?> call(final Observable<? extends Throwable> t1) {
					return t1.zipWith(Observable.range(1, facade.nrOfRetries()), new Func2<Throwable, Integer, Tuple2<Throwable, Integer>>() {
		                @Override
		                public Tuple2<Throwable, Integer> call(final Throwable throwable, final Integer attempts) {
		                    return Tuple.create(throwable, attempts);
		                }
		            })
		            .flatMap(retryOrFail());				
				}

			};
		}

		
		public static class NrOfRetriesExceededException extends RuntimeException {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1134730390551774483L;
			public NrOfRetriesExceededException(final Throwable e) {
				super(e);
			}
			
		}
		
		int expRetryDelayInMs(final int attempt) {
			return Math.min(1000, (int)Math.pow(2, attempt));
		}
		private Func1<Tuple2<Throwable, Integer>, Observable<?>> retryOrFail() {
			return new Func1<Tuple2<Throwable, Integer>, Observable<?>>() {
                @Override
                public Observable<?> call(final Tuple2<Throwable, Integer> attempt) {
                	incFailedMim(attempt.value1());
                	logger.info(attempt.toString());
            		if (shouldThrowImediately(attempt.value1())) {
            			logger.info("Return imediately");
            			return Observable.error(attempt.value1());
            		}
            		if (attempt.value2() == facade.nrOfRetries()) {
            			logger.info("nr of retries exceeded, throwing exception");
            			return Observable.error(new NrOfRetriesExceededException(attempt.value1()));
            		}
                    return Observable.timer(expRetryDelayInMs(attempt.value2()), TimeUnit.MILLISECONDS);
                }
            };
		}
		
		public Action1<? super Object> updateMims(final long startTime) {
			return new Action1<Object>() {
				@Override
				public void call(final Object t1) {
					incSuccessful();
					updateSlowest(facade.currentTimeInMs() - startTime);
				}
			};
		}
		
		
		abstract void incFailed();
		abstract void updateSlowest(long l);
		abstract void incSuccessful();
		abstract Observable<T> _async();
		abstract boolean shouldThrowImediately(Throwable value1);
		void incFailedMim(final Throwable e) {
			incFailed();
		}
	}

	@SafeVarargs
	public static boolean isInstanceOf(final Throwable e, final Class<? extends Exception>... classes) {
		for (final Class<? extends Exception> clazz: classes) {
			if (clazz.isInstance(e)) {
				return true;
			}
		}
		return false;
	}



}
