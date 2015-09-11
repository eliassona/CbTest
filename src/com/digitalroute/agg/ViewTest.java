package com.digitalroute.agg;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.simple.JSONValue;
import org.mockito.Mockito;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Observer;
import rx.Subscriber;
import rx.functions.Func1;

import com.couchbase.client.core.lang.Tuple;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.view.AsyncViewResult;
import com.couchbase.client.java.view.AsyncViewRow;
import com.couchbase.client.java.view.DesignDocument;
import com.couchbase.client.java.view.Stale;



/**
 * Test the view on the new 2.* couchbase client
 * @author anderse
 *
 */
public class ViewTest {
	public static final String SESSION_TIMEOUT = "SessionTimeout";

	static RawJsonDocument createJsonDoc(final String id, final long nextTimeout, final int nrOfFields) {
		final Map<String, Object> m = new HashMap<>();
		m.put(SESSION_TIMEOUT, nextTimeout(nextTimeout));
		return RawJsonDocument.create(id, moreData(m, nrOfFields));
	}
	private static String moreData(final Map<String, Object> obj, final int nrOfFields) {
		for (int i = 0; i < nrOfFields; i++) {
			obj.put("f" + i, Arrays.asList(new Integer[]{1, 2, 3, 4, 5, 6, 7, 8, 9}));
		}
		return JSONValue.toJSONString(obj);
	}
	public static long nextTimeout(final long nextTimeout) {
		return System.currentTimeMillis() + nextTimeout;
	}
	public static long nextTimeout() {
		return nextTimeout(60000);
	}

	static RawJsonDocument createJsonDoc(final JsonDocument doc) {
		final Map m = new HashMap<>();
		m.put(SESSION_TIMEOUT, nextTimeout());
		return RawJsonDocument.create(doc.id(), JSONValue.toJSONString(m));
	}
	
	public static Observable<Integer> countRows(final CbFacade cbFacade) {
		return Observable.create(new OnSubscribe<Integer>() {

			@Override
			public void call(final Subscriber<? super Integer> subscriber) {
				final AtomicInteger count = new AtomicInteger();
				cbFacade.queryTimeoutView(System.currentTimeMillis()).
				flatMap(new Func1<AsyncViewResult, Observable<? extends AsyncViewRow>>() {
					@Override
					public Observable<? extends AsyncViewRow> call(final AsyncViewResult t1) {
						return t1.rows();
					}
				}).
				throttleFirst(100, TimeUnit.MICROSECONDS).
				flatMap(new Func1<AsyncViewRow, Observable<? extends RawJsonDocument>>() {

					@Override
					public Observable<? extends RawJsonDocument> call(final AsyncViewRow row) {
						return cbFacade.read(row.id());
					}
				}).
				flatMap(new Func1<RawJsonDocument, Observable<? extends RawJsonDocument>>() {
					@Override
					public Observable<? extends RawJsonDocument> call(final RawJsonDocument doc) {
						final Map m = new HashMap<>();
						m.put(SESSION_TIMEOUT, nextTimeout());
						final RawJsonDocument newDoc = RawJsonDocument.create(doc.id(), JSONValue.toJSONString(m), doc.cas());
						return cbFacade.update(newDoc);
					}
				}).
				subscribe(new Observer<Object>() {
					
					@Override
					public void onCompleted() {
						subscriber.onNext(count.get());
						subscriber.onCompleted();
					}
					
					@Override
					public void onError(final Throwable e) {
						subscriber.onError(e);
					}
					
					@Override
					public void onNext(final Object t) {
						//could have used the count operator, but wanted to try my own subscription
						count.incrementAndGet();
					}
					
				});
				
			}
		});
	}
	
	
	public static void createDocs(final CbFacade cbFacade, final int size, final long nextTimeout, final int nrOfFields) {
		final Semaphore semaphore = new Semaphore(50);
		Observable.
		from(ids(size)).
	    map(new Func1<String, RawJsonDocument>() {
			@Override
			public RawJsonDocument call(final String id) {
				try {
					semaphore.acquire();
				} catch (final InterruptedException e) {
					e.printStackTrace();
				}
				return createJsonDoc(id, nextTimeout, nrOfFields);
			}
	    }).
	    flatMap(new Func1<RawJsonDocument, Observable<? extends RawJsonDocument>>() {
			@Override
			public Observable<? extends RawJsonDocument> call(final RawJsonDocument doc) {
				return cbFacade.create(doc);
			}
		}).subscribe(new Subscriber<Object>() {
			@Override
			public void onCompleted() {
			}

			@Override
			public void onError(final Throwable e) {
				e.printStackTrace();
			}

			@Override
			public void onNext(final Object t) {
				semaphore.release();
			}
		});
	}

	public static List<String> ids(final int size) {
		final List<String> l = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			l.add("" + i);
		}
		return l;
	}
	public static void createViewDef(final Bucket bucket) {
    	bucket.bucketManager().upsertDesignDocument(viewOf("timeout"));
	}

	
	public static final String javascript = 
			  "function (doc, meta) {\n" +
			  "if(meta.type!=\"json\") { return; }\n" + 
			  "st = null;\n" + 
			  "if(doc.drFormatVersion == 1) {\n" +
			  "  if(doc.data.Content.SessionTimeout) {\n" + 
			  "    st = doc.data.Content.SessionTimeout;\n" +
			  "  }\n" + 
			  "} else {\n" +
			  "  if(doc.SessionTimeout){\n" +
			       "st = doc.SessionTimeout;\n" +
			  "  }\n" +
			  "}\n" +
			  "emit(st.toString(), null);\n" +
			  "}\n";


    public static DesignDocument viewOf(final String viewName) {
    	//TODO properties
        return DesignDocument.from(viewName, 
                JsonObject.empty().put("views", 
                        JsonObject.empty().put(viewName, 
                                JsonObject.empty().put("map", javascript))));
    }

	
    public static CountDownLatch runTimeoutLoop(final CbFacade cbFacade) {
		final CountDownLatch latch = new CountDownLatch(1);
		Observable.interval(2, TimeUnit.SECONDS).
		   flatMap(new Func1<Long, Observable<? extends Integer>>() {
			@Override
			public Observable<? extends Integer> call(final Long t1) {
				return countRows(cbFacade); 
			}
		}).
		subscribe(new Observer<Object>() {

			@Override
			public void onCompleted() {
				System.out.println("onCompleted");
				latch.countDown();
			}

			@Override
			public void onError(final Throwable e) {
				latch.countDown();
				e.printStackTrace();
				System.out.println("exiting...");
				System.exit(0);
			}

			@Override
			public void onNext(final Object t) {
				System.out.println(String.format("onNext %s: %s", System.currentTimeMillis(), t));
			}
		});
		return latch;

    }
    public static CrudFacade createCrudFacade(final Bucket bucket, final CouchbaseStatistics statistics) {
    	return new CrudFacadeImpl(bucket, statistics, 1, 10L, 0, 0, "timeout", Stale.FALSE, Executors.newFixedThreadPool(8, Executors.privilegedThreadFactory()), Tuple.create(10L, TimeUnit.SECONDS));
    }


	public static void main(final String[] args) throws InterruptedException {
		final CouchbaseCluster cluster = CouchbaseCluster.create();
		final Bucket bucket = cluster.openBucket("aggregation");
		final CouchbaseStatistics statistics = Mockito.mock(CouchbaseStatistics.class);
		final CrudFacade crudFacade = createCrudFacade(bucket, statistics);
		final CbFacade cbFacade = new CbFacadeImpl(false, crudFacade);
		bucket.bucketManager().flush();
		
		createViewDef(bucket);
		
		createDocs(cbFacade, 1000000, 120000, 100);
		
		
		
		runTimeoutLoop(cbFacade).await();
		
	}



}
