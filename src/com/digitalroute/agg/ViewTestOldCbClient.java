package com.digitalroute.agg;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import net.spy.memcached.CASValue;

import org.json.simple.JSONValue;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.protocol.views.ComplexKey;
import com.couchbase.client.protocol.views.Paginator;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.Stale;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewRow;


/**
 * Test the view on the 1.4.* couchbase client
 * @author anderse
 *
 */
public class ViewTestOldCbClient {

	/**
	 * Create view def and documents with the new clients (because that code already exists)
	 */
	public static void createViewAndDocs() {
		final Bucket bucket = CouchbaseCluster.create().openBucket("aggregation");
		ViewTest.createViewDef(bucket);
		final CouchbaseStatistics statistics = mock(CouchbaseStatistics.class);
		ViewTest.createDocs(new CbFacadeImpl(false, ViewTest.createCrudFacade(bucket, statistics)), 1000000, 10000);
		
	}
	

	
	
	public static Query initializeKeyRangeQuery(final Stale stale, final String endKey) {
		final Query query = new Query().setStale(stale);
		query.setIncludeDocs(false);
	    query.setRangeEnd(ComplexKey.of(endKey));
		return query;
	}
	
	
	public static ExecutorService createExec() {
        return new ThreadPoolExecutor(3, 3, 0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(3 * 2),
                new ThreadFactory() {
					
					@Override
					public Thread newThread(final Runnable r) {
						return new Thread();
					}
				},
                new ThreadPoolExecutor.CallerRunsPolicy());

	}

	public static void main(final String[] args) throws IOException, URISyntaxException, InterruptedException, ExecutionException {
		final CouchbaseClient client = new CouchbaseClient(Arrays.asList(new URI(String.format("http://%s:%s/pools", "localhost", 8091))), "aggregation", "");
		if (!client.flush().get()) throw new IllegalStateException("Couldn't flush couchbase");
//		
		final ExecutorService executor = createExec();
		createViewAndDocs();
		
		doTest(client, executor);
	}




	private static void doTest(final CouchbaseClient client, final ExecutorService executor) throws InterruptedException {
		while (true) {
			final AtomicLong longestDuration = new AtomicLong();
			final View view = client.getView("timeout", "timeout");
			final Query query = initializeKeyRangeQuery(Stale.FALSE, System.currentTimeMillis() + "");
			final Paginator pa = client.paginatedQuery(view, query, 1000);
			System.out.println("Do timeouts");
			while (pa.hasNext()) {
				final Iterator<ViewRow> iter = pa.next().iterator();
				while (iter.hasNext()) {
					final String id = iter.next().getId();
					executor.submit(new TimeoutTask(id, client, longestDuration));
				}
			}
			System.out.println(String.format("Longest timeout %s ms", longestDuration.get()));
			Thread.sleep(10000);
		}
	}		

}


class TimeoutTask implements Runnable {
	private final CouchbaseClient client;
	private final String id;
	private final AtomicLong longestTimeout;

	public TimeoutTask(final String id, final CouchbaseClient client, final AtomicLong longestTimeout) {
		this.id = id;
		this.client = client;
		this.longestTimeout = longestTimeout;
	}


	@Override
	public void run() {
		try {
			final long startTime = System.currentTimeMillis();
			final CASValue<Object> casDoc = client.asyncGetAndLock(id, 0).get();
			final long duration = (System.currentTimeMillis() - startTime);
			updateLongest(duration);
			if (casDoc.getValue() != null) {
				final Map<String, Object> m = (Map<String, Object>) JSONValue.parse((String) casDoc.getValue());
				final long t = (long) m.get(ViewTest.SESSION_TIMEOUT);
				if (t < System.currentTimeMillis()) {
					m.put(ViewTest.SESSION_TIMEOUT, System.currentTimeMillis() + 10000);
					client.cas(id, casDoc.getCas(), JSONValue.toJSONString(m));
	//				System.out.println(String .format("%s timed out by %s", id, Thread.currentThread().getName()));
				} else {
					client.unlock(id, casDoc.getCas());
				}
			}
		} catch (final Throwable e) {
			e.printStackTrace();
		}
	}


	private void updateLongest(final long duration) {
		for (int i = 0; i < 10; i++) {
			final long oldDuration = longestTimeout.get();
			if (duration > oldDuration) {
				if (longestTimeout.compareAndSet(oldDuration, duration)) {
					return;
				}
			}
		}
	}
}

