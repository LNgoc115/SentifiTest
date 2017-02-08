package com.sentifi.test.quandl.connection;

import com.sentifi.test.quandl.util.DefaultRESTDataProvider;
import com.sentifi.test.quandl.util.RESTDataProvider;

/**
 * Contain options for connect to Quandl
 * 
 * @author NGOCLM
 *
 */
public class SessionOptions {

	private static final long ONE_SECOND = 1000L;
	private static final long FIVE_SECONDS = 5000L;
	private static final long TWENTY_SECONDS = 20000L;
	private static final long SIXTY_SECONDS = 60000L;

	private RESTDataProvider restDataProvider = new DefaultRESTDataProvider();
	// retry connect policy
	private RetryPolicy retryPolicy = RetryPolicy
			.createSequenceRetryPolicy(new long[] { ONE_SECOND, FIVE_SECONDS, TWENTY_SECONDS, SIXTY_SECONDS });
	
	public RESTDataProvider getRestDataProvider() {
		return restDataProvider;
	}
	public void setRestDataProvider(RESTDataProvider restDataProvider) {
		this.restDataProvider = restDataProvider;
	}
	public RetryPolicy getRetryPolicy() {
		return retryPolicy;
	}
	public void setRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
	}

}
