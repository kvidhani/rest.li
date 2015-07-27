package com.linkedin.multipart;

import com.linkedin.r2.filter.R2Constants;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Provides the setup and teardown of a {@link java.util.concurrent.ExecutorService} that is necessary
 * for most tests.
 *
 * @author Karim Vidhani
 */
public abstract class AbstractMIMEUnitTest {
    protected static ScheduledExecutorService _scheduledExecutorService;
    protected static int TEST_TIMEOUT = 30000;

    @BeforeClass
    public void threadPoolSetup() {
        //Gradle could run multiple test suites concurrently so we need plenty of threads.
        _scheduledExecutorService = Executors.newScheduledThreadPool(30);
    }

    @AfterClass
    public void threadPoolTearDown() {
        _scheduledExecutorService.shutdownNow();
        TEST_TIMEOUT = 30000; //In case a subclass changed this
    }

    //This is used by a lot of tests so we place it here.
    @DataProvider(name = "chunkSizes")
    public Object[][] chunkSizes() throws Exception {
        return new Object[][]{
                {1},
                {R2Constants.DEFAULT_DATA_CHUNK_SIZE}
        };
    }
}