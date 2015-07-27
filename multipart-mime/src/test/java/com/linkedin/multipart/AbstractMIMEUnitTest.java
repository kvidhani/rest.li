/*
   Copyright (c) 2015 LinkedIn Corp.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.linkedin.multipart;


import com.linkedin.r2.filter.R2Constants;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;


/**
 * Provides the setup and teardown of a {@link java.util.concurrent.ExecutorService} that is necessary
 * for most tests.
 *
 * @author Karim Vidhani
 */
public abstract class AbstractMIMEUnitTest
{
  protected static ScheduledExecutorService _scheduledExecutorService;
  protected static int TEST_TIMEOUT = 30000;

  @BeforeClass
  public void threadPoolSetup()
  {
    //Gradle could run multiple test suites concurrently so we need plenty of threads.
    _scheduledExecutorService = Executors.newScheduledThreadPool(30);
  }

  @AfterClass
  public void threadPoolTearDown()
  {
    _scheduledExecutorService.shutdownNow();
    TEST_TIMEOUT = 30000; //In case a subclass changed this
  }

  //This is used by a lot of tests so we place it here.
  @DataProvider(name = "chunkSizes")
  public Object[][] chunkSizes() throws Exception
  {
    return new Object[][]{{1}, {R2Constants.DEFAULT_DATA_CHUNK_SIZE}};
  }
}