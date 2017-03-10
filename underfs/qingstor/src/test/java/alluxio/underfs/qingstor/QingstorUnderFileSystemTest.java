/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.qingstor;

import alluxio.AlluxioURI;
import alluxio.underfs.options.DeleteOptions;

import com.qingstor.sdk.exception.QSException;
import com.qingstor.sdk.service.Bucket;
import com.qingstor.sdk.service.QingStor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;

/**
 * Unit tests for the {@link alluxio.underfs.qingstor.QingstorUnderFileSystem}.
 */
public class QingstorUnderFileSystemTest {

  private QingstorUnderFileSystem mQingstorUnderFileSystem;
  private QingStor mClient;
  private Bucket mBucket;

  private static final String PATH = "path";
  private static final String SRC = "src";
  private static final String DST = "dst";

  private static final String BUCKET_NAME = "bucket";
  private static final short BUCKET_MODE = 0;
  private static final String ACCOUNT_OWNER = "account owner";

  /**
   * Set up.
   */
  @Before
  public void before() throws InterruptedException, QSException {
    mClient = Mockito.mock(QingStor.class);
    mBucket = Mockito.mock(Bucket.class);

    mQingstorUnderFileSystem = new QingstorUnderFileSystem(new AlluxioURI(""), mClient, mBucket,
        BUCKET_NAME, ACCOUNT_OWNER, BUCKET_MODE);
  }

  /**
   * Test case for {@link QingstorUnderFileSystem#deleteDirectory(String, DeleteOptions)}.
   */
  @Test
  public void deleteNonRecursiveOnServiceException() throws Exception {
    Mockito.when(mBucket.listObjects(Matchers.any(Bucket.ListObjectsInput.class)))
        .thenThrow(QSException.class);


    boolean result = mQingstorUnderFileSystem.deleteDirectory(PATH,
        DeleteOptions.defaults().setRecursive(false));
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link QingstorUnderFileSystem#deleteDirectory(String, DeleteOptions)}.
   */
  @Test
  public void deleteRecursiveOnServiceException() throws Exception {
    Mockito.when(mBucket.listObjects(Matchers.any(Bucket.ListObjectsInput.class)))
        .thenThrow(QSException.class);

    boolean result =
        mQingstorUnderFileSystem.deleteDirectory(PATH, DeleteOptions.defaults().setRecursive(true));
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link QingstorUnderFileSystem#renameFile(String, String)}.
   */
  @Test
  public void renameOnServiceException() throws Exception {
    Mockito.when(mBucket.listObjects(Matchers.any(Bucket.ListObjectsInput.class)))
        .thenThrow(QSException.class);


    boolean result = mQingstorUnderFileSystem.renameFile(SRC, DST);
    Assert.assertFalse(result);
  }
}
