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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

import com.qingstor.sdk.exception.QSException;
import com.qingstor.sdk.service.Bucket;

import alluxio.underfs.MultiRangeObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A stream for reading a file from Qingstor. This input stream returns 0 when calling read with an
 * empty buffer.
 */
@NotThreadSafe
public class QingstorInputStream extends MultiRangeObjectInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(QingstorInputStream.class);

  /** Bucket for operations with Qingstor objects. */
  private final Bucket mBucket;

  /** The path of the object to read. */
  private final String mKey;

  /** The size of the object in bytes. */
  private final long mContentLength;

  /**
   * Creates a new instance of {@link QingstorInputStream}.
   *
   * @param bucket the bucket for operations with Qingstor objects
   * @param key the key of the file
   * @throws IOException if an I/O error occurs
   */
  public QingstorInputStream(Bucket bucket, String key) throws IOException, QSException {
    this(bucket, key, 0L);
  }

  /**
   * Creates a new instance of {@link QingstorInputStream}.
   *
   * @param bucket the bucket for operations with Qingstor objects
   * @param key the key of the file
   * @param position the position to begin reading from
   * @throws IOException if an I/O error occurs
   */
  public QingstorInputStream(Bucket bucket, String key, long position)
      throws IOException, QSException {
    mBucket = bucket;
    mKey = key;
    mPos = position;

    // get object size
    Bucket.HeadObjectInput headObjectInput = new Bucket.HeadObjectInput();
    Bucket.HeadObjectOutput headObjectOutput = mBucket.headObject(key, headObjectInput);
    mContentLength = headObjectOutput == null ? 0 : headObjectOutput.getContentLength();
  }

  @Override
  protected InputStream createStream(long startPos, long endPos) throws IOException {
    Bucket.GetObjectInput input = new Bucket.GetObjectInput();
    // OSS returns entire object if we read past the end
    long rangeEnd = endPos < mContentLength ? endPos - 1 : mContentLength - 1;
    String range = String.valueOf(startPos) + "-" + String.valueOf(rangeEnd);
    input.setRange(range);
    try {
      Bucket.GetObjectOutput qingstorObject = mBucket.getObject(mKey, input);
      return new BufferedInputStream(qingstorObject.getBodyInputStream());
    } catch (QSException e) {
      LOG.error("Failed to get object file {} , range from {} to {}.", mKey, startPos, endPos);
      throw (new IOException(e));
    }
  }
}
