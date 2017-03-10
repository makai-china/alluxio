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
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import com.qingstor.sdk.config.EvnContext;
import com.qingstor.sdk.exception.QSException;
import com.qingstor.sdk.service.Bucket;
import com.qingstor.sdk.service.QingStor;
import com.qingstor.sdk.service.Types;
import com.qingstor.sdk.utils.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

public class QingstorUnderFileSystem extends ObjectUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(QingstorUnderFileSystem.class);

  /** Suffix for an empty file to flag it as a directory. */
  private static final String FOLDER_SUFFIX = "_$folder$";

  /** Static hash for a directory's empty contents. */
  private static final String DIR_HASH;

  /** Qingstor service. */
  private final QingStor mClient;

  /** Qingstor bucket. */
  private final Bucket mBucket;

  /** Bucket name of user's configured Alluxio bucket. */
  private final String mBucketName;

  /** The name of the qingstor bucket owner. */
  private final String mOwner;

  /** The permission mode that the account owner has to the bucket. */
  private final short mBucketMode;

  static {
    byte[] dirByteHash = DigestUtils.md5(new byte[0]);
    DIR_HASH = new String(Base64.encode(dirByteHash));
  }

  /**
   * Constructs a new instance of {@link QingstorUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @return the created {@link QingstorUnderFileSystem} instance
   * @throws Exception when a connection to GCS could not be created
   */
  public static QingstorUnderFileSystem createInstance(AlluxioURI uri) throws Exception {
    String bucketName = uri.getHost();

    Preconditions.checkArgument(Configuration.containsKey(PropertyKey.QINGSTOR_ACCESS_KEY),
        "Property " + PropertyKey.QINGSTOR_ACCESS_KEY + " is required to connect to Qingstor");
    Preconditions.checkArgument(Configuration.containsKey(PropertyKey.QINGSTOR_SECRET_KEY),
        "Property " + PropertyKey.QINGSTOR_SECRET_KEY + " is required to connect to Qingstor");
    Preconditions.checkArgument(Configuration.containsKey(PropertyKey.QINGSTOR_ZONE),
        "Property " + PropertyKey.QINGSTOR_ZONE + " is required to connect to Qingstor");

    String accessId = Configuration.get(PropertyKey.QINGSTOR_ACCESS_KEY);
    String accessKey = Configuration.get(PropertyKey.QINGSTOR_SECRET_KEY);
    String zone = Configuration.get(PropertyKey.QINGSTOR_ZONE);

    // get Qingstor service
    EvnContext evn = new EvnContext(accessId, accessKey);
    QingStor qingstorClient = new QingStor(evn, zone);

    // get Qingstor bucket
    Bucket bucket = qingstorClient.getBucket(bucketName, zone);

    // get Qingstor owner
    // TODO(XW): Getting null owner, use id instead.
    Bucket.GetBucketACLOutput getBucketACLOutput = bucket.getACL();
    String owner = getBucketACLOutput.getOwner().getID();

    // Default to readable and writable by the user.
    short bucketMode = (short) 700;

    return new QingstorUnderFileSystem(uri, qingstorClient, bucket, bucketName, owner, bucketMode);
  }

  /**
   * Constructs an {@link ObjectUnderFileSystem}.
   * 
   * @param uri the {@link AlluxioURI} used to create this ufs
   * @param mClient
   * @param mBucketName
   * @param mOwner
   * @param mBucketMode
   */
  protected QingstorUnderFileSystem(AlluxioURI uri, QingStor mClient, Bucket mBucket,
      String mBucketName, String mOwner, short mBucketMode) {
    super(uri);
    this.mClient = mClient;
    this.mBucket = mBucket;
    this.mBucketName = mBucketName;
    this.mOwner = mOwner;
    this.mBucketMode = mBucketMode;
  }

  @Override
  public String getUnderFSType() {
    return "qingstor";
  }

  // TODO(XW): Return the name of the bucket grantee.
  @Override
  public String getGroup(String path) throws IOException {
    return mOwner;
  }

  @Override
  public short getMode(String path) throws IOException {
    return mBucketMode;
  }

  @Override
  public String getOwner(String path) throws IOException {
    return mOwner;
  }

  // Setting Qingstor owner via Alluxio is not supported yet. This is a no-op.
  @Override
  public void setOwner(String path, String owner, String group) {}

  // Setting Qingstor mode via Alluxio is not supported yet. This is a no-op.
  @Override
  public void setMode(String path, short mode) throws IOException {}

  @Override
  protected boolean createEmptyObject(String key) {
    try {
      Bucket.PutObjectInput input = new Bucket.PutObjectInput();
      input.setContentLength((long) 0);
      input.setContentMD5(DIR_HASH);
      mBucket.putObject(key, input);
      return true;
    } catch (QSException e) {
      LOG.error("Failed to create object: {}", key, e);
      return false;
    }
  }

  @Override
  protected OutputStream createObject(String key) throws IOException {
    return new QingstorOutputStream(mBucket, key);
  }

  @Override
  protected boolean copyObject(String src, String dst) throws IOException {
    try {
      LOG.info("Copying {} to {}", src, dst);
      Bucket.PutObjectInput input = new Bucket.PutObjectInput();
      input.setXQSCopySource(src);
      mBucket.putObject(dst, input);
      return true;
    } catch (QSException e) {
      LOG.error("Failed to copy file {} to {}", src, dst, e);
      return false;
    }
  }

  @Override
  protected boolean deleteObject(String key) throws IOException {
    try {
      mBucket.deleteObject(key);
    } catch (QSException e) {
      LOG.error("Failed to delete {}", key, e);
      return false;
    }
    return true;
  }

  @Override
  protected ObjectStatus getObjectStatus(String key) {
    try {
      Bucket.HeadObjectInput headObjectInput = new Bucket.HeadObjectInput();
      Bucket.HeadObjectOutput headObjectOutput = mBucket.headObject(key, headObjectInput);

      if (headObjectOutput == null || headObjectOutput.getLastModified() == null) {
        return null;
      }

      // format metadata
      String modifiedStr = headObjectOutput.getLastModified();
      long contentLen = modifiedStr == null ? 0 : headObjectOutput.getContentLength();
      SimpleDateFormat simpleDateFormat =
          new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US);
      long lastModified = modifiedStr == null ? 0 : simpleDateFormat.parse(modifiedStr).getTime();

      return new ObjectStatus(contentLen, lastModified);
    } catch (QSException e) {
      return null;
    } catch (ParseException e) {
      return null;
    }
  }

  @Override
  protected String getFolderSuffix() {
    return FOLDER_SUFFIX;
  }

  @Override
  protected ObjectListingChunk getObjectListingChunk(String key, boolean recursive)
      throws IOException {


    String delimiter = recursive ? "" : PATH_SEPARATOR;
    key = PathUtils.normalizePath(key, PATH_SEPARATOR);
    // In case key is root (empty string) do not normalize prefix
    key = key.equals(PATH_SEPARATOR) ? "" : key;
    Bucket.ListObjectsInput input = new Bucket.ListObjectsInput();
    input.setPrefix(key);
    input.setLimit((long) getListingChunkLength());
    input.setDelimiter(delimiter);
    input.setMarker(null);

    Bucket.ListObjectsOutput result = getObjectListingChunk(input);

    if (result != null) {
      return new QingstorObjectListingChunk(input, result);
    }
    return null;
  }

  // Get next chunk of listing result
  private Bucket.ListObjectsOutput getObjectListingChunk(Bucket.ListObjectsInput input) {
    Bucket.ListObjectsOutput result;
    try {
      result = mBucket.listObjects(input);
    } catch (QSException e) {
      LOG.error("Failed to list path {}", input.getPrefix(), e);
      result = null;
    }
    return result;
  }

  /**
   * Wrapper over Qingstor {@link Bucket.ListObjectsOutput}.
   */
  private final class QingstorObjectListingChunk implements ObjectListingChunk {
    final Bucket.ListObjectsInput mRequest;
    final Bucket.ListObjectsOutput mResult;

    QingstorObjectListingChunk(Bucket.ListObjectsInput request, Bucket.ListObjectsOutput result)
        throws IOException {
      mRequest = request;
      mResult = result;
      if (mResult == null) {
        throw new IOException("Qingstor listing result is null");
      }
    }

    @Override
    public String[] getObjectNames() {
      List<Types.KeyModel> keys = mResult.getKeys();
      String[] ret = new String[keys.size()];
      int i = 0;
      for (Types.KeyModel key : keys) {
        ret[i++] = key.getKey();
      }
      return ret;
    }

    @Override
    public String[] getCommonPrefixes() {
      List<String> res = mResult.getCommonPrefixes();
      return res.toArray(new String[res.size()]);
    }

    @Override
    public ObjectListingChunk getNextChunk() throws IOException {
      if (mResult.getKeys().size() == mResult.getLimit()) {
        mRequest.setMarker(mResult.getNextMarker());
        Bucket.ListObjectsOutput nextResult = getObjectListingChunk(mRequest);
        if (nextResult != null) {
          return new QingstorObjectListingChunk(mRequest, nextResult);
        }
      }
      return null;
    }
  }

  @Override
  protected String getRootKey() {
    return Constants.HEADER_QINGSTOR + mBucketName;
  }

  @Override
  protected InputStream openObject(String key, OpenOptions options) throws IOException {
    try {
      return new QingstorInputStream(mBucket, key, options.getOffset());
    } catch (QSException e) {
      throw new IOException(e.getMessage());
    }
  }
}
