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
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link QingstorUnderFileSystem}. It will ensure Qingstor credentials are
 * present before returning a client. The validity of the credentials is checked by the client.
 */
@ThreadSafe
public class QingstorUnderFileSystemFactory implements UnderFileSystemFactory {

  /**
   * Constructs a new {@link QingstorUnderFileSystemFactory}.
   */
  public QingstorUnderFileSystemFactory() {}

  @Override
  public UnderFileSystem create(String path, Object unusedConf) {
    Preconditions.checkNotNull(path);

    if (checkQingstorCredentials()) {
      try {
        return QingstorUnderFileSystem.createInstance(new AlluxioURI(path));
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    String err = "Qingstor Credentials not available, cannot create Qingstor Under File System.";
    throw Throwables.propagate(new IOException(err));
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(Constants.HEADER_QINGSTOR);
  }

  /**
   * @return true if both access and secret key are present, false otherwise
   */
  private boolean checkQingstorCredentials() {
    return Configuration.containsKey(PropertyKey.QINGSTOR_ACCESS_KEY)
        && Configuration.containsKey(PropertyKey.QINGSTOR_SECRET_KEY);
  }
}
