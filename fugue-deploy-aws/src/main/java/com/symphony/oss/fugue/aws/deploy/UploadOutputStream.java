/*
 *
 *
 * Copyright 2018 Symphony Communication Services, LLC.
 *
 * Licensed to The Symphony Software Foundation (SSF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.symphony.oss.fugue.aws.deploy;

import java.io.IOException;
import java.io.OutputStream;

import com.amazonaws.services.s3.AmazonS3;

import alex.mojaki.s3upload.MultiPartOutputStream;
import alex.mojaki.s3upload.StreamTransferManager;

/**
 * Simple wrapper around StreamTransferManager, flush() calls checksize() and close() calls complete().
 * 
 * @author Bruce Skingle
 *
 */
public class UploadOutputStream extends OutputStream
{

  private StreamTransferManager manager_;
  private MultiPartOutputStream out_;

  /**
   * Initiates a multipart upload to S3 using the first three parameters. Creates several
   * {@link MultiPartOutputStream}s and threads to upload the parts they produce in parallel.
   * Parts that have been produced sit in a queue of specified capacity while they wait for a thread to upload them.
   * The worst case memory usage is therefore {@code (numStreams + numUploadThreads + queueCapacity) * partSize},
   * while higher values for these first three parameters may lead to better resource usage and throughput.
   * <p>
   * S3 allows at most 10 000 parts to be uploaded. This means that if you are uploading very large files, the part
   * size must be big enough to compensate. Moreover the part numbers are distributed equally among streams so keep
   * this in mind if you might write much more data to some streams than others.
   * @param bucketName        the name of the bucket
   * @param putKey            the object key
   * @param s3Client          an S3 client
   *@param numUploadThreads   the number of threads that will upload parts as they are produced.
   * @param queueCapacity     the capacity of the queue that holds parts yet to be uploaded.
   * @param partSize          the minimum size of each part in MB before it gets uploaded. Minimum is 5 due to limitations of S3.
   *                          More than 500 is not useful in most cases as this corresponds to the limit of 5 TB total for any upload.
   */
  public UploadOutputStream(String bucketName,
      String putKey,
      AmazonS3 s3Client,
      int numUploadThreads,
      int queueCapacity,
      int partSize)
  {
    manager_ = new StreamTransferManager( bucketName,
         putKey,
         s3Client,
         1,
         numUploadThreads,
         queueCapacity,
         partSize);

    out_ = manager_.getMultiPartOutputStreams().get(0);
    
    
  }

  @Override
  public void flush() throws IOException
  {
    out_.flush();
    try
    {
      out_.checkSize();
    }
    catch (InterruptedException e)
    {
      throw new IOException(e);
    }
  }

  @Override
  public void write(int b)
  {
    out_.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len)
  {
    out_.write(b, off, len);
  }

  @Override
  public void write(byte[] b)
  {
    out_.write(b);
  }

  @Override
  public void close()
  {
    out_.close();
    manager_.complete();
  }

}
