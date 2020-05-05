/*
 *
 *
 * Copyright 2019 Symphony Communication Services, LLC.
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

package com.symphony.oss.fugue.aws.kv.table;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.symphony.oss.commons.fault.CodingFault;
import com.symphony.oss.commons.fault.FaultAccumulator;
import com.symphony.oss.commons.hash.Hash;
import com.symphony.oss.fugue.Fugue;
import com.symphony.oss.fugue.aws.config.S3Helper;
import com.symphony.oss.fugue.kv.IKvItem;
import com.symphony.oss.fugue.store.NoSuchObjectException;
import com.symphony.oss.fugue.trace.ITraceContext;

/**
 * S3/DynamoDb implementation of IKvTable.
 * 
 * @author Bruce Skingle
 *
 */
public class S3DynamoDbKvTable extends AbstractDynamoDbKvTable<S3DynamoDbKvTable>
{
  private static final char SEPARATOR = '/';
  
  protected final String   objectBucketName_;
  protected final AmazonS3 s3Client_;
  private final boolean deferSecondaryStorage_;
  
  protected S3DynamoDbKvTable(S3DynamoDbKvTable.AbstractBuilder<?,?> builder)
  {
    super(builder);
    
    objectBucketName_       = config_.getConfiguration(serviceId_).getString("objectsBucketName", objectTableName_.toString());
    
    s3Client_ = builder.s3ClientBuilder_
        .withPathStyleAccessEnabled(true)
      .build();
    deferSecondaryStorage_ = builder.deferSecondaryStorage_;
  }

  @Override
  protected String fetchFromSecondaryStorage(Hash absoluteHash, ITraceContext trace)
      throws NoSuchObjectException
  {
    try
    {
      trace.trace("ABOUT-TO-READ-S3", "OBJECT", absoluteHash.toStringBase64());
      S3Object object = s3Client_.getObject(new GetObjectRequest(objectBucketName_, s3Key(absoluteHash)));
      
      if(object.getObjectMetadata().getContentLength() > Integer.MAX_VALUE)
        throw new IllegalStateException("Blob is too big");
      
      try(
          InputStream is = object.getObjectContent();
          InputStreamReader in = new InputStreamReader(is, StandardCharsets.UTF_8);
        )
      {
        int contentLength = (int)object.getObjectMetadata().getContentLength();
        StringBuilder buf = new StringBuilder(contentLength);
        
        char[] cbuf = new char[1024];
        int nbytes;
        
        while((nbytes = in.read(cbuf)) > 0)
        {
          buf.append(cbuf, 0, nbytes);
        }

        trace.trace("READ-S3", "OBJECT", absoluteHash.toStringBase64());
        return buf.toString();
      }
    }
    catch(AmazonS3Exception | IOException e)
    {

      trace.trace("FAILED-TO-READ-S3", "OBJECT", absoluteHash.toStringBase64());
      throw new NoSuchObjectException("Failed to read object from S3", e);
    }
    // we only call for objects which we know exist and are not in dynamo
  }
  
  public void storeToSecondaryStorage(Hash absoluteHash, String payload, ITraceContext trace)
  {
    byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
    
    try(InputStream in = new ByteArrayInputStream(bytes))
    {
      s3Client_.putObject(new PutObjectRequest(objectBucketName_, s3Key(absoluteHash), in, getS3MetaData(absoluteHash, bytes.length)));
      trace.trace("WRITTEN-S3");
    }
    catch (IOException e)
    {
      throw new CodingFault("In memory I/O - can't happen", e);
    }
    catch(RuntimeException e)
    {
      trace.trace("FAILED-TO-WRITE-S3");
      throw e;
    }
  }
  
  @Override
  protected boolean storeToSecondaryStorage(IKvItem kvItem, boolean payloadNotStored, ITraceContext trace)
  {
    if(deferSecondaryStorage_ && !payloadNotStored)
    {
      trace.trace("DEFER-S3", kvItem);
      return false;
    }
    
    byte[] bytes = kvItem.getJson().getBytes(StandardCharsets.UTF_8);
    
    try(InputStream in = new ByteArrayInputStream(bytes))
    {
      s3Client_.putObject(new PutObjectRequest(objectBucketName_, s3Key(kvItem.getAbsoluteHash()), in, getS3MetaData(kvItem.getAbsoluteHash(), bytes.length)));
      trace.trace("WRITTEN-S3", kvItem);
      return true;
    }
    catch (IOException e)
    {
      throw new CodingFault("In memory I/O - can't happen", e);
    }
    catch(RuntimeException e)
    {
      trace.trace("FAILED-TO-WRITE-S3", kvItem);
      throw e;
    }
  }
  
  @Override
  protected void deleteFromSecondaryStorage(Hash absoluteHash, ITraceContext trace)
  {
    s3Client_.deleteObject(objectBucketName_, s3Key(absoluteHash));
    
    trace.trace("DELETED-S3", "OBJECT", absoluteHash.toStringBase64());
  }
  
  private ObjectMetadata getS3MetaData(
      Hash absoluteHash, 
      long contentLength)
  {
    ObjectMetadata metaData = new ObjectMetadata();
        
    metaData.setContentLength(contentLength);
    metaData.setContentDisposition("attachment; filename=" + absoluteHash.toStringUrlSafeBase64() + ".json");
    metaData.setContentType("application/json");
    
    return metaData;
  }
  
  protected String s3Key(Hash absoluteHash)
  {
    return s3Key(absoluteHash.toStringUrlSafeBase64());
  }
  
  private String s3Key(String partitionKey)
  {
    // Return the S3 key used to store an object, we break the partition key into several directories to prevent a single directory
    // from getting too large.
    
    StringBuilder s = new StringBuilder();
    
    return s.append(partitionKey.substring(0, 4))
        .append(SEPARATOR)
        .append(partitionKey.substring(4, 8))
        .append(SEPARATOR)
        .append(partitionKey.substring(8))
        .toString();
  }

  protected static abstract class AbstractBuilder<T extends AbstractBuilder<T,B>, B extends AbstractDynamoDbKvTable<B>> extends AbstractDynamoDbKvTable.AbstractBuilder<T,B>
  {
    protected final AmazonS3ClientBuilder       s3ClientBuilder_;
    protected boolean deferSecondaryStorage_;
    
    protected AbstractBuilder(Class<T> type)
    {
      super(type);
      
      s3ClientBuilder_ = AmazonS3ClientBuilder.standard();
    }
    
    @Override
    public void validate(FaultAccumulator faultAccumulator)
    {
      super.validate(faultAccumulator);
      
//      IConfiguration s3Config = config_.getConfiguration("org/symphonyoss/s2/fugue/aws/s3");
//      
//      ClientConfiguration clientConfig = new ClientConfiguration()
//          .withMaxConnections(s3Config.getInt("maxConnections", ClientConfiguration.DEFAULT_MAX_CONNECTIONS))
//          .withClientExecutionTimeout(s3Config.getInt("clientExecutionTimeout", ClientConfiguration.DEFAULT_CLIENT_EXECUTION_TIMEOUT))
//          .withConnectionMaxIdleMillis(s3Config.getLong("connectionMaxIdleMillis", ClientConfiguration.DEFAULT_CONNECTION_MAX_IDLE_MILLIS))
//          .withConnectionTimeout(s3Config.getInt("connectionTimeout", ClientConfiguration.DEFAULT_CONNECTION_TIMEOUT))
//          ;
//      
//      log_.info("Starting S3 object store client in " + region_ + " with " + clientConfig.getMaxConnections() + " max connections...");
//
//      
      s3ClientBuilder_
        .withRegion(region_)
//        .withClientConfiguration(clientConfig)
        ;
    }

    public T withDeferSecondaryStorage(boolean deferSecondaryStorage)
    {
      deferSecondaryStorage_ = deferSecondaryStorage;
      
      return self();
    }

    @Override
    public T withCredentials(AWSCredentialsProvider credentials)
    {
      s3ClientBuilder_.withCredentials(credentials);
      
      return super.withCredentials(credentials);
    }
  }
  
  @Override
  public void createTable(boolean dryRun)
  {
    super.createTable(dryRun);
    
    Map<String, String> tags = new HashMap<>(nameFactory_.getTags());
    
    tags.put(Fugue.TAG_FUGUE_SERVICE, serviceId_);
    tags.put(Fugue.TAG_FUGUE_ITEM, objectBucketName_);
    
    S3Helper.createBucketIfNecessary(s3Client_, objectBucketName_, tags, dryRun);
  }

  @Override
  public void deleteTable(boolean dryRun)
  {
    S3Helper.deleteBucket(s3Client_, objectBucketName_, dryRun);
    
    super.deleteTable(dryRun);
  }

  /**
   * Builder for S3DynamoDbKvTable.
   * 
   * @author Bruce Skingle
   *
   */
  public static class Builder extends AbstractBuilder<Builder, S3DynamoDbKvTable>
  {
    /**
     * Constructor.
     */
    public Builder()
    {
      super(Builder.class);
    }

    @Override
    protected S3DynamoDbKvTable construct()
    {
      return new S3DynamoDbKvTable(this);
    }
  }
}
