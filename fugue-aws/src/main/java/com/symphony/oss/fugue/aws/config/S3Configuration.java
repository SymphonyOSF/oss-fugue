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

package com.symphony.oss.fugue.aws.config;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.symphony.oss.fugue.Fugue;
import com.symphony.oss.fugue.config.Configuration;
import com.symphony.oss.fugue.config.IConfigurationFactory;

/**
 * An implementation of IConfiguration which reads a JSON document from an S3 bucket.
 * 
 * If the value of the given variable is not a valid URL this class attempts to read
 * that value as a file name.
 * 
 * @author Bruce Skingle
 *
 */
public class S3Configuration extends Configuration
{
  /** Singleton factory */
  public static final IConfigurationFactory FACTORY = new IConfigurationFactory()
  {
    @Override
    public S3Configuration newInstance()
    {
      return new S3Configuration();
    }
  };
  
  private static final Logger log_ = LoggerFactory.getLogger(S3Configuration.class);

  private static final String AWS_COM = ".amazonaws.com";
  
  private S3Configuration()
  {
    this(new S3Config());
  }
  
  private S3Configuration(S3Config s3Config)
  {
    super(s3Config.tree_);
    
    setName(s3Config.name_);
  }

  private static class S3Config
  {
    private String name_;
    private JsonNode tree_;


    private S3Config()
    {
      String fugueConfig = Fugue.getRequiredProperty(Fugue.FUGUE_CONFIG);
      
      if(fugueConfig.trim().startsWith("{"))
      {
        log_.info("Loading direct config " + fugueConfig);
        
        loadConfig(fugueConfig);
        name_ = "Direct Config";
      }
      else
      {
        try
        {
          URL configUrl = new URL(fugueConfig);
          
          log_.info("Loading config from {}", configUrl);
          
          String host = configUrl.getHost();
          
          if(host.endsWith(AWS_COM))
          {
            String region = host.substring(0, host.length() - AWS_COM.length());
            
            if(region.equals("s3"))
            {
              region = System.getenv("AWS_REGION");
            }
            else if(region.startsWith("s3"))
            {
              region = region.substring(3);
            }
            
            String path = configUrl.getPath();
            int i = path.indexOf('/', 1);
            
            String bucket = path.substring(0, i);
            String key = path.substring(i+1);
            
            loadConfig(region, bucket, key);
            name_ = configUrl + "#";
          }
          else
          {
            throw new IllegalStateException(Fugue.FUGUE_CONFIG + " is " + configUrl + " a " + AWS_COM + " hostname is expected.");
          }
        }
        catch (MalformedURLException e)
        {
          File file = new File(fugueConfig);
          
          if(file.exists())
          {
            try(InputStream in = new FileInputStream(file))
            {
              log_.info("Loading config from file " + file.getAbsolutePath());
              
              loadConfig(in);
              name_ = file.getAbsolutePath() + " ";
            }
            catch (FileNotFoundException e1)
            {
              throw new IllegalStateException(Fugue.FUGUE_CONFIG + " is " + fugueConfig + " but this file does not exist.", e1);
            }
            catch (IOException e1)
            {
              log_.warn("Failed to close config input", e1);
            }
          }
          else
          {
            throw new IllegalStateException(Fugue.FUGUE_CONFIG + " is " + fugueConfig + " but this file does not exist.");
          }
        }
      }
    }
  
  
    private void loadConfig(String region, String bucket, String key)
    {
      log_.info("Loading config from region: " + region + " bucket: " + bucket + " key: " + key);
      AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
          .withRegion(region)
          .build();
    
      
      
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      
      log_.info("About to read from S3");
      
      S3Object s3object = s3Client.getObject(new GetObjectRequest(bucket, key));
      
      try(S3ObjectInputStream in = s3object.getObjectContent())
      {
        byte buf[] = new byte[1024];
        int nbytes;
        
        while((nbytes = in.read(buf))>0)
        {
          bout.write(buf, 0, nbytes);
        }
      }
      catch (IOException e)
      {
        log_.warn("Failed to close config input", e);
      }
      log_.info("Loaded " + bout.size() + " bytes from S3");
      try(InputStream in = new ByteArrayInputStream(bout.toByteArray()))
      {
        loadConfig(in);
      }
      catch (IOException e)
      {
        log_.warn("Failed to close config input", e);
      }
      log_.info("Loaded " + tree_.size() + " top level config items");
    }
  
  
    private void loadConfig(InputStream in)
    {
      ObjectMapper mapper = new ObjectMapper();
      
      try
      {
        JsonNode configSpec = mapper.readTree(in);
        
        tree_ = configSpec;
      }
      catch (IOException e1)
      {
        throw new IllegalStateException("Cannot parse config.", e1);
      }
    }
    
    private void loadConfig(String directConfig)
    {
      ObjectMapper mapper = new ObjectMapper();
      
      try
      {
        JsonNode configSpec = mapper.readTree(directConfig);
        
        tree_ = configSpec;
      }
      catch (IOException e1)
      {
        throw new IllegalStateException("Cannot parse config.", e1);
      }
    }
  }
}
