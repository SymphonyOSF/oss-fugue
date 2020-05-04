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

package com.symphony.oss.fugue.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.symphony.oss.fugue.Fugue;

/**
 * Implementation of IConfiguration from a GitHub repo.
 * 
 * @author Bruce Skingle
 *
 */
public class GitHubConfiguration extends Configuration
{
  private static final Logger log_ = LoggerFactory.getLogger(GitHubConfiguration.class);
  
  /**
   * Constructor.
   */
  public GitHubConfiguration()
  {
    this(new GitHubConfig(Fugue.getRequiredProperty(Fugue.FUGUE_CONFIG)));
  }
  
  /* package */ GitHubConfiguration(String fileName)
  {
    this(new GitHubConfig(fileName));
  }
  
  private GitHubConfiguration(GitHubConfig gitHubConfig)
  {
    super(gitHubConfig.tree_);
    setName(gitHubConfig.name_);
  }

  private static class GitHubConfig
  {
    private JsonNode tree_;
    private String name_;

    private GitHubConfig(String fileName)
    {
      if(fileName==null || fileName.trim().length()==0)
        throw new IllegalArgumentException("fileName may not be null.");
      
      name_ = fileName;
      
      InputStream in = null;
      
      try
      {
        try
        {
          URL configUrl = new URL(fileName);
          
          log_.info("Loading config spec from {}", configUrl);
          
          try
          {
            in = configUrl.openStream();
          }
          catch (IOException e)
          {
            throw new IllegalStateException("FUGUE_CONFIG is " + configUrl + " but this URL is not readable", e);
          }
        }
        catch (MalformedURLException e)
        {
          File file = new File(fileName);
          
          if(!file.isFile())
            throw new IllegalStateException("FUGUE_CONFIG \"" + fileName + "\" is neither a URL or a valid file name.");
          
          if(!file.canRead())
            throw new IllegalStateException("FUGUE_CONFIG \"" + fileName + "\" is an unreadable file.");
          
          log_.info("Loading config spec from file {}", file.getAbsolutePath());
          try
          {
            in = new FileInputStream(file);
          }
          catch (FileNotFoundException e1)
          {
            // We already checked this but....
            throw new IllegalStateException("FUGUE_CONFIG \"" + fileName + "\" is neither a URL or a valid file name.", e1);
          }
        }
        
        ObjectMapper mapper = new ObjectMapper();
        
        try
        {
          JsonNode configSpec = mapper.readTree(in);
          
          JsonNode n;
          
          if((n = configSpec.get("url")) != null)
          {
            loadDirectConfig(fileName, n);
          }
          else if((n = configSpec.get("config")) != null)
          {
            tree_ = n;
          }
          else
          {
            throw new IllegalStateException("FUGUE_CONFIG \"" + fileName + "\" is invalid.");
          }
        }
        catch (IOException e1)
        {
          throw new IllegalStateException("Cannot parse config spec from FUGUE_CONFIG \"" + fileName + "\".", e1);
        }
      }
      finally
      {
        if(in != null)
        {
          try
          {
            in.close();
          }
          catch (IOException e)
          {
            log_.error("Failed to close config", e);
          }
        }
      }
    }
    
    private void loadDirectConfig(String fileName, JsonNode urlNode)
    {
      if(!urlNode.isTextual())
        throw new IllegalStateException("FUGUE_CONFIG \"" + fileName + "\" has a non-textual url.");
  
      try
      {
        URL configUrl = new URL(urlNode.asText());
        
        String host = configUrl.getHost();
        
        switch(host)
        {
          case "api.github.com":
            loadFromGitHub(configUrl);
            break;
          
          default:
            // We will assume that the url just returns the raw config data
            loadFromUrl(configUrl);
        }
      }
      catch (MalformedURLException e)
      {
        throw new IllegalStateException("FUGUE_CONFIG \"" + fileName + "\" has an invalid url \"" + urlNode + "\"", e);
      }
    }
    
    private void loadFromUrl(URL configUrl)
    {
      try(InputStream in =configUrl.openStream())
      {
        ObjectMapper mapper = new ObjectMapper();
        
        tree_ = mapper.readTree(in);
      }
      catch (IOException e)
      {
        throw new IllegalStateException("FUGUE_CONFIG is " + configUrl + " but this URL is not readable", e);
      }
    }
  
    private void loadFromGitHub(URL configUrl)
    {
      try(InputStream in =configUrl.openStream())
      {
        ObjectMapper mapper = new ObjectMapper();
        
        JsonNode tree = mapper.readTree(in);
        
        JsonNode content = tree.get("content");
        
        if(content == null || !content.isTextual())
          throw new RuntimeException("FUGUE_CONFIG is " + configUrl + " but there is no content node in the JSON there");
        
        byte[] bytes = Base64.decodeBase64(content.asText());
        
        tree_ = mapper.readTree(bytes);
      }
      catch (IOException e)
      {
        throw new IllegalStateException("FUGUE_CONFIG is " + configUrl + " but this URL is not readable", e);
      }
    }
  }
}
