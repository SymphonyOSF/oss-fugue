/*
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

package com.symphony.oss.fugue.inmemory.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.time.Instant;

import org.junit.Test;

import com.symphony.oss.commons.hash.Hash;
import com.symphony.oss.commons.hash.HashProvider;
import com.symphony.oss.commons.immutable.ImmutableByteArray;
import com.symphony.oss.fugue.inmemory.store.InMemoryObjectStoreWritable;
import com.symphony.oss.fugue.store.IFugueObject;
import com.symphony.oss.fugue.store.IFugueObjectPayload;
import com.symphony.oss.fugue.store.IFuguePodId;
import com.symphony.oss.fugue.store.IFugueVersionedObject;
import com.symphony.oss.fugue.store.NoSuchObjectException;
import com.symphony.oss.fugue.store.ObjectExistsException;
import com.symphony.oss.fugue.trace.NoOpTraceContext;

/**
 * Unit test for in memory object store.
 * 
 * @author Bruce Skingle
 *
 */
public class TestInMemoryObjectStore
{
  private InMemoryObjectStoreWritable objectStore_ = new InMemoryObjectStoreWritable.Builder()
      .build();
  
  private int payloadLimit_ = 400 * 1024;
  
  /**
   * Test store and retrieve.
   * 
   * @throws NoSuchObjectException If there is a problem.
   */
  @Test
  public void testStore() throws NoSuchObjectException
  {
    FugueObject  objectOne = new FugueObject("Object One");
    FugueObject  objectTwo = new FugueObject("Object Two");

    objectStore_.save(objectOne, payloadLimit_, NoOpTraceContext.INSTANCE);
    objectStore_.save(objectTwo, payloadLimit_, NoOpTraceContext.INSTANCE);
    
    String retOne = objectStore_.fetchAbsolute(objectOne.getAbsoluteHash());
    String retTwo = objectStore_.fetchAbsolute(objectTwo.getAbsoluteHash());
    
    assertEquals(objectOne.toString(), retOne);
    assertEquals(objectTwo.toString(), retTwo);
  }
  

//  @Test(expected=NoSuchObjectException.class)
//  public void testDelete() throws NoSuchObjectException
//  {
//    FugueObject  objectOne = new FugueObject("Object One");
//    FugueObject  objectTwo = new FugueObject("Object Two");
//
//    objectStore_.save(objectOne, NoOpTraceContext.INSTANCE);
//    //objectStore_.save(objectTwo, NoOpTraceContext.INSTANCE);
//    
//    String retOne = objectStore_.fetchAbsolute(objectOne.getAbsoluteHash());
//    
//    assertEquals(objectOne.toString(), retOne);
//    
//    objectStore_.delete(objectOne.getAbsoluteHash(), objectOne.getAbsoluteHash(), NoOpTraceContext.INSTANCE);
//    
//    retOne = objectStore_.fetchAbsolute(objectOne.getAbsoluteHash());
//    
//    assertEquals(objectOne.toString(), retOne);
//  }
  
  @Test
  public void testStoreIfNotExist() throws NoSuchObjectException, ObjectExistsException
  {
    FugueObject  id = new FugueObject("ID Object");
    FugueObject  payload1 = new FugueObject(id.getAbsoluteHash(), "Object One");
    FugueObject  payload2 = new FugueObject(id.getAbsoluteHash(), "Object Two");
    
    try
    {
      objectStore_.saveIfNotExists(payload1, payload2, payloadLimit_, NoOpTraceContext.INSTANCE);
      fail("Should throw exception");
    }
    catch(IllegalArgumentException e)
    {
      // expected
    }

    objectStore_.saveIfNotExists(id, payload1, payloadLimit_, NoOpTraceContext.INSTANCE);
    
    try
    {
      objectStore_.saveIfNotExists(id, payload2, payloadLimit_, NoOpTraceContext.INSTANCE);
      
      fail("Request should throw ObjectExistsException");
    }
    catch(ObjectExistsException e)
    {
      // expected
    }
  }
  
  class FugueObjectPayload implements IFugueObjectPayload
  {
    final String             value_;
    final ImmutableByteArray serialized_;
    final Hash               absoluteHash_;
   
    public FugueObjectPayload(String value)
    {

      value_= value;
      serialized_ = ImmutableByteArray.newInstance(value);
      absoluteHash_ = HashProvider.getHashOf(serialized_);
    }

    @Override
    public int hashCode()
    {
      return value_.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
      return value_.equals(obj);
    }

    @Override
    public String toString()
    {
      return value_.toString();
    }

    @Override
    public String getDescription()
    {
      return "Test object";
    }

    @Override
    public IFuguePodId getPodId()
    {
      return new IFuguePodId()
          {
            @Override
            public String toString()
            {
              return "101";
            }
            
            @Override
            public Integer getValue()
            {
              return 101;
            }
          };
    }

    @Override
    public String getPayloadType()
    {
      return "test.payload.type.id";
    }

    @Override
    public Instant getPurgeDate()
    {
      return null;
    }
  };
  
  class FugueVersionedObjectPayload extends FugueObjectPayload implements IFugueVersionedObject
  {
    final Hash baseHash_;
    final Hash prevHash_;

    public FugueVersionedObjectPayload(Hash baseHash, Hash prevHash, String value)
    {
      super(value);
      baseHash_ = baseHash;
      prevHash_ = prevHash;
    }

    @Override
    public Hash getBaseHash()
    {
      return baseHash_;
    }

    @Override
    public Hash getPrevHash()
    {
      return prevHash_;
    }

    @Override
    public Hash getAbsoluteHash()
    {
      return absoluteHash_;
    }

    @Override
    public String getRangeKey()
    {
      return value_;
    }
  }
  
  class FugueObject implements IFugueObject
  {
    private final String description_;
    private final FugueObjectPayload payload_;
    private final String value_;

    FugueObject(String value)
    {
      description_ = "FugueObject(" + value + ")";
      payload_ = new FugueObjectPayload(value);
      value_ = value;
    }
    
    public FugueObject(Hash absoluteHash, String value)
    {
      description_ = "FugueObject(" + value + ")";
      payload_ = new FugueVersionedObjectPayload(absoluteHash, absoluteHash, value);
      value_ = value;
    }

    @Override
    public int hashCode()
    {
      return value_.hashCode();
    }

    @Override
    public String toString()
    {
      return value_;
    }

    @Override
    public boolean equals(Object obj)
    {
      if(obj instanceof FugueObject)
        return value_.equals(((FugueObject) obj).value_);
      
      return false;
    }

    @Override
    public String getDescription()
    {
      return description_;
    }

    @Override
    public ImmutableByteArray serialize()
    {
      return payload_.serialized_;
    }

    @Override
    public Hash getAbsoluteHash()
    {
      return payload_.absoluteHash_;
    }

    @Override
    public String getRangeKey()
    {
      return value_;
    }

    @Override
    public IFugueObjectPayload getPayload()
    {
      return payload_;
    }

    @Override
    public IFuguePodId getPodId()
    {
      return payload_.getPodId();
    }
  }
}
