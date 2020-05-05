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

package com.symphony.oss.fugue.google.pubsub;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.ReceivedMessage;
import com.symphony.oss.commons.fault.CodingFault;
import com.symphony.oss.fugue.Fugue;
import com.symphony.oss.fugue.counter.IBusyCounter;
import com.symphony.oss.fugue.counter.ICounter;
import com.symphony.oss.fugue.pipeline.IThreadSafeRetryableConsumer;
import com.symphony.oss.fugue.pubsub.AbstractPullSubscriber;
import com.symphony.oss.fugue.pubsub.IPullSubscriberContext;
import com.symphony.oss.fugue.pubsub.IPullSubscriberMessage;
import com.symphony.oss.fugue.trace.ITraceContext;
import com.symphony.oss.fugue.trace.ITraceContextTransaction;
import com.symphony.oss.fugue.trace.ITraceContextTransactionFactory;

/**
 * A subscriber to a single topic.
 * 
 * @author Bruce Skingle
 *
 */
public class GoogleSubscriber extends AbstractPullSubscriber
{
  private static final int EXTENSION_TIMEOUT_SECONDS = 10;
  private static final int EXTENSION_FREQUENCY_MILLIS = 5000;
  
  private static final Logger                                    log_     = LoggerFactory
      .getLogger(GoogleSubscriber.class);

  private final GoogleSubscriberManager                          manager_;
  private final ITraceContextTransactionFactory                  traceFactory_;
  private final IThreadSafeRetryableConsumer<String> consumer_;
  private final NonIdleSubscriber                                nonIdleSubscriber_;
  private final String                                           subscriptionName_;
  private final String                                           tenantId_;
  private int                                                    batchSize_ = 10;
  private SubscriberStubSettings subscriberStubSettings_;

  private final PullRequest blockingPullRequest_;
  private final PullRequest nonBlockingPullRequest_;

  /* package */ GoogleSubscriber(GoogleSubscriberManager manager,
      String subscriptionName, ITraceContextTransactionFactory traceFactory,
      IThreadSafeRetryableConsumer<String> consumer, ICounter counter, IBusyCounter busyCounter, String tenantId)
  {
    super(manager, subscriptionName, counter, busyCounter, EXTENSION_FREQUENCY_MILLIS, consumer);
    
    if(Fugue.isDebugSingleThread())
    {
      batchSize_ = 1;
    }
    
    manager_ = manager;
    subscriptionName_ = subscriptionName;
    traceFactory_ = traceFactory;
    consumer_ = consumer;
    nonIdleSubscriber_ = new NonIdleSubscriber();
    tenantId_ = tenantId;
    
    try
    {
      // Set the timeout to 60 seconds, needed to overcome https://github.com/googleapis/google-cloud-java/issues/4246
      
      SubscriberStubSettings.Builder settingsBuilder = SubscriberStubSettings.newBuilder();
      settingsBuilder.pullSettings().setSimpleTimeoutNoRetries(Duration.ofSeconds(60));
      subscriberStubSettings_ = settingsBuilder.build();
    }
    catch (IOException e)
    {
      throw new CodingFault(e);
    }

    blockingPullRequest_ = PullRequest.newBuilder().setMaxMessages(batchSize_)
        .setReturnImmediately(false) // return immediately if messages are not available
        .setSubscription(subscriptionName_)
        .build();
    
    nonBlockingPullRequest_ = PullRequest.newBuilder().setMaxMessages(batchSize_)
        .setReturnImmediately(true) // return immediately if messages are not available
        .setSubscription(subscriptionName_)
        .build();
  }
  
  String getSubscriptionName()
  {
    return subscriptionName_;
  }

  class NonIdleSubscriber implements Runnable
  {
    @Override
    public void run()
    {
      GoogleSubscriber.this.run(false);
    }
  }

  @Override
  protected NonIdleSubscriber getNonIdleSubscriber()
  {
    return nonIdleSubscriber_;
  }

  @Override
  protected IPullSubscriberContext getContext() throws IOException
  {
    return new GooglePullSubscriberContext();
  }
  
  class GooglePullSubscriberContext implements IPullSubscriberContext
  {
    private final GrpcSubscriberStub subscriber_;

    GooglePullSubscriberContext() throws IOException
    {
      subscriber_ = GrpcSubscriberStub.create(subscriberStubSettings_);
    }
    
    @Override
    public Collection<IPullSubscriberMessage> nonBlockingPull()
    {
      return pull(nonBlockingPullRequest_);
    }

    @Override
    public Collection<IPullSubscriberMessage> blockingPull()
    {
      return pull(blockingPullRequest_);
    }

    private Collection<IPullSubscriberMessage> pull(PullRequest pullRequest)
    {
      List<IPullSubscriberMessage>result = new LinkedList<>();
      
      for(ReceivedMessage receivedMessage : subscriber_.pullCallable().call(pullRequest).getReceivedMessagesList())
      {
        result.add(new GooglePullSubscriberMessage(subscriber_, receivedMessage));
      }
      
      return result;
    }

    @Override
    public void close()
    {
      subscriber_.close();
    }
  }

  private class GooglePullSubscriberMessage implements IPullSubscriberMessage
  {
    private final GrpcSubscriberStub subscriber_;
    private final ReceivedMessage    receivedMessage_;
    private boolean                  running_ = true;
    
    private GooglePullSubscriberMessage(GrpcSubscriberStub subscriber, ReceivedMessage receivedMessage)
    {
      subscriber_ = subscriber;
      receivedMessage_ = receivedMessage;
    }

    @Override
    public String getMessageId()
    {
      return receivedMessage_.getMessage().getMessageId();
    }

    @Override
    public String toString()
    {
      return receivedMessage_.toString();
    }

    @Override
    public void run()
    {
      PubsubMessage message = receivedMessage_.getMessage();
      Timestamp     ts      = message.getPublishTime();
      
      log_.debug("process message " + getMessageId());
      
      try(ITraceContextTransaction traceTransaction = traceFactory_.createTransaction("PubSub:Google", message.getMessageId(),
          tenantId_, Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos())))
      {
        ITraceContext trace = traceTransaction.open();
        
        trace.trace("RECEIVED");
        long retryTime = manager_.handleMessage(consumer_, message.getData().toStringUtf8(), trace, message.getMessageId());
        
        synchronized(this)
        {
          // There is no point trying to extend the ack deadline now
          running_ = false;
        
          if(retryTime < 0)
          {
            trace.trace("ABOUT_TO_ACK");
            
            AcknowledgeRequest acknowledgeRequest = AcknowledgeRequest
                .newBuilder()
                .setSubscription(subscriptionName_)
                .addAckIds(receivedMessage_.getAckId())
                .build();
            
            subscriber_.acknowledgeCallable().call(acknowledgeRequest);
            traceTransaction.finished();
            log_.debug("ACK message " + getMessageId());
          }
          else
          {
            trace.trace("ABOUT_TO_NACK");
            
            int visibilityTimout = (int) (retryTime / 1000);
            
            ModifyAckDeadlineRequest request = ModifyAckDeadlineRequest
              .newBuilder()
              .setSubscription(subscriptionName_)
              .setAckDeadlineSeconds(visibilityTimout)
              .addAckIds(receivedMessage_.getAckId())
              .build();
  
            subscriber_.modifyAckDeadlineCallable().call(request);
            
            traceTransaction.aborted();
            log_.debug("NAK message " + getMessageId());
          }
        }
      }
      catch(RuntimeException e)
      {
        log_.error("Failed to process message " + getMessageId(), e);
      }
    }

    @Override
    public synchronized void extend()
    {
      if(running_)
      {
        try
        {
          ModifyAckDeadlineRequest request = ModifyAckDeadlineRequest
            .newBuilder()
            .setSubscription(subscriptionName_)
            .setAckDeadlineSeconds(EXTENSION_TIMEOUT_SECONDS)
            .addAckIds(receivedMessage_.getAckId())
            .build();
  
          subscriber_.modifyAckDeadlineCallable().call(request);
        }
        catch(RuntimeException e)
        {
          log_.error("Failed to extend message " + getMessageId(), e);
        }
      }
    }
  }
}
