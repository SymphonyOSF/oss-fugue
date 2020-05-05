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

package com.symphony.oss.fugue.aws.sqs;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
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
 * An SWS SNS subscriber.
 * 
 * @author Bruce Skingle
 *
 */
/* package */ class SqsSubscriber extends AbstractPullSubscriber
{
  private static final int EXTENSION_TIMEOUT_SECONDS = 30;
  private static final int EXTENSION_FREQUENCY_MILLIS = 15000;
  
  private static final Logger                        log_              = LoggerFactory.getLogger(SqsSubscriber.class);

  private final SqsSubscriberManager                 manager_;
  private final AmazonSQS                            sqsClient_;
  private final String                               queueUrl_;
  private final ITraceContextTransactionFactory      traceFactory_;
  private final IThreadSafeRetryableConsumer<String> consumer_;
  private final NonIdleSubscriber                                nonIdleSubscriber_;
  private final String                               tenantId_;
  private int                                        messageBatchSize_ = 10;

  private final ReceiveMessageRequest                blockingPullRequest_;
  private final ReceiveMessageRequest                nonBlockingPullRequest_;


  /* package */ SqsSubscriber(SqsSubscriberManager manager, AmazonSQS sqsClient, String queueUrl,
      String subscriptionName, ITraceContextTransactionFactory traceFactory,
      IThreadSafeRetryableConsumer<String> consumer, ICounter counter, IBusyCounter busyCounter, String tenantId)
  {
    super(manager, subscriptionName, counter, busyCounter, EXTENSION_FREQUENCY_MILLIS, consumer);
    
    if(Fugue.isDebugSingleThread())
    {
      messageBatchSize_ = 1;
    }
    
    manager_ = manager;
    sqsClient_ = sqsClient;
    queueUrl_ = queueUrl;
    traceFactory_ = traceFactory;
    consumer_ = consumer;
    nonIdleSubscriber_ = new NonIdleSubscriber();
    tenantId_ = tenantId;

    blockingPullRequest_ = new ReceiveMessageRequest(queueUrl_)
        .withMaxNumberOfMessages(messageBatchSize_ )
        .withWaitTimeSeconds(20);
    
    nonBlockingPullRequest_ = new ReceiveMessageRequest(queueUrl_)
        .withMaxNumberOfMessages(messageBatchSize_ );
  }
  
  class NonIdleSubscriber implements Runnable
  {
    @Override
    public void run()
    {
      SqsSubscriber.this.run(false);
    }
  }

  @Override
  protected NonIdleSubscriber getNonIdleSubscriber()
  {
    return nonIdleSubscriber_;
  }

  public String getQueueUrl()
  {
    return queueUrl_;
  }
  
  @Override
  protected IPullSubscriberContext getContext()
  {
    return new SqsPullSubscriberContext();
  }

  private class SqsPullSubscriberContext implements IPullSubscriberContext
  {
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

    private Collection<IPullSubscriberMessage> pull(ReceiveMessageRequest pullRequest)
    {
      try(ITraceContextTransaction traceTransaction = traceFactory_.createTransaction("PubSubPull:SQS", UUID.randomUUID().toString(), tenantId_))
      {
        ITraceContext trace = traceTransaction.open();
        
        List<IPullSubscriberMessage>result = new LinkedList<>();
        
        ReceiveMessageResult receiveResult = sqsClient_.receiveMessage(pullRequest);
        
        trace.trace("RECEIVED_SQS");
        for(Message receivedMessage : receiveResult.getMessages())
        {
          result.add(new SqsPullSubscriberMessage(receivedMessage, trace));
        }
        
        return result;
      }
    }

    @Override
    public void close()
    {
      // Nothing
    }
  }

  private class SqsPullSubscriberMessage implements IPullSubscriberMessage
  {
    private final Message message_;
    private boolean       running_ = true;
    private ITraceContext trace_;
    
    private SqsPullSubscriberMessage(Message message, ITraceContext trace)
    {
      message_ = message;
      trace_ = trace;
    }

    @Override
    public String getMessageId()
    {
      return message_.getMessageId();
    }

    @Override
    public void run()
    {
      try(ITraceContextTransaction traceTransaction = trace_.createSubContext("PubSubHandle:SQS", message_.getMessageId(), tenantId_))
      {
        ITraceContext trace = traceTransaction.open();
        
        long retryTime = manager_.handleMessage(consumer_, message_.getBody(), trace, message_.getMessageId());
        
        synchronized(this)
        {
          // There is no point trying to extend the ack deadline now
          running_ = false;

          if(retryTime < 0)
          {
            trace.trace("ABOUT_TO_ACK");
            sqsClient_.deleteMessage(queueUrl_, message_.getReceiptHandle());
            traceTransaction.finished();
          }
          else
          {
            trace.trace("ABOUT_TO_NACK");
            
            int visibilityTimout = (int) (retryTime / 1000);
            
            sqsClient_.changeMessageVisibility(queueUrl_, message_.getReceiptHandle(), visibilityTimout);
            traceTransaction.aborted();
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
          sqsClient_.changeMessageVisibility(queueUrl_, message_.getReceiptHandle(), EXTENSION_TIMEOUT_SECONDS);
        }
        catch(RuntimeException e)
        {
          log_.error("Failed to extend message " + getMessageId(), e);
        }
      }
    }
  }
}
