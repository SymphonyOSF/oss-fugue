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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.symphony.oss.fugue.pubsub.IPubSubMessage;
import com.symphony.oss.fugue.pubsub.IQueueSender;

/**
 * AWS SQS implementation of QueueManager.
 * 
 * 
 * @author Bruce Skingle
 *
 */
public class SqsQueueSender implements IQueueSender
{
  private static final Logger log_ = LoggerFactory.getLogger(SqsQueueSender.class);

  private final AmazonSQS     sqsClient_;
  private final String        queueUrl_;

  /**
   * Constructor.
   * 
   * @param sqsClient An SQS client.
   * @param queueName The name of a queue
   * 
   * @throws QueueDoesNotExistException if the queue does not exist.
   */
  SqsQueueSender(AmazonSQS sqsClient, String queueName)
  {
    sqsClient_     = sqsClient;
    
    queueUrl_ = sqsClient_.getQueueUrl(queueName).getQueueUrl();

    log_.info("Queue " + queueName + " exists as " + queueUrl_);
  }
  
  @Override
  public void sendMessage(IPubSubMessage pubSubMessage)
  {
    pubSubMessage.getTraceContext().trace("ABOUT-TO-SEND", "SQS_QUEUE", queueUrl_);

    SendMessageRequest sendRequest = new SendMessageRequest()
        .withQueueUrl(queueUrl_)
        .withMessageBody(pubSubMessage.getPayload())
        ;
    
    if(!pubSubMessage.getAttributes().isEmpty())
    {
      Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
      
      for(Entry<String, Object> entry : pubSubMessage.getAttributes().entrySet())
      {
        messageAttributes.put(entry.getKey(), getAttribute(entry.getValue()));
      }
      
      sendRequest.withMessageAttributes(messageAttributes);
    }
    
    sqsClient_.sendMessage(sendRequest);
    pubSubMessage.getTraceContext().trace("SENT", "SQS_QUEUE", queueUrl_);
  }

  private static MessageAttributeValue getAttribute(Object value)
  {
    if(value instanceof Number)
    {
      return new MessageAttributeValue()
          .withDataType("Number")
          .withStringValue(value.toString());
    }
    
    return new MessageAttributeValue()
        .withDataType("String")
        .withStringValue(value.toString());
    
  }
}
