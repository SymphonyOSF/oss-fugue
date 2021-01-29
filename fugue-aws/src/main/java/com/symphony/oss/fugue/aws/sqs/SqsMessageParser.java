/*
 *
 *
 * Copyright 2020 Symphony Communication Services, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.symphony.oss.fugue.aws.sqs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SqsMessageParser
{
  private static final Logger log_   = LoggerFactory.getLogger(SqsMessageParser.class);

  static ObjectMapper mapper = new ObjectMapper();

  public static List<SqsResponseMessage> parse(String json)
  {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode tree;
    JsonNode messages = null;
    try
    {
      tree = mapper.readTree(json);

//      JsonNode error = tree.get("Error");
//       if (error != null)
//        throw new IllegalStateException("Received SQS Message: " + json);
//       
//      JsonNode message = tree.get("message");
//       if (message != null)
//        throw new IllegalStateException("Received SQS Message: " + json);

      JsonNode response = tree.get("ReceiveMessageResponse");
      if(response == null)
      {
        if(tree.get("DeleteMessageResponse") == null)
        {
          log_.error("Received invalid SQS response: " + json);
        }
        
        return new ArrayList<>();
      }
      JsonNode result = response.get("ReceiveMessageResult");

      messages = result.get("messages");
      if ("null".equals(messages.toString()))
        return new ArrayList<>();

      return Arrays.asList(mapper.readValue(messages.toString(), SqsResponseMessage[].class));

    }
    catch (JsonProcessingException e)
    {
      throw new IllegalStateException("Error parsing the JSON: "+json);
    }
  }
}
