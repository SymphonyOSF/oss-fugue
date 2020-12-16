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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SqsMessageParser
{
  static ObjectMapper mapper = new ObjectMapper();

  public static List<SqsResponseMessage> parse(String json)
  {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode tree;
    JsonNode messages = null;
    try
    {
      tree = mapper.readTree(json);

      try
      {
        JsonNode error = tree.get("Error");
        if (error != null)
          throw new IllegalStateException("Received SQS Message: " + json);
      }
      catch (RuntimeException e1)
      {
        throw new IllegalStateException("Error returned: " + json);
      }
      JsonNode response = tree.get("ReceiveMessageResponse");
      if(response == null)
        return new ArrayList<>();
      JsonNode result = response.get("ReceiveMessageResult");

      messages = result.get("messages");
      if ("null".equals(messages.toString()))
        return new ArrayList<>();

      return Arrays.asList(mapper.readValue(messages.toString(), SqsResponseMessage[].class));

    }
    catch (JsonProcessingException e)
    {
      e.printStackTrace();
      throw new IllegalStateException("Error parsing the JSON: "+json);
    }
  }
}
