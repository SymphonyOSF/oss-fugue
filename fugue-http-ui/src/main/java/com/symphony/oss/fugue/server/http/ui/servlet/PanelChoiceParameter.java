/*
 *
 *
 * Copyright 2017-2018 Symphony Communication Services, LLC.
 *
 * Licensed to The Symphony Software Foundation (SSF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The SSF licenses this file
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

package com.symphony.oss.fugue.server.http.ui.servlet;

import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class PanelChoiceParameter<T> extends PanelParameter<T>
{
  private Map<T, ?> values_;

  public PanelChoiceParameter(Class<T> type, String label, Supplier<T> getter, Consumer<T> setter,
      Map<T,?> values)
  {
    super(type, label, getter, setter);
    values_ = values;
  }

  @Override
  public void handleContent(WizardRequest req, UIHtmlWriter out)
  {
    out.openElement("tr");
    out.printElement("td", getLabel());
    out.openElement("td");
    
    T value = getGetter().get();
    
    out.openElement("table");
    for(Entry<T, ?> entry : values_.entrySet())
    {
      out.openElement("tr");
      out.printElement("td", entry.getValue());
      out.openElement("td");
      
      if(isEnabled())
      {
        if(entry.getKey().equals(value))
          out.printRadioInput(getId(), entry.getKey().toString(), "checked");
        else
          out.printRadioInput(getId(), entry.getKey().toString());
      }
      else
      {
        if(entry.getKey().equals(value))
        {
          out.printHiddenInput(getId(), value.toString());
          out.printRadioInput(DisabledField, entry.getKey().toString(), DISABLED, "checked");
        }
        else
          out.printRadioInput(getId(), entry.getKey().toString(), DISABLED);
      }
      out.closeElement(); // td
      out.closeElement(); // tr
    }
    out.closeElement(); // table
    
    out.closeElement(); // td
    out.closeElement(); // tr
    
  }

}
