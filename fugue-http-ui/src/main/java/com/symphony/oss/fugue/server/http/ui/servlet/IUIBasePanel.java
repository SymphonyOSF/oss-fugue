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

public interface IUIBasePanel
{
  public static final String CLASS_STRIPY_TABLE = "w3-striped w3-bordered w3-border w3-hoverable w3-white";
  public static final String CLASS_CHOOSER_CONTROL = "chooser-control";
  public static final String CLASS = "class";
  public static final String TYPE = "type";
  public static final String CHECKBOX = "checkbox";
  public static final String CHECKED = "checked";
  public static final String VALUE = "value";
  public static final String ON_CHANGE = "onchange";
  public static final String ON_CLICK = "onclick";
  public static final String ID = "id";
  public static final String STYLE = "style";
  public static final String VISIBILITY_COLLAPSE = "visibility: collapse";
  public static final String VISIBILITY_SHOW = "visibility:";
  public static final String TABLE = "table";
  public static final String TRUE = "true";
  public static final String FALSE = "false";
  
  String getName();
  
  String  getId();
  
  IUIPanel getParent();

  void setParent(IUIPanel parent);

  String getPath();
  
  String getPath(String panelId);
  
  void setPathRoot(String uiServletRoot);
}
