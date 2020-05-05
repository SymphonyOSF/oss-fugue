package com.symphony.oss.fugue.server.http.ui.servlet;

import java.util.EnumSet;

import com.symphony.oss.fugue.FugueLifecycleState;

public interface ICommand
{

  String getName();

  String getPath();

  EnumSet<FugueLifecycleState> getValidStates();

  ICommandHandler getHandler();

}