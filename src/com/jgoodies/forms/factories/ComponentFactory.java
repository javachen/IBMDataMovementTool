package com.jgoodies.forms.factories;

import javax.swing.JComponent;
import javax.swing.JLabel;

public abstract interface ComponentFactory
{
  public abstract JLabel createLabel(String paramString);

  public abstract JLabel createTitle(String paramString);

  public abstract JComponent createSeparator(String paramString, int paramInt);
}