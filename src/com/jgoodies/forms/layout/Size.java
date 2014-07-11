package com.jgoodies.forms.layout;

import java.awt.Container;
import java.util.List;

public abstract interface Size
{
  public abstract int maximumSize(Container paramContainer, List paramList, FormLayout.Measure paramMeasure1, FormLayout.Measure paramMeasure2, FormLayout.Measure paramMeasure3);

  public abstract boolean compressible();

  public abstract String encode();
}