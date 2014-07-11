package com.jgoodies.forms.util;

import java.awt.Component;

public abstract interface UnitConverter
{
  public abstract int inchAsPixel(double paramDouble, Component paramComponent);

  public abstract int millimeterAsPixel(double paramDouble, Component paramComponent);

  public abstract int centimeterAsPixel(double paramDouble, Component paramComponent);

  public abstract int pointAsPixel(int paramInt, Component paramComponent);

  public abstract int dialogUnitXAsPixel(int paramInt, Component paramComponent);

  public abstract int dialogUnitYAsPixel(int paramInt, Component paramComponent);
}