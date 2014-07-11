package com.jgoodies.looks.common;

import com.jgoodies.looks.LookUtils;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.GraphicsConfiguration;
import java.awt.GraphicsDevice;
import java.awt.PrintGraphics;
import java.awt.RenderingHints;
import java.awt.RenderingHints.Key;
import java.awt.Toolkit;
import java.awt.print.PrinterGraphics;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.swing.JComponent;
import javax.swing.plaf.basic.BasicGraphicsUtils;

public final class RenderingUtils
{
  private static final String PROP_DESKTOPHINTS = "awt.font.desktophints";
  private static final String SWING_UTILITIES2_NAME = LookUtils.IS_JAVA_6_OR_LATER ? "sun.swing.SwingUtilities2" : "com.sun.java.swing.SwingUtilities2";

  private static Method drawStringUnderlineCharAtMethod = null;

  public static void drawStringUnderlineCharAt(JComponent c, Graphics g, String text, int underlinedIndex, int x, int y)
  {
    if (LookUtils.IS_JAVA_5_OR_LATER) {
      if (drawStringUnderlineCharAtMethod != null)
        try {
          drawStringUnderlineCharAtMethod.invoke(null, new Object[] { c, g, text, new Integer(underlinedIndex), new Integer(x), new Integer(y) });

          return;
        }
        catch (IllegalArgumentException e)
        {
        }
        catch (IllegalAccessException e) {
        }
        catch (InvocationTargetException e) {
        }
      Graphics2D g2 = (Graphics2D)g;
      Map oldRenderingHints = installDesktopHints(g2);
      BasicGraphicsUtils.drawStringUnderlineCharAt(g, text, underlinedIndex, x, y);
      if (oldRenderingHints != null) {
        g2.addRenderingHints(oldRenderingHints);
      }
      return;
    }
    BasicGraphicsUtils.drawStringUnderlineCharAt(g, text, underlinedIndex, x, y);
  }

  private static Method getMethodDrawStringUnderlineCharAt()
  {
    try
    {
      Class clazz = Class.forName(SWING_UTILITIES2_NAME);
      return clazz.getMethod("drawStringUnderlineCharAt", new Class[] { JComponent.class, Graphics.class, String.class, Integer.TYPE, Integer.TYPE, Integer.TYPE });
    }
    catch (ClassNotFoundException e)
    {
    }
    catch (SecurityException e)
    {
    }
    catch (NoSuchMethodException e)
    {
    }
    return null;
  }

  private static Map installDesktopHints(Graphics2D g2)
  {
    Map oldRenderingHints = null;
    if (LookUtils.IS_JAVA_6_OR_LATER) {
      Map desktopHints = desktopHints(g2);
      if ((desktopHints != null) && (!desktopHints.isEmpty())) {
        oldRenderingHints = new HashMap(desktopHints.size());

        for (Iterator i = desktopHints.keySet().iterator(); i.hasNext(); ) {
          RenderingHints.Key key = (RenderingHints.Key)i.next();
          oldRenderingHints.put(key, g2.getRenderingHint(key));
        }
        g2.addRenderingHints(desktopHints);
      }
    }
    return oldRenderingHints;
  }

  private static Map desktopHints(Graphics2D g2)
  {
    if (isPrinting(g2)) {
      return null;
    }
    Toolkit toolkit = Toolkit.getDefaultToolkit();
    GraphicsDevice device = g2.getDeviceConfiguration().getDevice();
    Map desktopHints = (Map)toolkit.getDesktopProperty("awt.font.desktophints." + device.getIDstring());

    if (desktopHints == null) {
      desktopHints = (Map)toolkit.getDesktopProperty("awt.font.desktophints");
    }

    if (desktopHints != null) {
      Object aaHint = desktopHints.get(RenderingHints.KEY_TEXT_ANTIALIASING);
      if ((aaHint == RenderingHints.VALUE_TEXT_ANTIALIAS_OFF) || (aaHint == RenderingHints.VALUE_TEXT_ANTIALIAS_DEFAULT))
      {
        desktopHints = null;
      }
    }
    return desktopHints;
  }

  private static boolean isPrinting(Graphics g)
  {
    return ((g instanceof PrintGraphics)) || ((g instanceof PrinterGraphics));
  }

  static
  {
    if (LookUtils.IS_JAVA_5_OR_LATER)
      drawStringUnderlineCharAtMethod = getMethodDrawStringUnderlineCharAt();
  }
}