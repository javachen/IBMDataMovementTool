package com.jgoodies.forms.builder;

import com.jgoodies.forms.layout.CellConstraints;
import com.jgoodies.forms.layout.FormLayout;
import java.awt.Component;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;

public abstract class AbstractI15dPanelBuilder extends PanelBuilder
{
  private static final String DEBUG_TOOL_TIPS_ENABLED_KEY = "I15dPanelBuilder.debugToolTipsEnabled";
  private static boolean debugToolTipsEnabled = getDebugToolTipSystemProperty();

  protected AbstractI15dPanelBuilder(FormLayout layout)
  {
    super(layout);
  }

  protected AbstractI15dPanelBuilder(FormLayout layout, JPanel panel)
  {
    super(layout, panel);
  }

  private static boolean getDebugToolTipSystemProperty()
  {
    try
    {
      String value = System.getProperty("I15dPanelBuilder.debugToolTipsEnabled");
      return "true".equalsIgnoreCase(value); } catch (SecurityException e) {
    }
    return false;
  }

  public static boolean isDebugToolTipsEnabled()
  {
    return debugToolTipsEnabled;
  }

  public static void setDebugToolTipsEnabled(boolean b)
  {
    debugToolTipsEnabled = b;
  }

  public final JLabel addI15dLabel(String resourceKey, CellConstraints constraints)
  {
    JLabel label = addLabel(getI15dString(resourceKey), constraints);
    if (isDebugToolTipsEnabled()) {
      label.setToolTipText(resourceKey);
    }
    return label;
  }

  public final JLabel addI15dLabel(String resourceKey, String encodedConstraints)
  {
    return addI15dLabel(resourceKey, new CellConstraints(encodedConstraints));
  }

  public final JLabel addI15dLabel(String resourceKey, CellConstraints labelConstraints, Component component, CellConstraints componentConstraints)
  {
    JLabel label = addLabel(getI15dString(resourceKey), labelConstraints, component, componentConstraints);

    if (isDebugToolTipsEnabled()) {
      label.setToolTipText(resourceKey);
    }
    return label;
  }

  public final JComponent addI15dSeparator(String resourceKey, CellConstraints constraints)
  {
    JComponent component = addSeparator(getI15dString(resourceKey), constraints);
    if (isDebugToolTipsEnabled()) {
      component.setToolTipText(resourceKey);
    }
    return component;
  }

  public final JComponent addI15dSeparator(String resourceKey, String encodedConstraints)
  {
    return addI15dSeparator(resourceKey, new CellConstraints(encodedConstraints));
  }

  public final JLabel addI15dTitle(String resourceKey, CellConstraints constraints)
  {
    JLabel label = addTitle(getI15dString(resourceKey), constraints);
    if (isDebugToolTipsEnabled()) {
      label.setToolTipText(resourceKey);
    }
    return label;
  }

  public final JLabel addI15dTitle(String resourceKey, String encodedConstraints)
  {
    return addI15dTitle(resourceKey, new CellConstraints(encodedConstraints));
  }

  protected abstract String getI15dString(String paramString);
}