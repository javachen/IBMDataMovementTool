package com.jgoodies.uif_lite.component;

import java.awt.Component;
import java.awt.Insets;
import javax.swing.AbstractButton;
import javax.swing.Action;
import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;

public final class Factory
{
  private static final Insets TOOLBAR_BUTTON_MARGIN = new Insets(1, 1, 1, 1);

  public static JScrollPane createStrippedScrollPane(Component component)
  {
    JScrollPane scrollPane = new JScrollPane(component);
    scrollPane.setBorder(BorderFactory.createEmptyBorder());
    return scrollPane;
  }

  public static JSplitPane createStrippedSplitPane(int orientation, Component comp1, Component comp2, double resizeWeight)
  {
    JSplitPane split = UIFSplitPane.createStrippedSplitPane(orientation, comp1, comp2);
    split.setResizeWeight(resizeWeight);
    return split;
  }

  public static AbstractButton createToolBarButton(Action action)
  {
    JButton button = new JButton(action);
    button.setFocusPainted(false);
    button.setMargin(TOOLBAR_BUTTON_MARGIN);

    button.setText("");
    return button;
  }
}