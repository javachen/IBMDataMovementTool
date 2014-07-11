package com.jgoodies.uif_lite.component;

import java.awt.Component;
import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JSplitPane;
import javax.swing.UIManager;
import javax.swing.plaf.SplitPaneUI;
import javax.swing.plaf.basic.BasicSplitPaneDivider;
import javax.swing.plaf.basic.BasicSplitPaneUI;

public final class UIFSplitPane extends JSplitPane
{
  public static final String PROPERTYNAME_DIVIDER_BORDER_VISIBLE = "dividerBorderVisible";
  private boolean dividerBorderVisible;

  public UIFSplitPane()
  {
    this(1, false, new JButton(UIManager.getString("SplitPane.leftButtonText")), new JButton(UIManager.getString("SplitPane.rightButtonText")));
  }

  public UIFSplitPane(int newOrientation)
  {
    this(newOrientation, false);
  }

  public UIFSplitPane(int newOrientation, boolean newContinuousLayout)
  {
    this(newOrientation, newContinuousLayout, null, null);
  }

  public UIFSplitPane(int orientation, Component leftComponent, Component rightComponent)
  {
    this(orientation, false, leftComponent, rightComponent);
  }

  public UIFSplitPane(int orientation, boolean continuousLayout, Component leftComponent, Component rightComponent)
  {
    super(orientation, continuousLayout, leftComponent, rightComponent);
    this.dividerBorderVisible = false;
  }

  public static UIFSplitPane createStrippedSplitPane(int orientation, Component leftComponent, Component rightComponent)
  {
    UIFSplitPane split = new UIFSplitPane(orientation, leftComponent, rightComponent);
    split.setBorder(BorderFactory.createEmptyBorder());
    split.setOneTouchExpandable(false);
    return split;
  }

  public boolean isDividerBorderVisible()
  {
    return this.dividerBorderVisible;
  }

  public void setDividerBorderVisible(boolean newVisibility)
  {
    boolean oldVisibility = isDividerBorderVisible();
    if (oldVisibility == newVisibility)
      return;
    this.dividerBorderVisible = newVisibility;
    firePropertyChange("dividerBorderVisible", oldVisibility, newVisibility);
  }

  public void updateUI()
  {
    super.updateUI();
    if (!isDividerBorderVisible())
      setEmptyDividerBorder();
  }

  private void setEmptyDividerBorder()
  {
    SplitPaneUI splitPaneUI = getUI();
    if ((splitPaneUI instanceof BasicSplitPaneUI)) {
      BasicSplitPaneUI basicUI = (BasicSplitPaneUI)splitPaneUI;
      basicUI.getDivider().setBorder(BorderFactory.createEmptyBorder());
    }
  }
}