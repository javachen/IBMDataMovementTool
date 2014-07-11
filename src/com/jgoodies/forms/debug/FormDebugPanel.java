package com.jgoodies.forms.debug;

import com.jgoodies.forms.layout.FormLayout;
import com.jgoodies.forms.layout.FormLayout.LayoutInfo;
import java.awt.Color;
import java.awt.Graphics;
import javax.swing.JPanel;

public class FormDebugPanel extends JPanel
{
  private static final Color DEFAULT_GRID_COLOR = Color.red;
  private boolean paintInBackground;
  private boolean paintDiagonals;
  private Color gridColor = DEFAULT_GRID_COLOR;

  public FormDebugPanel()
  {
    this(null);
  }

  public FormDebugPanel(FormLayout layout)
  {
    this(layout, false, false);
  }

  public FormDebugPanel(boolean paintInBackground, boolean paintDiagonals)
  {
    this(null, paintInBackground, paintDiagonals);
  }

  public FormDebugPanel(FormLayout layout, boolean paintInBackground, boolean paintDiagonals)
  {
    super(layout);
    setPaintInBackground(paintInBackground);
    setPaintDiagonals(paintDiagonals);
    setGridColor(DEFAULT_GRID_COLOR);
  }

  public void setPaintInBackground(boolean b)
  {
    this.paintInBackground = b;
  }

  public void setPaintDiagonals(boolean b)
  {
    this.paintDiagonals = b;
  }

  public void setGridColor(Color color)
  {
    this.gridColor = color;
  }

  protected void paintComponent(Graphics g)
  {
    super.paintComponent(g);
    if (this.paintInBackground)
      paintGrid(g);
  }

  public void paint(Graphics g)
  {
    super.paint(g);
    if (!this.paintInBackground)
      paintGrid(g);
  }

  private void paintGrid(Graphics g)
  {
    if (!(getLayout() instanceof FormLayout)) {
      return;
    }
    FormLayout.LayoutInfo layoutInfo = FormDebugUtils.getLayoutInfo(this);
    int left = layoutInfo.getX();
    int top = layoutInfo.getY();
    int width = layoutInfo.getWidth();
    int height = layoutInfo.getHeight();

    g.setColor(this.gridColor);

    for (int col = 0; col < layoutInfo.columnOrigins.length; col++) {
      g.fillRect(layoutInfo.columnOrigins[col], top, 1, height);
    }

    for (int row = 0; row < layoutInfo.rowOrigins.length; row++) {
      g.fillRect(left, layoutInfo.rowOrigins[row], width, 1);
    }

    if (this.paintDiagonals) {
      g.drawLine(left, top, left + width, top + height);
      g.drawLine(left, top + height, left + width, top);
    }
  }
}