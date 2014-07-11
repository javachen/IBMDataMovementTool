package com.jgoodies.forms.builder;

import com.jgoodies.forms.factories.FormFactory;
import com.jgoodies.forms.layout.CellConstraints;
import com.jgoodies.forms.layout.ColumnSpec;
import com.jgoodies.forms.layout.FormLayout;
import com.jgoodies.forms.layout.RowSpec;
import java.awt.Color;
import java.awt.Component;
import java.awt.ComponentOrientation;
import javax.swing.JPanel;
import javax.swing.border.Border;

public abstract class AbstractButtonPanelBuilder
{
  private final JPanel container;
  private final FormLayout layout;
  private final CellConstraints currentCellConstraints;
  private boolean leftToRight;

  protected AbstractButtonPanelBuilder(FormLayout layout, JPanel container)
  {
    if (layout == null) {
      throw new NullPointerException("The layout must not be null.");
    }
    if (container == null) {
      throw new NullPointerException("The layout container must not be null.");
    }
    this.container = container;
    this.layout = layout;

    container.setLayout(layout);
    this.currentCellConstraints = new CellConstraints();
    ComponentOrientation orientation = container.getComponentOrientation();
    this.leftToRight = ((orientation.isLeftToRight()) || (!orientation.isHorizontal()));
  }

  public final JPanel getContainer()
  {
    return this.container;
  }

  public final JPanel getPanel()
  {
    return getContainer();
  }

  public final FormLayout getLayout()
  {
    return this.layout;
  }

  public final void setBackground(Color background)
  {
    getPanel().setBackground(background);
  }

  public final void setBorder(Border border)
  {
    getPanel().setBorder(border);
  }

  public final void setOpaque(boolean b)
  {
    getPanel().setOpaque(b);
  }

  public final boolean isLeftToRight()
  {
    return this.leftToRight;
  }

  public final void setLeftToRight(boolean b)
  {
    this.leftToRight = b;
  }

  protected final void nextColumn()
  {
    nextColumn(1);
  }

  private void nextColumn(int columns)
  {
    this.currentCellConstraints.gridX += columns * getColumnIncrementSign();
  }

  protected final void nextRow()
  {
    nextRow(1);
  }

  private void nextRow(int rows)
  {
    this.currentCellConstraints.gridY += rows;
  }

  protected final void appendColumn(ColumnSpec columnSpec)
  {
    getLayout().appendColumn(columnSpec);
  }

  protected final void appendGlueColumn()
  {
    appendColumn(FormFactory.GLUE_COLSPEC);
  }

  protected final void appendRelatedComponentsGapColumn()
  {
    appendColumn(FormFactory.RELATED_GAP_COLSPEC);
  }

  protected final void appendUnrelatedComponentsGapColumn()
  {
    appendColumn(FormFactory.UNRELATED_GAP_COLSPEC);
  }

  protected final void appendRow(RowSpec rowSpec)
  {
    getLayout().appendRow(rowSpec);
  }

  protected final void appendGlueRow()
  {
    appendRow(FormFactory.GLUE_ROWSPEC);
  }

  protected final void appendRelatedComponentsGapRow()
  {
    appendRow(FormFactory.RELATED_GAP_ROWSPEC);
  }

  protected final void appendUnrelatedComponentsGapRow()
  {
    appendRow(FormFactory.UNRELATED_GAP_ROWSPEC);
  }

  protected final Component add(Component component)
  {
    this.container.add(component, this.currentCellConstraints);
    return component;
  }

  private int getColumnIncrementSign()
  {
    return isLeftToRight() ? 1 : -1;
  }
}