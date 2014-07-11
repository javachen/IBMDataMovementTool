package com.jgoodies.forms.builder;

import com.jgoodies.forms.factories.FormFactory;
import com.jgoodies.forms.layout.CellConstraints;
import com.jgoodies.forms.layout.CellConstraints.Alignment;
import com.jgoodies.forms.layout.ColumnSpec;
import com.jgoodies.forms.layout.FormLayout;
import com.jgoodies.forms.layout.RowSpec;
import java.awt.Component;
import java.awt.ComponentOrientation;
import java.awt.Container;

public abstract class AbstractFormBuilder
{
  private final Container container;
  private final FormLayout layout;
  private final CellConstraints currentCellConstraints;
  private boolean leftToRight;

  public AbstractFormBuilder(FormLayout layout, Container container)
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

  public final Container getContainer()
  {
    return this.container;
  }

  public final FormLayout getLayout()
  {
    return this.layout;
  }

  public final int getColumnCount()
  {
    return getLayout().getColumnCount();
  }

  public final int getRowCount()
  {
    return getLayout().getRowCount();
  }

  public final boolean isLeftToRight()
  {
    return this.leftToRight;
  }

  public final void setLeftToRight(boolean b)
  {
    this.leftToRight = b;
  }

  public final int getColumn()
  {
    return this.currentCellConstraints.gridX;
  }

  public final void setColumn(int column)
  {
    this.currentCellConstraints.gridX = column;
  }

  public final int getRow()
  {
    return this.currentCellConstraints.gridY;
  }

  public final void setRow(int row)
  {
    this.currentCellConstraints.gridY = row;
  }

  public final void setColumnSpan(int columnSpan)
  {
    this.currentCellConstraints.gridWidth = columnSpan;
  }

  public final void setRowSpan(int rowSpan)
  {
    this.currentCellConstraints.gridHeight = rowSpan;
  }

  public final void setOrigin(int column, int row)
  {
    setColumn(column);
    setRow(row);
  }

  public final void setExtent(int columnSpan, int rowSpan)
  {
    setColumnSpan(columnSpan);
    setRowSpan(rowSpan);
  }

  public final void setBounds(int column, int row, int columnSpan, int rowSpan)
  {
    setColumn(column);
    setRow(row);
    setColumnSpan(columnSpan);
    setRowSpan(rowSpan);
  }

  public final void nextColumn()
  {
    nextColumn(1);
  }

  public final void nextColumn(int columns)
  {
    this.currentCellConstraints.gridX += columns * getColumnIncrementSign();
  }

  public final void nextRow()
  {
    nextRow(1);
  }

  public final void nextRow(int rows)
  {
    this.currentCellConstraints.gridY += rows;
  }

  public final void nextLine()
  {
    nextLine(1);
  }

  public final void nextLine(int lines)
  {
    nextRow(lines);
    setColumn(getLeadingColumn());
  }

  public final void setHAlignment(CellConstraints.Alignment alignment)
  {
    this.currentCellConstraints.hAlign = alignment;
  }

  public final void setVAlignment(CellConstraints.Alignment alignment)
  {
    this.currentCellConstraints.vAlign = alignment;
  }

  public final void setAlignment(CellConstraints.Alignment hAlign, CellConstraints.Alignment vAlign)
  {
    setHAlignment(hAlign);
    setVAlignment(vAlign);
  }

  public final void appendColumn(ColumnSpec columnSpec)
  {
    getLayout().appendColumn(columnSpec);
  }

  public final void appendColumn(String encodedColumnSpec)
  {
    appendColumn(ColumnSpec.decode(encodedColumnSpec));
  }

  public final void appendGlueColumn()
  {
    appendColumn(FormFactory.GLUE_COLSPEC);
  }

  public final void appendLabelComponentsGapColumn()
  {
    appendColumn(FormFactory.LABEL_COMPONENT_GAP_COLSPEC);
  }

  public final void appendRelatedComponentsGapColumn()
  {
    appendColumn(FormFactory.RELATED_GAP_COLSPEC);
  }

  public final void appendUnrelatedComponentsGapColumn()
  {
    appendColumn(FormFactory.UNRELATED_GAP_COLSPEC);
  }

  public final void appendRow(RowSpec rowSpec)
  {
    getLayout().appendRow(rowSpec);
  }

  public final void appendRow(String encodedRowSpec)
  {
    appendRow(RowSpec.decode(encodedRowSpec));
  }

  public final void appendGlueRow()
  {
    appendRow(FormFactory.GLUE_ROWSPEC);
  }

  public final void appendRelatedComponentsGapRow()
  {
    appendRow(FormFactory.RELATED_GAP_ROWSPEC);
  }

  public final void appendUnrelatedComponentsGapRow()
  {
    appendRow(FormFactory.UNRELATED_GAP_ROWSPEC);
  }

  public final void appendParagraphGapRow()
  {
    appendRow(FormFactory.PARAGRAPH_GAP_ROWSPEC);
  }

  public Component add(Component component, CellConstraints cellConstraints)
  {
    this.container.add(component, cellConstraints);
    return component;
  }

  public final Component add(Component component, String encodedCellConstraints)
  {
    this.container.add(component, new CellConstraints(encodedCellConstraints));
    return component;
  }

  public final Component add(Component component)
  {
    add(component, this.currentCellConstraints);
    return component;
  }

  protected final CellConstraints cellConstraints()
  {
    return this.currentCellConstraints;
  }

  protected int getLeadingColumn()
  {
    return isLeftToRight() ? 1 : getColumnCount();
  }

  protected final int getColumnIncrementSign()
  {
    return isLeftToRight() ? 1 : -1;
  }

  protected final CellConstraints createLeftAdjustedConstraints(int columnSpan)
  {
    int firstColumn = isLeftToRight() ? getColumn() : getColumn() + 1 - columnSpan;

    return new CellConstraints(firstColumn, getRow(), columnSpan, cellConstraints().gridHeight);
  }
}