package com.jgoodies.forms.builder;

import com.jgoodies.forms.factories.Borders;
import com.jgoodies.forms.factories.FormFactory;
import com.jgoodies.forms.layout.ColumnSpec;
import com.jgoodies.forms.layout.ConstantSize;
import com.jgoodies.forms.layout.FormLayout;
import com.jgoodies.forms.layout.RowSpec;
import com.jgoodies.forms.util.LayoutStyle;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JPanel;

public final class ButtonBarBuilder extends PanelBuilder
{
  private static final ColumnSpec[] COL_SPECS = new ColumnSpec[0];

  private static final RowSpec[] ROW_SPECS = { RowSpec.decode("center:pref") };
  private static final String NARROW_KEY = "jgoodies.isNarrow";
  private boolean leftToRight;

  public ButtonBarBuilder()
  {
    this(new JPanel(null));
  }

  public ButtonBarBuilder(JPanel panel)
  {
    super(new FormLayout(COL_SPECS, ROW_SPECS), panel);
    this.leftToRight = LayoutStyle.getCurrent().isLeftToRightButtonOrder();
  }

  public static ButtonBarBuilder createLeftToRightBuilder()
  {
    ButtonBarBuilder builder = new ButtonBarBuilder();
    builder.setLeftToRightButtonOrder(true);
    return builder;
  }

  public boolean isLeftToRightButtonOrder()
  {
    return this.leftToRight;
  }

  public void setLeftToRightButtonOrder(boolean newButtonOrder)
  {
    this.leftToRight = newButtonOrder;
  }

  public void setDefaultButtonBarGapBorder()
  {
    setBorder(Borders.BUTTON_BAR_GAP_BORDER);
  }

  public void addGriddedButtons(JButton[] buttons)
  {
    int length = buttons.length;
    for (int i = 0; i < length; i++) {
      int index = this.leftToRight ? i : length - 1 - i;
      addGridded(buttons[index]);
      if (i < buttons.length - 1)
        addRelatedGap();
    }
  }

  public void addGriddedGrowingButtons(JButton[] buttons)
  {
    int length = buttons.length;
    for (int i = 0; i < length; i++) {
      int index = this.leftToRight ? i : length - 1 - i;
      addGriddedGrowing(buttons[index]);
      if (i < buttons.length - 1)
        addRelatedGap();
    }
  }

  public void addFixed(JComponent component)
  {
    getLayout().appendColumn(FormFactory.PREF_COLSPEC);
    add(component);
    nextColumn();
  }

  public void addFixedNarrow(JComponent component)
  {
    component.putClientProperty("jgoodies.isNarrow", Boolean.TRUE);
    addFixed(component);
  }

  public void addGridded(JComponent component)
  {
    getLayout().appendColumn(FormFactory.BUTTON_COLSPEC);
    getLayout().addGroupedColumn(getColumn());
    component.putClientProperty("jgoodies.isNarrow", Boolean.TRUE);
    add(component);
    nextColumn();
  }

  public void addGriddedGrowing(JComponent component)
  {
    getLayout().appendColumn(FormFactory.GROWING_BUTTON_COLSPEC);
    getLayout().addGroupedColumn(getColumn());
    component.putClientProperty("jgoodies.isNarrow", Boolean.TRUE);
    add(component);
    nextColumn();
  }

  public void addGlue()
  {
    appendGlueColumn();
    nextColumn();
  }

  public void addRelatedGap()
  {
    appendRelatedComponentsGapColumn();
    nextColumn();
  }

  public void addUnrelatedGap()
  {
    appendUnrelatedComponentsGapColumn();
    nextColumn();
  }

  public void addStrut(ConstantSize width)
  {
    getLayout().appendColumn(ColumnSpec.createGap(width));
    nextColumn();
  }
}