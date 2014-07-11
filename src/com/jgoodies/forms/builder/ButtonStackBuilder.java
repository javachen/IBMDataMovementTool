package com.jgoodies.forms.builder;

import com.jgoodies.forms.factories.FormFactory;
import com.jgoodies.forms.layout.ColumnSpec;
import com.jgoodies.forms.layout.ConstantSize;
import com.jgoodies.forms.layout.FormLayout;
import com.jgoodies.forms.layout.RowSpec;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JPanel;

public final class ButtonStackBuilder extends PanelBuilder
{
  private static final ColumnSpec[] COL_SPECS = { FormFactory.BUTTON_COLSPEC };

  private static final RowSpec[] ROW_SPECS = new RowSpec[0];
  private static final String NARROW_KEY = "jgoodies.isNarrow";

  public ButtonStackBuilder()
  {
    this(new JPanel(null));
  }

  public ButtonStackBuilder(JPanel panel)
  {
    this(new FormLayout(COL_SPECS, ROW_SPECS), panel);
  }

  public ButtonStackBuilder(FormLayout layout, JPanel panel)
  {
    super(layout, panel);
  }

  public void addButtons(JButton[] buttons)
  {
    for (int i = 0; i < buttons.length; i++) {
      addGridded(buttons[i]);
      if (i < buttons.length - 1)
        addRelatedGap();
    }
  }

  public void addFixed(JComponent component)
  {
    getLayout().appendRow(FormFactory.PREF_ROWSPEC);
    add(component);
    nextRow();
  }

  public void addGridded(JComponent component)
  {
    getLayout().appendRow(FormFactory.PREF_ROWSPEC);
    getLayout().addGroupedRow(getRow());
    component.putClientProperty("jgoodies.isNarrow", Boolean.TRUE);
    add(component);
    nextRow();
  }

  public void addGlue()
  {
    appendGlueRow();
    nextRow();
  }

  public void addRelatedGap()
  {
    appendRelatedComponentsGapRow();
    nextRow();
  }

  public void addUnrelatedGap()
  {
    appendUnrelatedComponentsGapRow();
    nextRow();
  }

  public void addStrut(ConstantSize size)
  {
    getLayout().appendRow(new RowSpec(RowSpec.TOP, size, 0.0D));

    nextRow();
  }
}