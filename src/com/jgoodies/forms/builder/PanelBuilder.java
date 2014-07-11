package com.jgoodies.forms.builder;

import com.jgoodies.forms.factories.Borders;
import com.jgoodies.forms.factories.ComponentFactory;
import com.jgoodies.forms.factories.DefaultComponentFactory;
import com.jgoodies.forms.layout.CellConstraints;
import com.jgoodies.forms.layout.FormLayout;
import java.awt.Color;
import java.awt.Component;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.border.Border;

public class PanelBuilder extends AbstractFormBuilder
{
  private ComponentFactory componentFactory;

  public PanelBuilder(FormLayout layout)
  {
    this(layout, new JPanel(null));
  }

  public PanelBuilder(FormLayout layout, JPanel panel)
  {
    super(layout, panel);
  }

  public final JPanel getPanel()
  {
    return (JPanel)getContainer();
  }

  public final void setBackground(Color background)
  {
    getPanel().setBackground(background);
  }

  public final void setBorder(Border border)
  {
    getPanel().setBorder(border);
  }

  public final void setDefaultDialogBorder()
  {
    setBorder(Borders.DIALOG_BORDER);
  }

  public final void setOpaque(boolean b)
  {
    getPanel().setOpaque(b);
  }

  public final JLabel addLabel(String textWithMnemonic)
  {
    return addLabel(textWithMnemonic, cellConstraints());
  }

  public final JLabel addLabel(String textWithMnemonic, CellConstraints constraints)
  {
    JLabel label = getComponentFactory().createLabel(textWithMnemonic);
    add(label, constraints);
    return label;
  }

  public final JLabel addLabel(String textWithMnemonic, String encodedConstraints)
  {
    return addLabel(textWithMnemonic, new CellConstraints(encodedConstraints));
  }

  public final JLabel add(JLabel label, CellConstraints labelConstraints, Component component, CellConstraints componentConstraints)
  {
    if (labelConstraints == componentConstraints) {
      throw new IllegalArgumentException("You must provide two CellConstraints instances, one for the label and one for the component.\nConsider using #clone(). See the JavaDocs for details.");
    }

    add(label, labelConstraints);
    add(component, componentConstraints);
    label.setLabelFor(component);
    return label;
  }

  public final JLabel addLabel(String textWithMnemonic, CellConstraints labelConstraints, Component component, CellConstraints componentConstraints)
  {
    if (labelConstraints == componentConstraints) {
      throw new IllegalArgumentException("You must provide two CellConstraints instances, one for the label and one for the component.\nConsider using #clone(). See the JavaDocs for details.");
    }

    JLabel label = addLabel(textWithMnemonic, labelConstraints);
    add(component, componentConstraints);
    label.setLabelFor(component);
    return label;
  }

  public final JLabel addTitle(String textWithMnemonic)
  {
    return addTitle(textWithMnemonic, cellConstraints());
  }

  public final JLabel addTitle(String textWithMnemonic, CellConstraints constraints)
  {
    JLabel titleLabel = getComponentFactory().createTitle(textWithMnemonic);
    add(titleLabel, constraints);
    return titleLabel;
  }

  public final JLabel addTitle(String textWithMnemonic, String encodedConstraints)
  {
    return addTitle(textWithMnemonic, new CellConstraints(encodedConstraints));
  }

  public final JComponent addSeparator(String textWithMnemonic)
  {
    return addSeparator(textWithMnemonic, getLayout().getColumnCount());
  }

  public final JComponent addSeparator(String textWithMnemonic, CellConstraints constraints)
  {
    int titleAlignment = isLeftToRight() ? 2 : 4;

    JComponent titledSeparator = getComponentFactory().createSeparator(textWithMnemonic, titleAlignment);

    add(titledSeparator, constraints);
    return titledSeparator;
  }

  public final JComponent addSeparator(String textWithMnemonic, String encodedConstraints)
  {
    return addSeparator(textWithMnemonic, new CellConstraints(encodedConstraints));
  }

  public final JComponent addSeparator(String textWithMnemonic, int columnSpan)
  {
    return addSeparator(textWithMnemonic, createLeftAdjustedConstraints(columnSpan));
  }

  public final ComponentFactory getComponentFactory()
  {
    if (this.componentFactory == null) {
      this.componentFactory = DefaultComponentFactory.getInstance();
    }
    return this.componentFactory;
  }

  public final void setComponentFactory(ComponentFactory newFactory)
  {
    this.componentFactory = newFactory;
  }
}