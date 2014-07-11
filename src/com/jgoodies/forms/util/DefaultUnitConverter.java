package com.jgoodies.forms.util;

import java.awt.Component;
import java.awt.Font;
import java.awt.FontMetrics;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.UIManager;

public final class DefaultUnitConverter extends AbstractUnitConverter
{
  public static final String PROPERTY_AVERAGE_CHARACTER_WIDTH_TEST_STRING = "averageCharacterWidthTestString";
  public static final String PROPERTY_DEFAULT_DIALOG_FONT = "defaultDialogFont";
  private static final Logger LOGGER = Logger.getLogger(DefaultUnitConverter.class.getName());
  private static DefaultUnitConverter instance;
  private String averageCharWidthTestString = "X";
  private Font defaultDialogFont;
  private final PropertyChangeSupport changeSupport;
  private DialogBaseUnits cachedGlobalDialogBaseUnits = null;

  private final Map cachedDialogBaseUnits = new HashMap();

  private Font cachedDefaultDialogFont = null;

  private DefaultUnitConverter()
  {
    this.changeSupport = new PropertyChangeSupport(this);
  }

  public static DefaultUnitConverter getInstance()
  {
    if (instance == null) {
      instance = new DefaultUnitConverter();
    }
    return instance;
  }

  public String getAverageCharacterWidthTestString()
  {
    return this.averageCharWidthTestString;
  }

  public void setAverageCharacterWidthTestString(String newTestString)
  {
    if (newTestString == null)
      throw new NullPointerException("The test string must not be null.");
    if (newTestString.length() == 0) {
      throw new IllegalArgumentException("The test string must not be empty.");
    }
    String oldTestString = this.averageCharWidthTestString;
    this.averageCharWidthTestString = newTestString;
    this.changeSupport.firePropertyChange("averageCharacterWidthTestString", oldTestString, newTestString);
  }

  public Font getDefaultDialogFont()
  {
    return this.defaultDialogFont != null ? this.defaultDialogFont : getCachedDefaultDialogFont();
  }

  public void setDefaultDialogFont(Font newFont)
  {
    Font oldFont = this.defaultDialogFont;
    this.defaultDialogFont = newFont;
    clearCache();
    this.changeSupport.firePropertyChange("defaultDialogFont", oldFont, newFont);
  }

  protected double getDialogBaseUnitsX(Component component)
  {
    return getDialogBaseUnits(component).x;
  }

  protected double getDialogBaseUnitsY(Component component)
  {
    return getDialogBaseUnits(component).y;
  }

  private DialogBaseUnits getGlobalDialogBaseUnits()
  {
    if (this.cachedGlobalDialogBaseUnits == null) {
      this.cachedGlobalDialogBaseUnits = computeGlobalDialogBaseUnits();
    }
    return this.cachedGlobalDialogBaseUnits;
  }

  private DialogBaseUnits getDialogBaseUnits(Component c)
  {
    FormUtils.ensureValidCache();
    if (c == null)
    {
      return getGlobalDialogBaseUnits();
    }
    FontMetrics fm = c.getFontMetrics(getDefaultDialogFont());
    DialogBaseUnits dialogBaseUnits = (DialogBaseUnits)this.cachedDialogBaseUnits.get(fm);

    if (dialogBaseUnits == null) {
      dialogBaseUnits = computeDialogBaseUnits(fm);
      this.cachedDialogBaseUnits.put(fm, dialogBaseUnits);
    }
    return dialogBaseUnits;
  }

  private DialogBaseUnits computeDialogBaseUnits(FontMetrics metrics)
  {
    double averageCharWidth = computeAverageCharWidth(metrics, this.averageCharWidthTestString);

    int ascent = metrics.getAscent();
    double height = ascent + (15 - ascent) / 3;
    DialogBaseUnits dialogBaseUnits = new DialogBaseUnits(averageCharWidth, height);

    LOGGER.config("Computed dialog base units " + dialogBaseUnits + " for: " + metrics.getFont());

    return dialogBaseUnits;
  }

  private DialogBaseUnits computeGlobalDialogBaseUnits()
  {
    LOGGER.config("Computing global dialog base units...");
    Font dialogFont = getDefaultDialogFont();
    FontMetrics metrics = createDefaultGlobalComponent().getFontMetrics(dialogFont);
    DialogBaseUnits globalDialogBaseUnits = computeDialogBaseUnits(metrics);
    return globalDialogBaseUnits;
  }

  private Font getCachedDefaultDialogFont()
  {
    FormUtils.ensureValidCache();
    if (this.cachedDefaultDialogFont == null) {
      this.cachedDefaultDialogFont = lookupDefaultDialogFont();
    }
    return this.cachedDefaultDialogFont;
  }

  private Font lookupDefaultDialogFont()
  {
    Font buttonFont = UIManager.getFont("Button.font");
    return buttonFont != null ? buttonFont : new JButton().getFont();
  }

  private Component createDefaultGlobalComponent()
  {
    return new JPanel(null);
  }

  void clearCache()
  {
    this.cachedGlobalDialogBaseUnits = null;
    this.cachedDialogBaseUnits.clear();
    this.cachedDefaultDialogFont = null;
  }

  public synchronized void addPropertyChangeListener(PropertyChangeListener listener)
  {
    this.changeSupport.addPropertyChangeListener(listener);
  }

  public synchronized void removePropertyChangeListener(PropertyChangeListener listener)
  {
    this.changeSupport.removePropertyChangeListener(listener);
  }

  public synchronized void addPropertyChangeListener(String propertyName, PropertyChangeListener listener)
  {
    this.changeSupport.addPropertyChangeListener(propertyName, listener);
  }

  public synchronized void removePropertyChangeListener(String propertyName, PropertyChangeListener listener)
  {
    this.changeSupport.removePropertyChangeListener(propertyName, listener);
  }

  private static final class DialogBaseUnits
  {
    final double x;
    final double y;

    DialogBaseUnits(double dialogBaseUnitsX, double dialogBaseUnitsY)
    {
      this.x = dialogBaseUnitsX;
      this.y = dialogBaseUnitsY;
    }

    public String toString() {
      return "DBU(x=" + this.x + "; y=" + this.y + ")";
    }
  }
}