package ibm;

import javax.swing.LookAndFeel;

import com.jgoodies.looks.BorderStyle;
import com.jgoodies.looks.HeaderStyle;
import com.jgoodies.looks.plastic.PlasticLookAndFeel;
import com.jgoodies.looks.plastic.PlasticTheme;
import com.jgoodies.looks.plastic.PlasticXPLookAndFeel;

public class Settings {
	private LookAndFeel selectedLookAndFeel;
	private PlasticTheme selectedTheme;
	private boolean useNarrowButtons;
	private boolean tabIconsEnabled;
	private String plasticTabStyle;
	private boolean plasticHighContrastFocusEnabled;
	private Boolean popupDropShadowEnabled;
	private HeaderStyle menuBarHeaderStyle;
	private BorderStyle menuBarPlasticBorderStyle;
	private BorderStyle menuBarWindowsBorderStyle;
	private Boolean menuBar3DHint;
	private HeaderStyle toolBarHeaderStyle;
	private BorderStyle toolBarPlasticBorderStyle;
	private BorderStyle toolBarWindowsBorderStyle;
	private Boolean toolBar3DHint;

	public static Settings createDefault() {
		Settings settings = new Settings();
		settings.setSelectedLookAndFeel(new PlasticXPLookAndFeel());
		settings.setSelectedTheme(PlasticLookAndFeel.createMyDefaultTheme());
		settings.setUseNarrowButtons(true);
		settings.setTabIconsEnabled(true);
		settings.setPlasticTabStyle("default");
		settings.setPlasticHighContrastFocusEnabled(false);
		settings.setPopupDropShadowEnabled(null);
		settings.setMenuBarHeaderStyle(null);
		settings.setMenuBarPlasticBorderStyle(null);
		settings.setMenuBarWindowsBorderStyle(null);
		settings.setMenuBar3DHint(null);
		settings.setToolBarHeaderStyle(null);
		settings.setToolBarPlasticBorderStyle(null);
		settings.setToolBarWindowsBorderStyle(null);
		settings.setToolBar3DHint(null);
		return settings;
	}

	public Boolean getMenuBar3DHint() {
		return this.menuBar3DHint;
	}

	public void setMenuBar3DHint(Boolean menuBar3DHint) {
		this.menuBar3DHint = menuBar3DHint;
	}

	public HeaderStyle getMenuBarHeaderStyle() {
		return this.menuBarHeaderStyle;
	}

	public void setMenuBarHeaderStyle(HeaderStyle menuBarHeaderStyle) {
		this.menuBarHeaderStyle = menuBarHeaderStyle;
	}

	public BorderStyle getMenuBarPlasticBorderStyle() {
		return this.menuBarPlasticBorderStyle;
	}

	public void setMenuBarPlasticBorderStyle(
			BorderStyle menuBarPlasticBorderStyle) {
		this.menuBarPlasticBorderStyle = menuBarPlasticBorderStyle;
	}

	public BorderStyle getMenuBarWindowsBorderStyle() {
		return this.menuBarWindowsBorderStyle;
	}

	public void setMenuBarWindowsBorderStyle(
			BorderStyle menuBarWindowsBorderStyle) {
		this.menuBarWindowsBorderStyle = menuBarWindowsBorderStyle;
	}

	public Boolean isPopupDropShadowEnabled() {
		return this.popupDropShadowEnabled;
	}

	public void setPopupDropShadowEnabled(Boolean popupDropShadowEnabled) {
		this.popupDropShadowEnabled = popupDropShadowEnabled;
	}

	public boolean isPlasticHighContrastFocusEnabled() {
		return this.plasticHighContrastFocusEnabled;
	}

	public void setPlasticHighContrastFocusEnabled(
			boolean plasticHighContrastFocusEnabled) {
		this.plasticHighContrastFocusEnabled = plasticHighContrastFocusEnabled;
	}

	public String getPlasticTabStyle() {
		return this.plasticTabStyle;
	}

	public void setPlasticTabStyle(String plasticTabStyle) {
		this.plasticTabStyle = plasticTabStyle;
	}

	public LookAndFeel getSelectedLookAndFeel() {
		return this.selectedLookAndFeel;
	}

	public void setSelectedLookAndFeel(LookAndFeel selectedLookAndFeel) {
		this.selectedLookAndFeel = selectedLookAndFeel;
	}

	public void setSelectedLookAndFeel(String selectedLookAndFeelClassName) {
		try {
			Class<?> theClass = Class.forName(selectedLookAndFeelClassName);
			setSelectedLookAndFeel((LookAndFeel) theClass.newInstance());
		} catch (Exception e) {
			System.out.println("Can't instantiate "
					+ selectedLookAndFeelClassName);
			e.printStackTrace();
		}
	}

	public PlasticTheme getSelectedTheme() {
		return this.selectedTheme;
	}

	public void setSelectedTheme(PlasticTheme selectedTheme) {
		this.selectedTheme = selectedTheme;
	}

	public boolean isTabIconsEnabled() {
		return this.tabIconsEnabled;
	}

	public void setTabIconsEnabled(boolean tabIconsEnabled) {
		this.tabIconsEnabled = tabIconsEnabled;
	}

	public Boolean getToolBar3DHint() {
		return this.toolBar3DHint;
	}

	public void setToolBar3DHint(Boolean toolBar3DHint) {
		this.toolBar3DHint = toolBar3DHint;
	}

	public HeaderStyle getToolBarHeaderStyle() {
		return this.toolBarHeaderStyle;
	}

	public void setToolBarHeaderStyle(HeaderStyle toolBarHeaderStyle) {
		this.toolBarHeaderStyle = toolBarHeaderStyle;
	}

	public BorderStyle getToolBarPlasticBorderStyle() {
		return this.toolBarPlasticBorderStyle;
	}

	public void setToolBarPlasticBorderStyle(
			BorderStyle toolBarPlasticBorderStyle) {
		this.toolBarPlasticBorderStyle = toolBarPlasticBorderStyle;
	}

	public BorderStyle getToolBarWindowsBorderStyle() {
		return this.toolBarWindowsBorderStyle;
	}

	public void setToolBarWindowsBorderStyle(
			BorderStyle toolBarWindowsBorderStyle) {
		this.toolBarWindowsBorderStyle = toolBarWindowsBorderStyle;
	}

	public boolean isUseNarrowButtons() {
		return this.useNarrowButtons;
	}

	public void setUseNarrowButtons(boolean useNarrowButtons) {
		this.useNarrowButtons = useNarrowButtons;
	}
}