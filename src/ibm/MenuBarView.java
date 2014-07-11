package ibm;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.Icon;
import javax.swing.ImageIcon;
import javax.swing.JCheckBoxMenuItem;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JRadioButtonMenuItem;
import javax.swing.KeyStroke;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

public class MenuBarView {

	public MenuBarView() {
		optionMenu = new JCheckBoxMenuItem[10];
		deployMenu = new JCheckBoxMenuItem[18];
		cfg = null;
	}

	JMenuBar buildMenuBar(Settings settings, IBMExtractConfig cfg,
			String optionCodes[][], String deployCodes[][],
			ActionListener helpActionListener,
			ActionListener aboutActionListener,
			ActionListener getNewVersionActionListener,
			ActionListener executeAllListener, ActionListener executeListener,
			ActionListener revalidateListener,
			ActionListener db2FileOpenActionListener,
			ActionListener revalidateAllListener,
			ActionListener refreshListener, ActionListener discardListener) {
		this.cfg = cfg;
		this.optionCodes = optionCodes;
		this.deployCodes = deployCodes;
		JMenuBar bar = new JMenuBar();
		bar.putClientProperty("jgoodies.headerStyle", settings
				.getMenuBarHeaderStyle());
		bar.putClientProperty("Plastic.borderStyle", settings
				.getMenuBarPlasticBorderStyle());
		bar.putClientProperty("jgoodies.windows.borderStyle", settings
				.getMenuBarWindowsBorderStyle());
		bar.putClientProperty("Plastic.is3D", settings.getMenuBar3DHint());
		bar.add(buildFileMenu(executeAllListener, executeListener,
				revalidateListener, db2FileOpenActionListener,
				revalidateAllListener, refreshListener, discardListener));
		bar.add(buildOptionMenu());
		bar.add(buildDeployMenu());
		bar.add(buildHelpMenu(helpActionListener, aboutActionListener,
				getNewVersionActionListener));
		return bar;
	}

	private JMenu buildFileMenu(ActionListener executeAllListener,
			ActionListener executeListener, ActionListener revalidateListener,
			ActionListener db2FileOpenActionListener,
			ActionListener revalidateAllListener,
			ActionListener refreshListener, ActionListener discardListener) {
		JMenu menu = createMenu("File", 'F');
		JMenuItem item = createMenuItem("Select DB2 Objects Directory",
				readImageIcon("open.gif"), 'O', KeyStroke
						.getKeyStroke("ctrl O"));
		if (db2FileOpenActionListener != null)
			item.addActionListener(db2FileOpenActionListener);
		menu.add(item);
		menuRefresh = createMenuItem("Refresh objects\u2026",
				readImageIcon("valid.gif"), 'L', KeyStroke
						.getKeyStroke("ctrl L"));
		if (refreshListener != null)
			menuRefresh.addActionListener(refreshListener);
		menuRefresh.setEnabled(false);
		menu.add(menuRefresh);
		menu.addSeparator();
		menuExecuteAll = createMenuItem("Execute All Statements\u2026",
				readImageIcon("srcdb.png"), 'A', KeyStroke
						.getKeyStroke("ctrl A"));
		if (executeAllListener != null)
			menuExecuteAll.addActionListener(executeAllListener);
		menuExecuteAll.setEnabled(false);
		menu.add(menuExecuteAll);
		menuExecute = createMenuItem("Execute Selected Statements\u2026",
				readImageIcon("dstdb.png"), 'E', KeyStroke
						.getKeyStroke("ctrl E"));
		if (executeListener != null)
			menuExecute.addActionListener(executeListener);
		menuExecute.setEnabled(false);
		menu.add(menuExecute);
		menu.addSeparator();
		menuRevalidateAll = createMenuItem("Revalidate All Statements\u2026",
				readImageIcon("revalidate.png"), 'R', KeyStroke
						.getKeyStroke("ctrl R"));
		if (revalidateAllListener != null)
			menuRevalidateAll.addActionListener(revalidateAllListener);
		menuRevalidateAll.setEnabled(false);
		menu.add(menuRevalidateAll);
		menuRevalidate = createMenuItem("Revalidate Selected Statements\u2026",
				readImageIcon("valid.gif"), 'H', KeyStroke
						.getKeyStroke("ctrl H"));
		if (revalidateListener != null)
			menuRevalidate.addActionListener(revalidateListener);
		menuRevalidate.setEnabled(false);
		menu.add(menuRevalidate);
		menu.addSeparator();
		menuDiscard = createMenuItem("Do not deploy these objects\u2026",
				readImageIcon("valid.gif"), 'C', KeyStroke
						.getKeyStroke("ctrl C"));
		if (discardListener != null)
			menuDiscard.addActionListener(discardListener);
		menuDiscard.setEnabled(false);
		menu.add(menuDiscard);
		if (!isQuitInOSMenu()) {
			menu.addSeparator();
			item = createMenuItem("Exit", 'x');
			menu.add(item);
			item.addActionListener(new ActionListener() {

				public void actionPerformed(ActionEvent e) {
					System.exit(1);
				}
			});
		}
		return menu;
	}

	private void setSubMenu(String cbName, final int idx,
			final String menuHeading, JMenu menu,
			final JCheckBoxMenuItem subMenu[], final String codes[][]) {
		subMenu[idx] = createCheckBoxMenuItem(menuHeading, false);
		subMenu[idx].setName(cbName);
		subMenu[idx].setEnabled(true);
		subMenu[idx].addChangeListener(new ChangeListener() {

			public void stateChanged(ChangeEvent e) {
				JCheckBoxMenuItem source = (JCheckBoxMenuItem) e.getSource();
				if (source.isEnabled())
					source.setText(menuHeading);
			}

		});
		subMenu[idx].setIcon(readImageIcon("check.gif"));
		subMenu[idx].setSelectedIcon(readImageIcon("check_selected.gif"));
		if (cbName.equals("OPTION_TRAILING_BLANKS"))
			subMenu[idx].setSelected(Boolean.valueOf(
					cfg.getTrimTrailingSpaces()).booleanValue());
		else if (cbName.equals("OPTION_DBCLOBS"))
			subMenu[idx].setSelected(Boolean.valueOf(cfg.getDbclob())
					.booleanValue());
		else if (cbName.equals("OPTION_GRAPHICS"))
			subMenu[idx].setSelected(Boolean.valueOf(cfg.getGraphic())
					.booleanValue());
		else if (cbName.equals("OPTION_SPLIT_TRIGGER"))
			subMenu[idx].setSelected(Boolean.valueOf(
					cfg.getRegenerateTriggers()).booleanValue());
		else if (cbName.equals("OPTION_COMPRESS_TABLE"))
			subMenu[idx].setSelected(Boolean.valueOf(cfg.getCompressTable())
					.booleanValue());
		else if (cbName.equals("OPTION_COMPRESS_INDEX"))
			subMenu[idx].setSelected(Boolean.valueOf(cfg.getCompressIndex())
					.booleanValue());
		else if (cbName.equals("OPTION_EXTRACT_PARTITIONS"))
			subMenu[idx].setSelected(Boolean
					.valueOf(cfg.getExtractPartitions()).booleanValue());
		else if (cbName.equals("OPTION_EXTRACT_HASH_PARTITIONS"))
			subMenu[idx].setSelected(Boolean.valueOf(
					cfg.getExtractHashPartitions()).booleanValue());
		else if (cbName.equals("OPTION_GENERATE_CONS_NAMES"))
			subMenu[idx].setSelected(!Boolean.valueOf(
					cfg.getRetainConstraintsName()).booleanValue());
		else if (cbName.equals("OPTION_USE_BESTPRACTICE_TSNAMES"))
			subMenu[idx].setSelected(Boolean.valueOf(
					cfg.getUseBestPracticeTSNames()).booleanValue());
		subMenu[idx].addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				boolean value = subMenu[idx].isSelected();
				codes[idx][1] = Boolean.toString(value);
				JCheckBoxMenuItem o = (JCheckBoxMenuItem) e.getSource();
				if (o.getName().equals("OPTION_TRAILING_BLANKS")) {
					cfg.setTrimTrailingSpaces(Boolean.toString(value));
					codes[MenuBarView.OPTION_TRAILING_BLANKS.intValue()][1] = Boolean
							.toString(value);
				} else if (o.getName().equals("OPTION_DBCLOBS")) {
					cfg.setDbclob(Boolean.toString(value));
					codes[MenuBarView.OPTION_DBCLOBS.intValue()][1] = Boolean
							.toString(value);
				} else if (o.getName().equals("OPTION_GRAPHICS")) {
					cfg.setGraphic(Boolean.toString(value));
					codes[MenuBarView.OPTION_GRAPHICS.intValue()][1] = Boolean
							.toString(value);
				} else if (o.getName().equals("OPTION_SPLIT_TRIGGER")) {
					cfg.setRegenerateTriggers(Boolean.toString(value));
					codes[MenuBarView.OPTION_SPLIT_TRIGGER.intValue()][1] = Boolean
							.toString(value);
				} else if (o.getName().equals("OPTION_COMPRESS_TABLE")) {
					cfg.setCompressTable(Boolean.toString(value));
					codes[MenuBarView.OPTION_COMPRESS_TABLE.intValue()][1] = Boolean
							.toString(value);
				} else if (o.getName().equals("OPTION_COMPRESS_INDEX")) {
					cfg.setCompressTable(Boolean.toString(value));
					codes[MenuBarView.OPTION_COMPRESS_INDEX.intValue()][1] = Boolean
							.toString(value);
				} else if (o.getName().equals("OPTION_EXTRACT_PARTITIONS")) {
					cfg.setExtractPartitions(Boolean.toString(value));
					codes[MenuBarView.OPTION_EXTRACT_PARTITIONS.intValue()][1] = Boolean
							.toString(value);
				} else if (o.getName().equals("OPTION_EXTRACT_HASH_PARTITIONS")) {
					cfg.setExtractHashPartitions(Boolean.toString(value));
					codes[MenuBarView.OPTION_EXTRACT_HASH_PARTITIONS.intValue()][1] = Boolean
							.toString(value);
				} else if (o.getName().equals("OPTION_GENERATE_CONS_NAMES")) {
					cfg.setRetainConstraintsName(Boolean.toString(!value));
					codes[MenuBarView.OPTION_GENERATE_CONS_NAMES.intValue()][1] = Boolean
							.toString(!value);
				} else if (o.getName()
						.equals("OPTION_USE_BESTPRACTICE_TSNAMES")) {
					cfg.setUseBestPracticeTSNames(Boolean.toString(value));
					codes[MenuBarView.OPTION_USE_BESTPRACTICE_TSNAMES
							.intValue()][1] = Boolean.toString(value);
				}
			}

		});
		menu.add(subMenu[idx]);
	}

	private JMenu buildOptionMenu() {
		JMenu menu = createMenu("Options", 'O');
		setSubMenu("OPTION_TRAILING_BLANKS", OPTION_TRAILING_BLANKS.intValue(),
				"Trim trailing blanks during unload", menu, optionMenu,
				optionCodes);
		setSubMenu("OPTION_DBCLOBS", OPTION_DBCLOBS.intValue(),
				"Turn DB CLOB to varchar during unload", menu, optionMenu,
				optionCodes);
		setSubMenu("OPTION_GRAPHICS", OPTION_GRAPHICS.intValue(),
				"Turn graphics char to normal char", menu, optionMenu,
				optionCodes);
		setSubMenu("OPTION_SPLIT_TRIGGER", OPTION_SPLIT_TRIGGER.intValue(),
				"Split multiple action Triggers", menu, optionMenu, optionCodes);
		setSubMenu("OPTION_COMPRESS_TABLE", OPTION_COMPRESS_TABLE.intValue(),
				"Compress Tables", menu, optionMenu, optionCodes);
		setSubMenu("OPTION_COMPRESS_INDEX", OPTION_COMPRESS_INDEX.intValue(),
				"Compress Index", menu, optionMenu, optionCodes);
		setSubMenu("OPTION_EXTRACT_PARTITIONS", OPTION_EXTRACT_PARTITIONS
				.intValue(), "Extract Partitions", menu, optionMenu,
				optionCodes);
		setSubMenu("OPTION_EXTRACT_HASH_PARTITIONS",
				OPTION_EXTRACT_HASH_PARTITIONS.intValue(),
				"Extract Hash Partitions", menu, optionMenu, optionCodes);
		setSubMenu("OPTION_GENERATE_CONS_NAMES", OPTION_GENERATE_CONS_NAMES
				.intValue(), "Use Generated Constraints Names", menu,
				optionMenu, optionCodes);
		setSubMenu("OPTION_USE_BESTPRACTICE_TSNAMES",
				OPTION_USE_BESTPRACTICE_TSNAMES.intValue(),
				"Use Best Practice Tablespace Definitions", menu, optionMenu,
				optionCodes);
		return menu;
	}

	private JMenu buildDeployMenu() {
		JMenu menu = createMenu("Deploy", 'Y');
		setSubMenu("DEPLOY_TSBP", DEPLOY_TSBP.intValue(),
				"Include BUFFER POOL/TABLE SPACE in interactive Deploy", menu,
				deployMenu, deployCodes);
		setSubMenu("DEPLOY_ROLE", DEPLOY_ROLE.intValue(),
				"Include ROLE in interactive Deploy", menu, deployMenu,
				deployCodes);
		setSubMenu("DEPLOY_SEQUENCE", DEPLOY_SEQUENCE.intValue(),
				"Include SEQUENCES in interactive Deploy", menu, deployMenu,
				deployCodes);
		setSubMenu("DEPLOY_TABLE", DEPLOY_TABLE.intValue(),
				"Include TABLES in interactive Deploy", menu, deployMenu,
				deployCodes);
		setSubMenu("DEPLOY_DEFAULT", DEPLOY_DEFAULT.intValue(),
				"Include DEFAULTS in interactive Deploy", menu, deployMenu,
				deployCodes);
		setSubMenu("DEPLOY_CHECK_CONSTRAINTS", DEPLOY_CHECK_CONSTRAINTS
				.intValue(), "Include CHECK CONSTRAINTS in interactive Deploy",
				menu, deployMenu, deployCodes);
		setSubMenu("DEPLOY_PRIMARY_KEY", DEPLOY_PRIMARY_KEY.intValue(),
				"Include PRIMARY KEYS in interactive Deploy", menu, deployMenu,
				deployCodes);
		setSubMenu("DEPLOY_UNIQUE_INDEX", DEPLOY_UNIQUE_INDEX.intValue(),
				"Include UNIQUE INDEXES in interactive Deploy", menu,
				deployMenu, deployCodes);
		setSubMenu("DEPLOY_INDEX", DEPLOY_INDEX.intValue(),
				"Include INDEXES in interactive Deploy", menu, deployMenu,
				deployCodes);
		setSubMenu("DEPLOY_FOREIGN_KEYS", DEPLOY_FOREIGN_KEYS.intValue(),
				"Include FOREIGN KEYS in interactive Deploy", menu, deployMenu,
				deployCodes);
		setSubMenu("DEPLOY_TYPE", DEPLOY_TYPE.intValue(),
				"Include TYPE in interactive Deploy", menu, deployMenu,
				deployCodes);
		setSubMenu("DEPLOY_FUNCTION", DEPLOY_FUNCTION.intValue(),
				"Include FUNCTIONS in interactive Deploy", menu, deployMenu,
				deployCodes);
		setSubMenu("DEPLOY_VIEW", DEPLOY_VIEW.intValue(),
				"Include VIEWS in interactive Deploy", menu, deployMenu,
				deployCodes);
		setSubMenu("DEPLOY_MQT", DEPLOY_MQT.intValue(),
				"Include MQT in interactive Deploy", menu, deployMenu,
				deployCodes);
		setSubMenu("DEPLOY_TRIGGER", DEPLOY_TRIGGER.intValue(),
				"Include TRIGGERS in interactive Deploy", menu, deployMenu,
				deployCodes);
		setSubMenu("DEPLOY_PROCEDURE", DEPLOY_PROCEDURE.intValue(),
				"Include PROCEDURES in interactive Deploy", menu, deployMenu,
				deployCodes);
		setSubMenu("DEPLOY_PACKAGE", DEPLOY_PACKAGE.intValue(),
				"Include PACKAGES in interactive Deploy", menu, deployMenu,
				deployCodes);
		setSubMenu("DEPLOY_PACKAGE_BODY", DEPLOY_PACKAGE_BODY.intValue(),
				"Include PACKAGE BODIES in interactive Deploy", menu,
				deployMenu, deployCodes);
		return menu;
	}

	private JMenu buildHelpMenu(ActionListener helpActionListener,
			ActionListener aboutActionListener,
			ActionListener getNewVersionActionListener) {
		JMenu menu = createMenu("Help", 'H');
		JMenuItem item = createMenuItem("Help Contents",
				readImageIcon("help.gif"), 'H');
		if (helpActionListener != null)
			item.addActionListener(helpActionListener);
		item = createMenuItem("Check New Version",
				readImageIcon("check_selected.gif"), 'C');
		if (getNewVersionActionListener != null)
			item.addActionListener(getNewVersionActionListener);
		menu.add(item);
		if (!isAboutInOSMenu()) {
			menu.addSeparator();
			item = createMenuItem("About", 'a');
			item.addActionListener(aboutActionListener);
			menu.add(item);
		}
		return menu;
	}

	protected JMenu createMenu(String text, char mnemonic) {
		JMenu menu = new JMenu(text);
		menu.setMnemonic(mnemonic);
		return menu;
	}

	protected JMenuItem createMenuItem(String text) {
		return new JMenuItem(text);
	}

	protected JMenuItem createMenuItem(String text, char mnemonic) {
		return new JMenuItem(text, mnemonic);
	}

	protected JMenuItem createMenuItem(String text, char mnemonic, KeyStroke key) {
		JMenuItem menuItem = new JMenuItem(text, mnemonic);
		menuItem.setAccelerator(key);
		return menuItem;
	}

	protected JMenuItem createMenuItem(String text, Icon icon) {
		return new JMenuItem(text, icon);
	}

	protected JMenuItem createMenuItem(String text, Icon icon, char mnemonic) {
		JMenuItem menuItem = new JMenuItem(text, icon);
		menuItem.setMnemonic(mnemonic);
		return menuItem;
	}

	protected JMenuItem createMenuItem(String text, Icon icon, char mnemonic,
			KeyStroke key) {
		JMenuItem menuItem = createMenuItem(text, icon, mnemonic);
		menuItem.setAccelerator(key);
		return menuItem;
	}

	protected JRadioButtonMenuItem createRadioButtonMenuItem(String text,
			boolean selected) {
		return new JRadioButtonMenuItem(text, selected);
	}

	protected JCheckBoxMenuItem createCheckBoxMenuItem(String text,
			boolean selected) {
		return new JCheckBoxMenuItem(text, selected);
	}

	protected boolean isQuitInOSMenu() {
		return false;
	}

	protected boolean isAboutInOSMenu() {
		return false;
	}

	private JCheckBoxMenuItem createCheckItem(boolean enabled, boolean selected) {
		JCheckBoxMenuItem item = createCheckBoxMenuItem(getToggleLabel(enabled,
				selected), selected);
		item.setEnabled(enabled);
		item.addChangeListener(new ChangeListener() {

			public void stateChanged(ChangeEvent e) {
				JCheckBoxMenuItem source = (JCheckBoxMenuItem) e.getSource();
				source.setText(getToggleLabel(source.isEnabled(), source
						.isSelected()));
			}
		});
		return item;
	}

	protected String getToggleLabel(boolean enabled, boolean selected) {
		String prefix = enabled ? "Enabled" : "Disabled";
		String suffix = selected ? "Selected" : "Deselected";
		return (new StringBuilder()).append(prefix).append(" and ").append(
				suffix).toString();
	}

	private ImageIcon readImageIcon(String filename) {
		java.net.URL url = getClass().getResource(
				(new StringBuilder()).append("resources/images/").append(
						filename).toString());
		return new ImageIcon(url);
	}

	public JMenuItem menuExecuteAll;
	public JMenuItem menuExecute;
	public JMenuItem menuRevalidate;
	public JMenuItem menuRevalidateAll;
	public JMenuItem menuRefresh;
	public JMenuItem menuDiscard;
	public static final Integer OPTION_TRAILING_BLANKS = new Integer(0);
	public static final Integer OPTION_DBCLOBS = new Integer(1);
	public static final Integer OPTION_GRAPHICS = new Integer(2);
	public static final Integer OPTION_SPLIT_TRIGGER = new Integer(3);
	public static final Integer OPTION_COMPRESS_TABLE = new Integer(4);
	public static final Integer OPTION_COMPRESS_INDEX = new Integer(5);
	public static final Integer OPTION_EXTRACT_PARTITIONS = new Integer(6);
	public static final Integer OPTION_EXTRACT_HASH_PARTITIONS = new Integer(7);
	public static final Integer OPTION_GENERATE_CONS_NAMES = new Integer(8);
	public static final Integer OPTION_USE_BESTPRACTICE_TSNAMES = new Integer(9);
	public static final Integer DEPLOY_TSBP = new Integer(0);
	public static final Integer DEPLOY_ROLE = new Integer(1);
	public static final Integer DEPLOY_SEQUENCE = new Integer(2);
	public static final Integer DEPLOY_TABLE = new Integer(3);
	public static final Integer DEPLOY_DEFAULT = new Integer(4);
	public static final Integer DEPLOY_CHECK_CONSTRAINTS = new Integer(5);
	public static final Integer DEPLOY_PRIMARY_KEY = new Integer(6);
	public static final Integer DEPLOY_UNIQUE_INDEX = new Integer(7);
	public static final Integer DEPLOY_INDEX = new Integer(8);
	public static final Integer DEPLOY_FOREIGN_KEYS = new Integer(9);
	public static final Integer DEPLOY_TYPE = new Integer(10);
	public static final Integer DEPLOY_FUNCTION = new Integer(11);
	public static final Integer DEPLOY_VIEW = new Integer(12);
	public static final Integer DEPLOY_MQT = new Integer(13);
	public static final Integer DEPLOY_TRIGGER = new Integer(14);
	public static final Integer DEPLOY_PROCEDURE = new Integer(15);
	public static final Integer DEPLOY_PACKAGE = new Integer(16);
	public static final Integer DEPLOY_PACKAGE_BODY = new Integer(17);
	public JCheckBoxMenuItem optionMenu[];
	public JCheckBoxMenuItem deployMenu[];
	private String deployCodes[][];
	private String optionCodes[][];
	private IBMExtractConfig cfg;

}
