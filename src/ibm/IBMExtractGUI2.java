package ibm;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.net.URL;
import java.util.Hashtable;

import javax.swing.AbstractButton;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTabbedPane;
import javax.swing.JToggleButton;
import javax.swing.JToolBar;
import javax.swing.KeyStroke;
import javax.swing.LookAndFeel;
import javax.swing.UIManager;
import javax.swing.border.EmptyBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.plaf.metal.DefaultMetalTheme;
import javax.swing.plaf.metal.MetalLookAndFeel;

import com.jgoodies.looks.LookUtils;
import com.jgoodies.looks.Options;
import com.jgoodies.looks.plastic.PlasticLookAndFeel;

public class IBMExtractGUI2 extends JFrame {
	private static final long serialVersionUID = -7554199705627733295L;
	protected static final Dimension PREFERRED_SIZE = LookUtils.IS_LOW_RESOLUTION ? new Dimension(
			880, 650)
			: new Dimension(880, 650);
	private static final String COPYRIGHT = "© 2009 vikram.khatri@us.ibm.com. IBM Corporation All Rights Reserved.";
	private final Settings settings;
	private JTabbedPane tabbedPane;
	private String[][] optionCodes = { { "trimTrailingSpaces", "false" },
			{ "dbclob", "false" }, { "graphic", "false" },
			{ "regenerateTriggers", "false" }, { "loadRemote", "false" },
			{ "compressTable", "false" }, { "compressIndex", "false" },
			{ "extractPartitions", "true" },
			{ "extractHashPartitions", "true" },
			{ "retainConstraintsName", "false" },
			{ "useBestPracticeTSNames", "true" } };

	private String[][] deploCodes = { { "BPTS", "false" }, { "ROLE", "false" },
			{ "SEQUENCE", "false" }, { "TABLE", "false" },
			{ "DEFAULT", "false" }, { "CHECK_CONSTRAINTS", "false" },
			{ "PRIMARY_KEY", "false" }, { "UNIQUE_INDEX", "false" },
			{ "INDEX", "false" }, { "FOREIGN_KEYS", "false" },
			{ "TYPE", "false" }, { "FUNCTION", "false" }, { "VIEW", "false" },
			{ "MQT", "false" }, { "TRIGGER", "false" },
			{ "PROCEDURE", "false" }, { "PACKAGE", "false" },
			{ "PACKAGE_BODY", "false" } };
	private AbstractButton btnExecuteAll;
	private AbstractButton btnRevalidateAll;
	private AbstractButton btnExecute;
	private AbstractButton btnRevalidate;
	private AbstractButton btnDiscard;
	private AbstractButton btnRefresh;
	private IBMExtractConfig cfg;
	private AbstractButton btnDB2ScriptDeploy;
	private String srcVendor = "oracle";

	public static JLabel lblSplash = new JLabel("");
	public String outputDirectory = null;
	public StringBuffer buffer = new StringBuffer();
	StateExtractTab tab1;
	SplitExtractTab tab2;
	OutputFileTab tab3;
	MenuBarView menu = null;

	DB2FileOpenActionListener db2FileOpenActionListener = new DB2FileOpenActionListener();
	RefreshActionListener refreshActionListener = new RefreshActionListener();

	ExecuteAllActionListener executeAllActionListener = new ExecuteAllActionListener();
	ExecuteActionListener executeActionListener = new ExecuteActionListener();

	RevalidateAllActionListener revalidateAllActionListener = new RevalidateAllActionListener();
	RevalidateActionListener revalidateActionListener = new RevalidateActionListener();

	DiscardActionListener discardActionListener = new DiscardActionListener();

	AboutActionListener aboutActionListener = new AboutActionListener();
	CreateHelpActionListener createHelpActionListener = new CreateHelpActionListener();

	GetNewVersionActionListener getNewVersionActionListener = new GetNewVersionActionListener();

	OutputFileActionListener outputFileActionListener = new OutputFileActionListener();

	FillTextAreaTab3ActionListener fillTextAreaTab3ActionListener = new FillTextAreaTab3ActionListener();

	FillTextAreaWithFileActionListener fillTextAreaWithFileActionListener = new FillTextAreaWithFileActionListener();

	TailOutputActionListener tailOutputActionListener = new TailOutputActionListener();

	protected IBMExtractGUI2(Settings settings) {
		this.settings = settings;

		ImageIcon icon = readImageIcon("waiting.gif");
		lblSplash.setIcon(icon);
		lblSplash.setSize(icon.getIconWidth(), icon.getIconHeight());
		lblSplash.setVisible(false);

		configureUI();
		build();
		setDefaultCloseOperation(3);
	}

	private static Settings createDefaultSettings() {
		Settings settings = Settings.createDefault();
		return settings;
	}

	private Component buildToolBar() {
		String version = IBMExtractGUI2.class.getPackage()
				.getImplementationVersion();
		JLabel lblVersion = new JLabel();
		JToolBar toolBar = new JToolBar();
		toolBar.setFloatable(true);
		toolBar.putClientProperty("JToolBar.isRollover", Boolean.TRUE);

		toolBar.putClientProperty("jgoodies.headerStyle", this.settings
				.getToolBarHeaderStyle());

		toolBar.putClientProperty("Plastic.borderStyle", this.settings
				.getToolBarPlasticBorderStyle());

		toolBar.putClientProperty("jgoodies.windows.borderStyle", this.settings
				.getToolBarWindowsBorderStyle());

		toolBar.putClientProperty("Plastic.is3D", this.settings
				.getToolBar3DHint());

		this.btnDB2ScriptDeploy = createToolBarButton("open.gif",
				"Select directory having objects to be deployed",
				this.db2FileOpenActionListener, KeyStroke.getKeyStroke(68, 128));
		this.btnDB2ScriptDeploy
				.addActionListener(this.db2FileOpenActionListener);
		toolBar.add(this.btnDB2ScriptDeploy);

		this.btnRefresh = createToolBarButton("refresh.gif",
				"Refresh objects to be deployed");
		this.btnRefresh.addActionListener(this.refreshActionListener);
		toolBar.add(this.btnRefresh);

		toolBar.addSeparator();

		this.btnExecuteAll = createToolBarButton("srcdb.png",
				"Deploy All objects");
		this.btnExecuteAll.addActionListener(this.executeAllActionListener);
		toolBar.add(this.btnExecuteAll);

		this.btnExecute = createToolBarButton("dstdb.png",
				"Deploy Selected Objects");
		this.btnExecute.addActionListener(this.executeActionListener);
		toolBar.add(this.btnExecute);

		toolBar.addSeparator();

		this.btnRevalidateAll = createToolBarButton("revalidate.png",
				"Revalidate All objects");
		this.btnRevalidateAll
				.addActionListener(this.revalidateAllActionListener);
		toolBar.add(this.btnRevalidateAll);

		this.btnRevalidate = createToolBarButton("valid.gif",
				"Revalidate selected objects");
		this.btnRevalidate.addActionListener(this.revalidateActionListener);
		toolBar.add(this.btnRevalidate);

		toolBar.addSeparator();

		this.btnDiscard = createToolBarButton("remove.gif",
				"Do not deploy selected objects");
		this.btnDiscard.addActionListener(this.discardActionListener);
		toolBar.add(this.btnDiscard);

		toolBar.addSeparator();

		toolBar.add(lblSplash);

		if (version != null) {
			lblVersion.setText("             " + version);
			toolBar.add(lblVersion);
		}

		return toolBar;
	}

	protected AbstractButton createToolBarButton(String iconName,
			String toolTipText) {
		JButton button = new JButton(readImageIcon(iconName));
		button.setToolTipText(toolTipText);
		button.setFocusable(false);
		return button;
	}

	private AbstractButton createToolBarButton(String iconName,
			String toolTipText, ActionListener action, KeyStroke keyStroke) {
		AbstractButton button = createToolBarButton(iconName, toolTipText);
		button.registerKeyboardAction(action, keyStroke, 2);
		return button;
	}

	private JComponent buildContentPane() {
		JPanel panel = new JPanel(new BorderLayout());
		panel.add(buildToolBar(), "North");
		panel.add(buildMainPanel(), "Center");
		return panel;
	}

	protected MenuBarView createMenuBuilder() {
		return this.menu = new MenuBarView();
	}

	private void build() {
		setContentPane(buildContentPane());
		setTitle(getWindowTitle());

		setJMenuBar(createMenuBuilder().buildMenuBar(this.settings, this.cfg,
				this.optionCodes, this.deploCodes,
				this.createHelpActionListener, this.aboutActionListener,
				this.getNewVersionActionListener,
				this.executeAllActionListener, this.executeActionListener,
				this.revalidateActionListener, this.db2FileOpenActionListener,
				this.revalidateAllActionListener, this.refreshActionListener,
				this.discardActionListener));

		Enable(this.srcVendor);
		setIconImage(readImageIcon("cobra.gif").getImage());
	}

	protected AbstractButton createToolBarRadioButton(String iconName,
			String toolTipText) {
		JToggleButton button = new JToggleButton(readImageIcon(iconName));
		button.setToolTipText(toolTipText);
		button.setFocusable(false);
		return button;
	}

	public void Enable(String srcVendor) {
		if (this.menu == null) {
			return;
		}
		if (srcVendor.equals("oracle")) {
			this.menu.deployMenu[MenuBarView.DEPLOY_TSBP.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_ROLE.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_SEQUENCE.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_TABLE.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_DEFAULT.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_CHECK_CONSTRAINTS
					.intValue()].setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_PRIMARY_KEY.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_UNIQUE_INDEX.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_INDEX.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_FOREIGN_KEYS.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_TYPE.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_FUNCTION.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_VIEW.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_TRIGGER.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_PROCEDURE.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_PACKAGE.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_PACKAGE_BODY.intValue()]
					.setEnabled(true);

			this.menu.deployMenu[MenuBarView.DEPLOY_TSBP.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_ROLE.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_SEQUENCE.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_TABLE.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_DEFAULT.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_CHECK_CONSTRAINTS
					.intValue()].setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_PRIMARY_KEY.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_UNIQUE_INDEX.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_INDEX.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_FOREIGN_KEYS.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_TYPE.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_FUNCTION.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_VIEW.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_MQT.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_TRIGGER.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_PROCEDURE.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_PACKAGE.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_PACKAGE_BODY.intValue()]
					.setSelected(true);

			this.deploCodes[MenuBarView.DEPLOY_TSBP.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_ROLE.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_SEQUENCE.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_TABLE.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_DEFAULT.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_CHECK_CONSTRAINTS.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_PRIMARY_KEY.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_UNIQUE_INDEX.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_INDEX.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_FOREIGN_KEYS.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_TYPE.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_FUNCTION.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_VIEW.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_MQT.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_TRIGGER.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_PROCEDURE.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_PACKAGE.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_ROLE.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_PACKAGE_BODY.intValue()][1] = "true";

			this.menu.optionMenu[MenuBarView.OPTION_SPLIT_TRIGGER.intValue()]
					.setVisible(true);
			this.menu.optionMenu[MenuBarView.OPTION_GENERATE_CONS_NAMES
					.intValue()].setVisible(true);
			this.menu.optionMenu[MenuBarView.OPTION_USE_BESTPRACTICE_TSNAMES
					.intValue()].setVisible(true);
		} else if ((srcVendor.equals("db2")) || (srcVendor.equals("zdb2"))
				|| (srcVendor.equals("mssql"))
				|| (srcVendor.equals("postgres"))
				|| (srcVendor.equals("sybase")) || (srcVendor.equals("mysql"))) {
			this.menu.deployMenu[MenuBarView.DEPLOY_TSBP.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_ROLE.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_SEQUENCE.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_TABLE.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_DEFAULT.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_CHECK_CONSTRAINTS
					.intValue()].setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_PRIMARY_KEY.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_UNIQUE_INDEX.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_INDEX.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_FOREIGN_KEYS.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_TYPE.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_FUNCTION.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_VIEW.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_MQT.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_TRIGGER.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_PROCEDURE.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_PACKAGE.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_PACKAGE_BODY.intValue()]
					.setVisible(false);

			this.menu.deployMenu[MenuBarView.DEPLOY_TSBP.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_ROLE.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_SEQUENCE.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_TABLE.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_DEFAULT.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_CHECK_CONSTRAINTS
					.intValue()].setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_PRIMARY_KEY.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_UNIQUE_INDEX.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_INDEX.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_FOREIGN_KEYS.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_TYPE.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_FUNCTION.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_VIEW.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_MQT.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_TRIGGER.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_PROCEDURE.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_PACKAGE.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_PACKAGE_BODY.intValue()]
					.setSelected(true);

			this.deploCodes[MenuBarView.DEPLOY_TSBP.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_ROLE.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_SEQUENCE.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_TABLE.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_DEFAULT.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_CHECK_CONSTRAINTS.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_PRIMARY_KEY.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_UNIQUE_INDEX.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_INDEX.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_FOREIGN_KEYS.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_TYPE.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_FUNCTION.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_VIEW.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_MQT.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_TRIGGER.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_PROCEDURE.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_PACKAGE.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_ROLE.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_PACKAGE_BODY.intValue()][1] = "false";

			this.menu.optionMenu[MenuBarView.OPTION_SPLIT_TRIGGER.intValue()]
					.setVisible(false);
			this.menu.optionMenu[MenuBarView.OPTION_GENERATE_CONS_NAMES
					.intValue()].setVisible(false);
			this.menu.optionMenu[MenuBarView.OPTION_USE_BESTPRACTICE_TSNAMES
					.intValue()].setVisible(false);

			this.optionCodes[MenuBarView.OPTION_EXTRACT_PARTITIONS.intValue()][1] = "false";
			this.optionCodes[MenuBarView.OPTION_EXTRACT_HASH_PARTITIONS
					.intValue()][1] = "false";
		} else if (srcVendor.equals("access")) {
			this.menu.deployMenu[MenuBarView.DEPLOY_TSBP.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_ROLE.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_SEQUENCE.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_TABLE.intValue()]
					.setEnabled(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_DEFAULT.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_CHECK_CONSTRAINTS
					.intValue()].setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_PRIMARY_KEY.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_UNIQUE_INDEX.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_INDEX.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_FOREIGN_KEYS.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_TYPE.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_FUNCTION.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_VIEW.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_MQT.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_TRIGGER.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_PROCEDURE.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_PACKAGE.intValue()]
					.setVisible(false);
			this.menu.deployMenu[MenuBarView.DEPLOY_PACKAGE_BODY.intValue()]
					.setVisible(false);

			this.menu.deployMenu[MenuBarView.DEPLOY_TSBP.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_ROLE.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_SEQUENCE.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_TABLE.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_DEFAULT.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_CHECK_CONSTRAINTS
					.intValue()].setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_PRIMARY_KEY.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_UNIQUE_INDEX.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_INDEX.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_FOREIGN_KEYS.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_TYPE.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_FUNCTION.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_VIEW.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_MQT.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_TRIGGER.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_PROCEDURE.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_PACKAGE.intValue()]
					.setSelected(true);
			this.menu.deployMenu[MenuBarView.DEPLOY_PACKAGE_BODY.intValue()]
					.setSelected(true);

			this.deploCodes[MenuBarView.DEPLOY_TSBP.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_ROLE.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_SEQUENCE.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_TABLE.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_DEFAULT.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_CHECK_CONSTRAINTS.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_PRIMARY_KEY.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_UNIQUE_INDEX.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_INDEX.intValue()][1] = "true";
			this.deploCodes[MenuBarView.DEPLOY_FOREIGN_KEYS.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_TYPE.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_FUNCTION.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_VIEW.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_MQT.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_TRIGGER.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_PROCEDURE.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_PACKAGE.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_ROLE.intValue()][1] = "false";
			this.deploCodes[MenuBarView.DEPLOY_PACKAGE_BODY.intValue()][1] = "false";

			this.menu.optionMenu[MenuBarView.OPTION_TRAILING_BLANKS.intValue()]
					.setSelected(true);

			this.menu.optionMenu[MenuBarView.OPTION_SPLIT_TRIGGER.intValue()]
					.setVisible(false);
			this.menu.optionMenu[MenuBarView.OPTION_SPLIT_TRIGGER.intValue()]
					.setVisible(false);

			this.optionCodes[MenuBarView.OPTION_EXTRACT_PARTITIONS.intValue()][1] = "false";
			this.optionCodes[MenuBarView.OPTION_EXTRACT_HASH_PARTITIONS
					.intValue()][1] = "false";
		}
	}

	private Component buildMainPanel() {
		this.tabbedPane = new JTabbedPane(1);
		this.tabbedPane.addChangeListener(new ChangeListener() {
			public void stateChanged(ChangeEvent e) {
				if (e.getSource().equals(IBMExtractGUI2.this.tabbedPane)) {
					if (IBMExtractGUI2.this.tabbedPane.getSelectedIndex() == 0) {
						IBMExtractGUI2.this.btnExecuteAll.setEnabled(false);
						IBMExtractGUI2.this.btnExecute.setEnabled(false);
						IBMExtractGUI2.this.btnRevalidate.setEnabled(false);
						IBMExtractGUI2.this.btnRevalidateAll.setEnabled(false);
						IBMExtractGUI2.this.btnRefresh.setEnabled(false);
						IBMExtractGUI2.this.btnDiscard.setEnabled(false);
						if (IBMExtractGUI2.this.menu != null) {
							IBMExtractGUI2.this.menu.menuExecuteAll
									.setEnabled(false);
							IBMExtractGUI2.this.menu.menuExecute
									.setEnabled(false);
							IBMExtractGUI2.this.menu.menuRevalidate
									.setEnabled(false);
							IBMExtractGUI2.this.menu.menuRevalidateAll
									.setEnabled(false);
							IBMExtractGUI2.this.menu.menuRefresh
									.setEnabled(false);
							IBMExtractGUI2.this.menu.menuDiscard
									.setEnabled(false);
						}
					} else {
						IBMExtractGUI2.this.btnExecuteAll.setEnabled(true);
						IBMExtractGUI2.this.btnExecute.setEnabled(true);
						IBMExtractGUI2.this.btnRevalidate.setEnabled(true);
						IBMExtractGUI2.this.btnRevalidateAll.setEnabled(true);
						IBMExtractGUI2.this.btnRefresh.setEnabled(true);
						IBMExtractGUI2.this.btnDiscard.setEnabled(true);
						if (IBMExtractGUI2.this.menu != null) {
							IBMExtractGUI2.this.menu.menuExecuteAll
									.setEnabled(true);
							IBMExtractGUI2.this.menu.menuExecute
									.setEnabled(true);
							IBMExtractGUI2.this.menu.menuRevalidate
									.setEnabled(true);
							IBMExtractGUI2.this.menu.menuRevalidateAll
									.setEnabled(true);
							IBMExtractGUI2.this.menu.menuRefresh
									.setEnabled(true);
							IBMExtractGUI2.this.menu.menuDiscard
									.setEnabled(true);
						}
					}
				}
			}
		});
		addTabs(this.tabbedPane);

		this.tabbedPane.setBorder(new EmptyBorder(10, 10, 10, 10));
		return this.tabbedPane;
	}

	protected String getWindowTitle() {
		return "IBM Data Movement Tool";
	}

	protected static ImageIcon readImageIcon(String filename) {
		URL url = IBMExtractGUI2.class.getResource("resources/images/"
				+ filename);
		return new ImageIcon(url);
	}

	private void addTabs(JTabbedPane tabbedPane) {
		tabbedPane.addTab(" Extract / Deploy ",
				(this.tab1 = new StateExtractTab(this,
						this.fillTextAreaTab3ActionListener,
						this.fillTextAreaWithFileActionListener,
						this.tailOutputActionListener, this.buffer,
						this.optionCodes)).build());

		this.cfg = this.tab1.getCfg();
		this.srcVendor = this.cfg.getSrcVendor();
		tabbedPane.addTab(" Interactive Deploy ",
				(this.tab2 = new SplitExtractTab(this.tab1.getCfg(),
						this.executeAllActionListener,
						this.revalidateActionListener,
						this.executeActionListener,
						this.revalidateAllActionListener,
						this.discardActionListener)).build());

		tabbedPane.addTab(" View File ", (this.tab3 = new OutputFileTab(
				this.buffer)).build());
	}

	protected void locateOnScreen(Component component) {
		Dimension paneSize = component.getSize();
		Dimension screenSize = component.getToolkit().getScreenSize();
		component.setLocation((screenSize.width - paneSize.width) / 2,
				(screenSize.height - paneSize.height) / 2);
	}

	private void configureUI() {
		Options.setDefaultIconSize(new Dimension(18, 18));

		Options.setUseNarrowButtons(this.settings.isUseNarrowButtons());

		Options.setTabIconsEnabled(this.settings.isTabIconsEnabled());
		UIManager.put("jgoodies.popupDropShadowEnabled", this.settings
				.isPopupDropShadowEnabled());

		LookAndFeel selectedLaf = this.settings.getSelectedLookAndFeel();
		if ((selectedLaf instanceof PlasticLookAndFeel)) {
			PlasticLookAndFeel
					.setPlasticTheme(this.settings.getSelectedTheme());
			PlasticLookAndFeel.setTabStyle(this.settings.getPlasticTabStyle());
			PlasticLookAndFeel.setHighContrastFocusColorsEnabled(this.settings
					.isPlasticHighContrastFocusEnabled());
		} else if (selectedLaf.getClass() == MetalLookAndFeel.class) {
			MetalLookAndFeel.setCurrentTheme(new DefaultMetalTheme());
		}

		JRadioButton radio = new JRadioButton();
		radio.getUI().uninstallUI(radio);
		JCheckBox checkBox = new JCheckBox();
		checkBox.getUI().uninstallUI(checkBox);
		try {
			UIManager.setLookAndFeel(selectedLaf);
		} catch (Exception e) {
			System.out.println("Can't change L&F: " + e);
		}
	}

	public static void main(String[] args) {
		Settings settings = createDefaultSettings();
		if (args.length > 0) {
			String laf = args[0];
			if (laf != null) {
				String lafClassName;
				if ("Windows".equalsIgnoreCase(laf)) {
					lafClassName = "com.jgoodies.looks.windows.WindowsLookAndFeel";
				} else {
					if ("Plastic".equalsIgnoreCase(laf)) {
						lafClassName = "com.jgoodies.looks.plastic.PlasticLookAndFeel";
					} else {
						if ("Plastic3D".equalsIgnoreCase(laf)) {
							lafClassName = "com.jgoodies.looks.plastic.Plastic3DLookAndFeel";
						} else {
							if ("PlasticXP".equalsIgnoreCase(laf))
								lafClassName = "com.jgoodies.looks.plastic.PlasticXPLookAndFeel";
							else
								lafClassName = laf;
						}
					}
				}
				IBMExtractUtilities.log("L&f chosen: " + lafClassName);
				settings.setSelectedLookAndFeel(lafClassName);
			}
		}
		IBMExtractGUI2 instance = new IBMExtractGUI2(settings);
		instance.setSize(PREFERRED_SIZE);
		instance.locateOnScreen(instance);
		instance.setVisible(true);
	}

	final class AboutActionListener implements ActionListener {
		String currentVersion = IBMExtractGUI2.class.getPackage()
				.getImplementationVersion();

		AboutActionListener() {
		}

		public void actionPerformed(ActionEvent e) {
			JOptionPane
					.showMessageDialog(
							IBMExtractGUI2.this,
							"IBM Data Movement Tool "
									+ (this.currentVersion != null ? this.currentVersion
											: "")
									+ "\n\n"
									+ "© 2009 vikram.khatri@us.ibm.com. IBM Corporation All Rights Reserved.");
		}
	}

	final class GetNewVersionActionListener extends JDialog implements
			ActionListener {
		GetNewVersionActionListener() {
		}

		public void actionPerformed(ActionEvent e) {
			String message = IBMExtractUtilities.GetIDMTVersion();
			GetIDMTUpdate upd = new GetIDMTUpdate(IBMExtractGUI2.this,
					"IBM Data Movement Tool", message);
		}
	}

	final class CreateHelpActionListener implements ActionListener {
		CreateHelpActionListener() {
		}

		public void actionPerformed(ActionEvent e) {
		}
	}

	final class DiscardActionListener implements ActionListener {
		DiscardActionListener() {
		}

		public void actionPerformed(ActionEvent e) {
			IBMExtractGUI2.this.tab2.discardSelectedObjects();
		}
	}

	final class RevalidateActionListener implements ActionListener {
		RevalidateActionListener() {
		}

		public void actionPerformed(ActionEvent e) {
			JOptionPane
					.showMessageDialog(
							IBMExtractGUI2.this,
							"Revalidate statement\n\n© 2009 vikram.khatri@us.ibm.com. IBM Corporation All Rights Reserved.\n\n");
		}
	}

	final class RevalidateAllActionListener implements ActionListener {
		RevalidateAllActionListener() {
		}

		public void actionPerformed(ActionEvent e) {
			JOptionPane
					.showMessageDialog(
							IBMExtractGUI2.this,
							"Revalidate All statement\n\n© 2009 vikram.khatri@us.ibm.com. IBM Corporation All Rights Reserved.\n\n");
		}
	}

	final class ExecuteActionListener implements ActionListener {
		ExecuteActionListener() {
		}

		public void actionPerformed(ActionEvent e) {
			IBMExtractGUI2.this.tab2.executeSelectedObjects();
		}
	}

	final class FillTextAreaWithFileActionListener implements ActionListener {
		FillTextAreaWithFileActionListener() {
		}

		public void actionPerformed(ActionEvent e) {
			IBMExtractGUI2.this.tab3
					.FillTextAreaFromFile(IBMExtractGUI2.this.tab1
							.getTextfieldOutputDir());
			IBMExtractGUI2.this.outputFileActionListener.actionPerformed(e);
		}
	}

	final class TailOutputActionListener implements ActionListener {
		TailOutputActionListener() {
		}

		public void actionPerformed(ActionEvent e) {
			IBMExtractGUI2.this.tabbedPane.setSelectedIndex(2);
			int len = IBMExtractGUI2.this.buffer.toString().length();
			IBMExtractGUI2.this.tab3.getTextArea().setText(
					IBMExtractGUI2.this.buffer.toString());
			try {
				IBMExtractGUI2.this.tab3.getTextArea().setCaretPosition(len);
			} catch (Exception ex) {
			}
		}
	}

	final class FillTextAreaTab3ActionListener implements ActionListener {
		FillTextAreaTab3ActionListener() {
		}

		public void actionPerformed(ActionEvent e) {
			String scriptName = e.getActionCommand();
			IBMExtractGUI2.this.tab3.FillTextAreaFromOutput(scriptName);
			IBMExtractGUI2.this.tabbedPane.setSelectedIndex(2);
		}
	}

	final class ExecuteAllActionListener implements ActionListener {
		ExecuteAllActionListener() {
		}

		public void actionPerformed(ActionEvent e) {
			IBMExtractGUI2.this.tab2.executeAllObjects();
		}
	}

	final class RefreshActionListener implements ActionListener {
		RefreshActionListener() {
		}

		public void actionPerformed(ActionEvent e) {
			Hashtable hashTree = new Hashtable();
			Hashtable hashPLSQLSource = new Hashtable();

			if (IBMExtractGUI2.this.outputDirectory == null)
				IBMExtractGUI2.this.outputDirectory = IBMExtractGUI2.this.tab1
						.getTextfieldOutputDir();
			BuildPLSQLObjects pl = new BuildPLSQLObjects(
					IBMExtractGUI2.this.cfg.getDB2Compatibility(),
					IBMExtractGUI2.this.srcVendor,
					IBMExtractGUI2.this.outputDirectory);
			hashTree = pl.getTreeHash();
			hashPLSQLSource = pl.getPLSQLHash();
			IBMExtractGUI2.this.tab2.refreshStatementsTree(hashTree,
					hashPLSQLSource, IBMExtractGUI2.this.outputDirectory,
					IBMExtractGUI2.this.deploCodes);
		}
	}

	final class DB2FileOpenActionListener implements ActionListener {
		DB2FileOpenActionListener() {
		}

		public void actionPerformed(ActionEvent e) {
			try {
				if (IBMExtractGUI2.this.tab1.getDB2InstanceName().equals("")) {
					JOptionPane.showConfirmDialog(IBMExtractGUI2.this,
							"Connect to DB2 first", "Connect to DB2 first", -1);

					return;
				}
				IBMExtractGUI2.this.outputDirectory = IBMExtractGUI2.this.tab1
						.getTextfieldOutputDir();
				File f = new File(new File(IBMExtractGUI2.this.outputDirectory)
						.getCanonicalPath());
				JFileChooser fc = new JFileChooser();
				fc.setCurrentDirectory(f);
				fc.setDialogTitle("Select output directory");
				fc.setMultiSelectionEnabled(false);
				fc.setFileSelectionMode(1);

				int result = fc.showOpenDialog(null);
				if (result == 1) {
					return;
				}
				File fileSelected = null;
				fileSelected = fc.getSelectedFile();
				if (fileSelected != null) {
					if (!fileSelected.getName().equals("")) {
						IBMExtractGUI2.this.outputDirectory = fileSelected
								.getAbsolutePath();
					}
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			IBMExtractGUI2.this.tabbedPane.setSelectedIndex(1);
			new IBMExtractGUI2.RefreshActionListener().actionPerformed(e);
		}
	}

	final class OutputFileActionListener implements ActionListener {
		OutputFileActionListener() {
		}

		public void actionPerformed(ActionEvent e) {
			IBMExtractGUI2.this.tabbedPane.setSelectedIndex(2);
		}
	}
}