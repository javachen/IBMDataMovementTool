package ibm;

import com.jgoodies.forms.builder.DefaultFormBuilder;
import com.jgoodies.forms.layout.CellConstraints;
import com.jgoodies.forms.layout.FormLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.swing.Box;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.Timer;
import javax.swing.border.LineBorder;
import javax.swing.filechooser.FileFilter;

public class StateExtractTab extends JFrame implements ActionListener {
	private static final long serialVersionUID = -6130743742136890628L;
	private JLabel lblMessage = new JLabel("Ready");
	private JLabel lblSplash = IBMExtractGUI2.lblSplash;
	private JLabel lblDB2Instance = new JLabel();
	private JLabel lblDB2VarcharCompat = new JLabel();
	private JLabel lblDB2DateCompat = new JLabel();
	private JLabel lblDB2NumberCompat = new JLabel();
	private JLabel lblDB2Decflt_rounding = new JLabel();
	private JLabel lblDatabaseName = new JLabel("Database Name:");
	private JButton btnSrcJDBC = new JButton("...");
	private JButton btnDstJDBC = new JButton("...");
	private JButton btnSrcTestConn = new JButton("Connect to Source");
	private JButton btnExtract = new JButton("Extract DDL/Data");
	private JButton btnDstTestConn = new JButton("Connect to DB2");
	private JButton btnDeploy = new JButton(
			"            Deploy DDL/Data           ");
	private JButton btnDropObjs = new JButton(
			"                Drop Objects            ");
	private JButton btnView = new JButton(
			"          View Script/Output         ");
	private JButton btnDB2Script = new JButton(
			"         Execute DB2 Script         ");
	private JButton btnOutputDir = new JButton("...");
	private JButton btnCreateScript = new JButton(
			"Generate Data Movement Scripts");
	private JButton btnMeetScript = new JButton("Generate Input file for MEET");
	private JTextField textfieldOutputDir = new JTextField(40);
	private JTextField textfieldSrcServer = new JTextField(40);
	private JTextField textfieldDstServer = new JTextField(40);
	private JTextField textfieldSrcPortNum = new JTextField(40);
	private JTextField textfieldDstPortNum = new JTextField(40);
	private JTextField textfieldSrcDatabase = new JTextField(40);
	private JTextField textfieldDstDatabase = new JTextField(40);
	private JTextField textfieldSrcUserID = new JTextField(40);
	private JTextField textfieldDstUserID = new JTextField(40);
	private JPasswordField textfieldSrcPassword = new JPasswordField(40);
	private JPasswordField textfieldDstPassword = new JPasswordField(40);
	private JTextField textfieldSrcJDBC = new JTextField(35);
	private JTextField textfieldDstJDBC = new JTextField(35);
	private JTextField textLimitExtractRows = new JTextField(10);
	private JTextField textfieldNumTreads = new JTextField(10);
	private JTextField textLimitLoadRows = new JTextField(10);
	private JComboBox comboSrcVendor;
	private JComboBox comboDstVendor;
	private JCheckBox checkboxDDL = new JCheckBox("DDL", true);
	private JCheckBox checkboxData = new JCheckBox("Data Movement", true);

	private Timer busy = null;
	private IBMExtractConfig cfg;
	private String txtMessage;
	private String dstVendor = IBMExtractUtilities.osType
			.equalsIgnoreCase("z/OS") ? "zdb2" : "db2";
	private String sep = IBMExtractUtilities.osType.equalsIgnoreCase("Win") ? ";"
			: ":";
	private String executingScriptName = "";
	private boolean resetDstFields = true;

	private JPanel panelCheckBox = new JPanel();
	private JScrollPane scrollpane = new JScrollPane(this.panelCheckBox);
	private ActionListener fillTextAreaTab3ActionListener;
	private ActionListener fillTextAreaWithFileActionListener;
	private ActionListener tailOutputActionListener;
	private IBMExtractGUI2 maingui;
	private StringBuffer buffer;
	private String[][] optionCodes;

	public StateExtractTab(IBMExtractGUI2 maingui,
			ActionListener fillTextAreaTab3ActionListener,
			ActionListener fillTextAreaWithFileActionListener,
			ActionListener tailOutputActionListener, StringBuffer buffer,
			String[][] optionCodes) {
		this.maingui = maingui;
		this.buffer = buffer;
		this.optionCodes = optionCodes;
		this.fillTextAreaTab3ActionListener = fillTextAreaTab3ActionListener;
		this.fillTextAreaWithFileActionListener = fillTextAreaWithFileActionListener;
		this.tailOutputActionListener = tailOutputActionListener;
	}

	JComponent build() {
		FormLayout layout = new FormLayout(
				"right:max(50dlu;pref), 3dlu, pref, 7dlu, pref",
				"p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu,p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 5dlu, p, 2dlu, p");

		DefaultFormBuilder builder = new DefaultFormBuilder(layout);
		builder.setDefaultDialogBorder();
		builder.setOpaque(false);

		String[] srcChoices = { "oracle", "mssql", "sybase", "access", "mysql",
				"postgres", "zdb2", "idb2", "db2" };

		String[] dstChoices = { "DB2 With Compatibility Mode", "DB2" };

		this.scrollpane.setVerticalScrollBarPolicy(22);
		this.scrollpane.setPreferredSize(new Dimension(300, 100));

		this.comboSrcVendor = new JComboBox(srcChoices);
		this.comboDstVendor = new JComboBox(dstChoices);

		Box jdbcSrcBox = Box.createHorizontalBox();
		jdbcSrcBox.add(this.textfieldSrcJDBC);
		jdbcSrcBox.add(this.btnSrcJDBC);

		Box jdbcDstBox = Box.createHorizontalBox();
		jdbcDstBox.add(this.textfieldDstJDBC);
		jdbcDstBox.add(this.btnDstJDBC);

		Box dstBox = Box.createVerticalBox();
		dstBox.setPreferredSize(new Dimension(300, 100));
		dstBox.add(Box.createVerticalGlue());
		dstBox.add(this.lblDB2Instance);
		dstBox.add(Box.createVerticalGlue());
		dstBox.add(this.lblDB2DateCompat);
		dstBox.add(Box.createVerticalGlue());
		dstBox.add(this.lblDB2NumberCompat);
		dstBox.add(Box.createVerticalGlue());
		dstBox.add(this.lblDB2VarcharCompat);
		dstBox.add(Box.createVerticalGlue());
		dstBox.add(this.lblDB2Decflt_rounding);
		dstBox.add(Box.createVerticalGlue());
		dstBox.setBorder(new LineBorder(Color.BLUE));

		Box browseBox = Box.createHorizontalBox();
		browseBox.add(this.textfieldOutputDir);
		browseBox.add(this.btnOutputDir);

		Box migrationBox = Box.createHorizontalBox();
		migrationBox.add(this.checkboxDDL);
		migrationBox.add(this.checkboxData);
		migrationBox.add(new JLabel("| Num Threads: "));
		migrationBox.add(this.textfieldNumTreads);
		migrationBox.add(new JLabel("# Extract Rows: "));
		migrationBox.add(this.textLimitExtractRows);
		migrationBox.add(new JLabel("# Load Rows: "));
		migrationBox.add(this.textLimitLoadRows);

		Box meetBox = Box.createHorizontalBox();
		meetBox.add(this.btnCreateScript);
		meetBox.add(Box.createHorizontalGlue());
		meetBox.add(this.btnMeetScript);

		Box scriptBox = Box.createHorizontalBox();
		scriptBox.add(this.btnView);
		scriptBox.add(Box.createHorizontalGlue());
		scriptBox.add(this.btnDB2Script);

		Box deployBox = Box.createHorizontalBox();
		deployBox.add(this.btnDeploy);
		deployBox.add(Box.createHorizontalGlue());
		deployBox.add(this.btnDropObjs);

		CellConstraints cc = new CellConstraints();

		builder.addLabel("Source Database", cc.xy(3, 1));
		builder.addLabel("DB2 Database", cc.xy(5, 1));
		builder.addSeparator("", cc.xyw(1, 3, 5));
		builder.addLabel("Vendor", cc.xy(1, 5));
		builder.add(this.comboSrcVendor, cc.xy(3, 5));
		builder.add(this.comboDstVendor, cc.xy(5, 5));
		builder.addLabel("Server Name:", cc.xy(1, 7));
		builder.add(this.textfieldSrcServer, cc.xy(3, 7));
		builder.add(this.textfieldDstServer, cc.xy(5, 7));
		builder.addLabel("Port Number:", cc.xy(1, 9));
		builder.add(this.textfieldSrcPortNum, cc.xy(3, 9));
		builder.add(this.textfieldDstPortNum, cc.xy(5, 9));

		builder.add(this.lblDatabaseName, cc.xy(1, 11));
		builder.add(this.textfieldSrcDatabase, cc.xy(3, 11));
		builder.add(this.textfieldDstDatabase, cc.xy(5, 11));
		builder.addLabel("User ID:", cc.xy(1, 13));
		builder.add(this.textfieldSrcUserID, cc.xy(3, 13));
		builder.add(this.textfieldDstUserID, cc.xy(5, 13));
		builder.addLabel("Password:", cc.xy(1, 15));
		builder.add(this.textfieldSrcPassword, cc.xy(3, 15));
		builder.add(this.textfieldDstPassword, cc.xy(5, 15));
		builder.addLabel("JDBC Drivers:", cc.xy(1, 17));
		builder.add(jdbcSrcBox, cc.xy(3, 17));
		builder.add(jdbcDstBox, cc.xy(5, 17));
		builder.addLabel("Test Connections:", cc.xy(1, 19));
		builder.add(this.btnSrcTestConn, cc.xy(3, 19));
		builder.add(this.btnDstTestConn, cc.xy(5, 19));
		builder.addSeparator("", cc.xyw(1, 21, 5));
		builder.addLabel("Source Schema:", cc.xy(1, 23));
		builder.add(this.scrollpane, cc.xy(3, 23));
		builder.add(dstBox, cc.xy(5, 23));
		builder.addLabel("Output Directory:", cc.xy(1, 25));
		builder.add(browseBox, cc.xyw(3, 25, 3));
		builder.addLabel("Migration:", cc.xy(1, 27));
		builder.add(migrationBox, cc.xyw(3, 27, 3));
		builder.addSeparator("", cc.xyw(1, 29, 5));
		builder.addLabel("Extract/Deploy:", cc.xy(1, 31));
		builder.add(this.btnExtract, cc.xy(3, 31));
		builder.add(deployBox, cc.xy(5, 31));
		builder.addLabel("Create/Execute Scripts:", cc.xy(1, 33));
		builder.add(meetBox, cc.xy(3, 33));
		builder.add(scriptBox, cc.xy(5, 33));
		builder.addSeparator("", cc.xyw(1, 35, 5));
		builder.add(this.lblMessage, cc.xyw(1, 37, 5));

		addActionListeners();

		this.cfg = new IBMExtractConfig();
		this.cfg.loadConfigFile();
		this.cfg.getParamValues();

		boolean isRemote = Boolean.valueOf(this.cfg.getRemoteLoad())
				.booleanValue();
		if (IBMExtractUtilities.isDB2Installed(isRemote)) {
			if (!isRemote)
				SetLabelMessage(this.lblMessage, "DB2 was detected.", false);
			this.cfg.setJavaHome(IBMExtractUtilities.db2JavaPath());
			this.cfg.setDstJDBCHome(IBMExtractUtilities.db2JDBCHome());
		} else {
			SetLabelMessage(this.lblMessage, IBMExtractUtilities.Message, true);
		}
		SetTimer();
		getValues();
		return builder.getPanel();
	}

	public IBMExtractConfig getCfg() {
		return this.cfg;
	}

	public void setCfg(IBMExtractConfig cfg) {
		this.cfg = cfg;
	}

	private void addActionListeners() {
		this.btnSrcJDBC.addActionListener(this);
		this.btnDstJDBC.addActionListener(this);
		this.btnSrcTestConn.addActionListener(this);
		this.btnDstTestConn.addActionListener(this);
		this.btnExtract.addActionListener(this);
		this.btnDeploy.addActionListener(this);
		this.btnDropObjs.addActionListener(this);
		this.comboSrcVendor.addActionListener(this);
		this.comboDstVendor.addActionListener(this);

		this.btnView.addActionListener(this.fillTextAreaWithFileActionListener);

		this.btnDB2Script.addActionListener(this);
		this.btnOutputDir.addActionListener(this);
		this.btnCreateScript.addActionListener(this);
		this.btnMeetScript.addActionListener(this);
		this.checkboxData.addActionListener(this);
	}

	private void SetTimer() {
		if (this.busy == null) {
			this.busy = new Timer(500, new ActionListener() {
				public void actionPerformed(ActionEvent a) {
					if (IBMExtractUtilities.DataExtracted) {
						IBMExtractUtilities.DataExtracted = false;
						StateExtractTab.this.SetLabelMessage(
								StateExtractTab.this.lblMessage,
								"Extract completeted ...", false);
						StateExtractTab.this.lblSplash.setVisible(false);
						StateExtractTab.this.btnDeploy.setEnabled(true);
						StateExtractTab.this.btnDropObjs.setEnabled(true);
						StateExtractTab.this.btnDB2Script.setEnabled(true);
					} else if (IBMExtractUtilities.ScriptExecutionCompleted) {
						IBMExtractUtilities.ScriptExecutionCompleted = false;
						StateExtractTab.this.SetLabelMessage(
								StateExtractTab.this.lblMessage,
								"Script Execution completeted ...", false);
						StateExtractTab.this.lblSplash.setVisible(false);
						ActionEvent e = new ActionEvent(this, 0,
								StateExtractTab.this.executingScriptName);
						StateExtractTab.this.fillTextAreaTab3ActionListener
								.actionPerformed(e);
					} else if (IBMExtractUtilities.db2ScriptCompleted) {
						IBMExtractUtilities.db2ScriptCompleted = false;
						StateExtractTab.this.SetLabelMessage(
								StateExtractTab.this.lblMessage,
								"db2 script Execution completeted ...", false);
						StateExtractTab.this.lblSplash.setVisible(false);
					}
				}
			});
			this.busy.start();
		}
	}

	private void AddJarsToClasspath(String jarList) {
		String[] tmp = jarList.split(this.sep);
		for (int i = 0; i < tmp.length; i++) {
			File f = new File(tmp[i]);
			try {
				IBMExtractUtilities.AddFile(f);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}

	private void SetLabelMessage(JLabel label, String message, boolean warning) {
		if (warning) {
			label.setForeground(Color.RED);
		} else {
			label.setForeground(Color.BLUE);
		}
		label.setText(message);
	}

	public void actionPerformed(ActionEvent e) {
		String srcVendor = (String) this.comboSrcVendor.getSelectedItem();
		SetLabelMessage(this.lblMessage, "", false);
		if (e.getSource().equals(this.comboSrcVendor)) {
			this.lblDatabaseName.setText("Database Name:");
			this.resetDstFields = true;
			if (srcVendor.equals("oracle")) {
				this.comboDstVendor.setSelectedIndex(0);
				this.comboDstVendor.setEnabled(true);
			} else {
				this.comboDstVendor.setSelectedIndex(1);
				this.comboDstVendor.setEnabled(false);
			}
			if (srcVendor.equalsIgnoreCase("oracle"))
				this.textfieldSrcPortNum.setText("1521");
			else if (srcVendor.equalsIgnoreCase("mssql"))
				this.textfieldSrcPortNum.setText("1433");
			else if (srcVendor.equalsIgnoreCase("sybase"))
				this.textfieldSrcPortNum.setText("4100");
			else if (srcVendor.equalsIgnoreCase("mysql"))
				this.textfieldSrcPortNum.setText("3306");
			else if (srcVendor.equalsIgnoreCase("postgres"))
				this.textfieldSrcPortNum.setText("5432");
			else if (srcVendor.equalsIgnoreCase("db2"))
				this.textfieldSrcPortNum.setText("50000");
			else if (srcVendor.equalsIgnoreCase("zdb2"))
				this.textfieldSrcPortNum.setText("0");
			else if (!srcVendor.equalsIgnoreCase("access")) {
				if (srcVendor.equalsIgnoreCase("idb2")) {
					this.textfieldSrcPortNum.setText("0");
				}
			}
			this.btnSrcTestConn
					.setText("Connect to " + srcVendor.toUpperCase());
			if (srcVendor.equals("access")) {
				SetLabelMessage(this.lblMessage,
						"Type Access file name in Server name field", false);
				this.textfieldSrcPortNum.setText("0");
				this.textfieldSrcJDBC.setText("");
				this.textfieldSrcDatabase.setText("access");
				this.textfieldSrcUserID.setText("null");
				this.textfieldSrcPassword.setText("null");
				this.cfg.setSrcSchName("ADMIN");
			} else if (srcVendor.equals("zdb2")) {
				this.lblDatabaseName.setText("Location Name:");
				this.textfieldSrcJDBC.setText("");
				this.textfieldSrcDatabase.setText("");
				this.textfieldSrcUserID.setText("");
				this.textfieldSrcPassword.setText("");
			} else if (srcVendor.equals("idb2")) {
				this.textfieldSrcDatabase.setText("SYSBAS");
				this.textfieldSrcJDBC.setText("");
				this.textfieldSrcUserID.setText("");
				this.textfieldSrcPassword.setText("");
			} else {
				this.textfieldSrcJDBC.setText("");
				this.textfieldSrcDatabase.setText("");
				this.textfieldSrcUserID.setText("");
				this.textfieldSrcPassword.setText("");
			}
			this.maingui.Enable(srcVendor);
			setAccessFields(srcVendor);
		} else if (e.getSource().equals(this.comboDstVendor)) {
			if (this.resetDstFields) {
				this.textfieldDstJDBC.setText("");
				this.textfieldDstDatabase.setText("");
				this.textfieldDstUserID.setText("");
				this.textfieldDstPassword.setText("");
				this.lblDB2Instance.setText("");
				this.resetDstFields = true;
			}
			this.lblDB2VarcharCompat.setText("");
			this.lblDB2DateCompat.setText("");
			this.lblDB2NumberCompat.setText("");
			this.lblDB2Decflt_rounding.setText("");
		} else if (e.getSource().equals(this.btnOutputDir)) {
			try {
				File f = new File(new File(".").getCanonicalPath());
				JFileChooser fc = new JFileChooser();
				fc.setCurrentDirectory(f);
				fc.setDialogTitle("Select " + srcVendor + " output directory");
				fc.setFileSelectionMode(1);

				int result = fc.showOpenDialog(null);

				if (result == 1) {
					return;
				}

				File fileSelected = null;
				fileSelected = fc.getSelectedFile();
				if ((fileSelected != null)
						&& (!fileSelected.getName().equals(""))) {
					this.cfg.setOutputDirectory(fileSelected.getAbsolutePath());
					this.textfieldOutputDir.setText(this.cfg
							.getOutputDirectory());
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		} else if (e.getSource().equals(this.btnSrcJDBC)) {
			if (srcVendor.equals("oracle")) {
				JOptionPane
						.showMessageDialog(
								this,
								"For Oracle 9i and up, you need\n"
										+ this.cfg.getJDBCList(srcVendor)
										+ "\nin order to connect to "
										+ srcVendor
										+ ".\nThe first file is mandatory and others are optional."
										+ "\nYou will need all of the above if you have XML data type."
										+ "\n"
										+ "\nFor Oracle 8i or lower, you can still use above mentioned driver but if you get "
										+ "\nerror, you should consider using classes12.jar or classes111.jar as the case may be."
										+ "");
			} else {
				JOptionPane.showMessageDialog(this, "You need\n"
						+ this.cfg.getJDBCList(srcVendor)
						+ "\nin order to connect to " + srcVendor
						+ "\nPlease locate these files and include them.");
			}

			try {
				String tmpDir = ".";
				if (srcVendor.equals("db2")) {
					tmpDir = IBMExtractUtilities.db2JDBCHome();
				}
				File f = new File(new File(tmpDir).getCanonicalPath());
				JFileChooser fc = new JFileChooser();
				fc.setCurrentDirectory(f);
				fc.setDialogTitle("Select " + srcVendor + "'s JDBC Driver(s)");
				fc.setMultiSelectionEnabled(true);
				fc.setFileSelectionMode(2);

				int result = fc.showOpenDialog(null);

				if (result == 1) {
					return;
				}

				File[] fileSelected = null;
				fileSelected = fc.getSelectedFiles();
				if (fileSelected != null) {
					for (int i = 0; i < fileSelected.length; i++) {
						if (fileSelected[i].getName().equals(""))
							continue;
						String tmp = fileSelected[i].getAbsolutePath();
						String tmp2 = this.textfieldSrcJDBC.getText();
						if ((tmp2 == null) || (tmp2.equals(""))) {
							this.textfieldSrcJDBC.setText(tmp);
						} else if (!tmp2.contains(tmp)) {
							this.textfieldSrcJDBC
									.setText(tmp2 + this.sep + tmp);
						}
					}
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			if (srcVendor.equals("db2"))
				this.textfieldDstJDBC.setText(this.textfieldSrcJDBC.getText());
		} else if (e.getSource().equals(this.btnDstJDBC)) {
			JOptionPane.showMessageDialog(this, "You need\n"
					+ this.cfg.getJDBCList("db2")
					+ "\nin order to connect to DB2"
					+ "\nPlease locate these files and include them.");
			try {
				String tmpDir = ".";
				tmpDir = IBMExtractUtilities.db2JDBCHome();
				File f = new File(new File(tmpDir).getCanonicalPath());
				JFileChooser fc = new JFileChooser();
				fc.setCurrentDirectory(f);
				fc.setDialogTitle("Select DB2 JDBC Driver(s)");
				fc.setMultiSelectionEnabled(true);

				fc.setFileSelectionMode(2);

				int result = fc.showOpenDialog(null);

				if (result == 1) {
					return;
				}

				File[] fileSelected = null;
				fileSelected = fc.getSelectedFiles();
				if (fileSelected != null) {
					for (int i = 0; i < fileSelected.length; i++) {
						if (fileSelected[i].getName().equals(""))
							continue;
						String tmp = fileSelected[i].getAbsolutePath();
						String tmp2 = this.textfieldDstJDBC.getText();
						if ((tmp2 == null) || (tmp2.equals(""))) {
							this.textfieldDstJDBC.setText(tmp);
						} else if (!tmp2.contains(tmp)) {
							this.textfieldDstJDBC
									.setText(tmp2 + this.sep + tmp);
						}
					}
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		} else if (e.getSource().equals(this.btnSrcTestConn)) {
			String schemaList = "";
			validateSrcFields();
			if (!this.txtMessage.equals("")) {
				SetLabelMessage(this.lblMessage, this.txtMessage, true);
				return;
			}
			if (!IBMExtractUtilities.isJDBCLicenseAdded(srcVendor,
					this.textfieldSrcJDBC.getText())) {
				SetLabelMessage(this.lblMessage, IBMExtractUtilities.Message,
						true);
				return;
			}
			if (this.textfieldSrcJDBC.getText().contains("db2java.zip")) {
				SetLabelMessage(this.lblMessage,
						"You selected db2java.zip. This is not the right JAR",
						true);
				return;
			}
			setValues();
			if (this.cfg.pingJDBCDrivers(this.cfg.getSrcJDBC())) {
				AddJarsToClasspath(this.cfg.getSrcJDBC());
				if (IBMExtractUtilities.TestConnection(false, false, this.cfg
						.getSrcVendor(), this.cfg.getSrcServer(), this.cfg
						.getSrcPort(), this.cfg.getSrcDBName(), this.cfg
						.getSrcUid(), this.cfg.getSrcPwd())) {
					this.cfg
							.setSrcDB2Instance(IBMExtractUtilities.InstanceName);
					this.cfg.setSrcDB2Home(IBMExtractUtilities.DB2Path);
					schemaList = IBMExtractUtilities.GetSchemaList(this.cfg
							.getSrcVendor(), this.cfg.getSrcServer(), this.cfg
							.getSrcPort(), this.cfg.getSrcDBName(), this.cfg
							.getSrcUid(), this.cfg.getSrcPwd());

					if (!schemaList.equals("")) {
						if (IBMExtractUtilities.Message.equals("")) {
							setSchemaCheckBoxes(schemaList);
							this.cfg.setSrcSchName(schemaList);
							this.btnExtract.setEnabled(true);
							this.btnCreateScript.setEnabled(true);
							if (srcVendor.equalsIgnoreCase("oracle"))
								this.btnMeetScript.setEnabled(true);
							else
								this.btnMeetScript.setEnabled(false);
							SetLabelMessage(
									this.lblMessage,
									"Connect to "
											+ srcVendor
											+ " succeeded and schema information obtained.",
									false);
						} else {
							SetLabelMessage(this.lblMessage,
									IBMExtractUtilities.Message, true);
						}
					} else
						SetLabelMessage(this.lblMessage,
								"No user schema found in your database", true);
				} else {
					SetLabelMessage(this.lblMessage,
							IBMExtractUtilities.Message, true);
				}
			} else {
				SetLabelMessage(this.lblMessage, this.cfg.Message, true);
			}
		} else if (e.getSource().equals(this.btnDstTestConn)) {
			validateDstFields();
			if (!this.txtMessage.equals("")) {
				SetLabelMessage(this.lblMessage, this.txtMessage, true);
				return;
			}
			if (!IBMExtractUtilities.isJDBCLicenseAdded(this.dstVendor,
					this.textfieldDstJDBC.getText())) {
				SetLabelMessage(this.lblMessage, IBMExtractUtilities.Message,
						true);
				return;
			}
			if (this.textfieldSrcJDBC.getText().contains("db2java.zip")) {
				SetLabelMessage(this.lblMessage,
						"You selected db2java.zip. This is not the right JAR",
						true);
				return;
			}
			setValues();
			this.cfg.writeConfigFile();
			this.cfg.getParamValues();
			boolean remote = Boolean.valueOf(this.cfg.getRemoteLoad())
					.booleanValue();

			if (IBMExtractUtilities.isDB2Installed(remote)) {
				if (this.cfg.pingJDBCDrivers(this.cfg.getDstJDBC())) {
					AddJarsToClasspath(this.cfg.getDstJDBC());
					boolean compatibilityMode = this.comboDstVendor
							.getSelectedIndex() == 0;
					if (IBMExtractUtilities.TestConnection(remote,
							compatibilityMode, this.cfg.getDstVendor(),
							this.cfg.getDstServer(), this.cfg.getDstPort(),
							this.cfg.getDstDBName(), this.cfg.getDstUid(),
							this.cfg.getDstPwd())) {
						boolean isCompatibleMode = IBMExtractUtilities.DB2Compatibility;
						if ((isCompatibleMode)
								&& (srcVendor.equalsIgnoreCase("oracle"))) {
							this.resetDstFields = false;
							this.comboDstVendor.setSelectedIndex(0);
						} else {
							this.resetDstFields = false;
							this.comboDstVendor.setSelectedIndex(1);
						}
						this.cfg.setDB2Compatibility(Boolean
								.toString(isCompatibleMode));
						this.cfg
								.setDstDB2Instance(IBMExtractUtilities.InstanceName);
						this.cfg.setDstDB2Home(IBMExtractUtilities.DB2Path);
						this.cfg
								.setDstDB2Release(IBMExtractUtilities.ReleaseLevel);
						if (this.comboDstVendor.getSelectedIndex() == 0) {
							if (!isCompatibleMode) {
								SetLabelMessage(this.lblMessage,
										IBMExtractUtilities.Message, true);
								return;
							}
							if ((IBMExtractUtilities.Varchar2_Compat
									.equalsIgnoreCase("on"))
									|| (IBMExtractUtilities.Date_Compat
											.equalsIgnoreCase("on"))
									|| (IBMExtractUtilities.Number_Compat
											.equalsIgnoreCase("on"))) {
								this.btnDeploy.setEnabled(true);
								this.btnDropObjs.setEnabled(true);
								this.btnDB2Script.setEnabled(true);
								SetLabelMessage(
										this.lblDB2Instance,
										" Instance Name "
												+ IBMExtractUtilities.InstanceName
												+ " ("
												+ IBMExtractUtilities.ReleaseLevel
												+ ")", false);
								SetLabelMessage(
										this.lblDB2VarcharCompat,
										" varchar2_compat "
												+ IBMExtractUtilities.Varchar2_Compat,
										false);
								SetLabelMessage(
										this.lblDB2DateCompat,
										" date_compat "
												+ IBMExtractUtilities.Date_Compat,
										false);
								SetLabelMessage(
										this.lblDB2NumberCompat,
										" number_compat "
												+ IBMExtractUtilities.Number_Compat,
										false);
								SetLabelMessage(this.lblMessage,
										IBMExtractUtilities.Message, false);
							} else {
								SetLabelMessage(
										this.lblDB2Instance,
										" Instance Name "
												+ IBMExtractUtilities.InstanceName
												+ " ("
												+ IBMExtractUtilities.ReleaseLevel
												+ ")", false);
								SetLabelMessage(
										this.lblDB2VarcharCompat,
										" varchar2_compat "
												+ IBMExtractUtilities.Varchar2_Compat,
										true);
								SetLabelMessage(
										this.lblDB2DateCompat,
										" date_compat "
												+ IBMExtractUtilities.Date_Compat,
										true);
								SetLabelMessage(
										this.lblDB2NumberCompat,
										" number_compat "
												+ IBMExtractUtilities.Number_Compat,
										true);
								SetLabelMessage(
										this.lblMessage,
										"*WARNING* Database is not in Oracle compatibility mode. Drop and re-create it.",
										true);
							}
							if (IBMExtractUtilities.Decflt_rounding
									.equalsIgnoreCase("round_half_up")) {
								SetLabelMessage(
										this.lblDB2Decflt_rounding,
										" decflt_rounding "
												+ IBMExtractUtilities.Decflt_rounding,
										false);
							} else
								SetLabelMessage(
										this.lblDB2Decflt_rounding,
										" *Warning* decflt_rounding is not ROUND_HALF_UP",
										true);
						} else {
							SetLabelMessage(this.lblDB2Instance,
									" Instance Name "
											+ IBMExtractUtilities.InstanceName
											+ " ("
											+ IBMExtractUtilities.ReleaseLevel
											+ ")", false);
							this.btnDeploy.setEnabled(true);
							this.btnDropObjs.setEnabled(true);
							this.btnDB2Script.setEnabled(true);
							SetLabelMessage(this.lblMessage,
									IBMExtractUtilities.Message, false);
						}
					} else {
						SetLabelMessage(this.lblMessage,
								IBMExtractUtilities.Message, true);
					}
				} else {
					SetLabelMessage(this.lblMessage, this.cfg.Message, true);
				}
			} else {
				SetLabelMessage(this.lblMessage, IBMExtractUtilities.Message,
						true);
			}
			setValues();
			this.cfg.writeConfigFile();
			this.cfg.getParamValues();
		} else if (e.getSource().equals(this.checkboxData)) {
			if (this.checkboxData.isSelected()) {
				this.textLimitExtractRows.setEnabled(true);
				this.textLimitLoadRows.setEnabled(true);
			} else {
				this.textLimitExtractRows.setEnabled(false);
				this.textLimitLoadRows.setEnabled(false);
			}
		} else if (e.getSource().equals(this.btnExtract)) {
			boolean dataExtract = this.checkboxData.isSelected();
			boolean ddlExtrcat = this.checkboxDDL.isSelected();
			String limitExtractRows = this.textLimitExtractRows.getText();
			if ((limitExtractRows == null) || (limitExtractRows.length() == 0)) {
				SetLabelMessage(
						this.lblMessage,
						"Specify Limit # of extract rows to be either ALL or a number > 0",
						true);
				return;
			}

			try {
				if (!limitExtractRows.equalsIgnoreCase("ALL")) {
					int x = Integer.parseInt(limitExtractRows);
					if (x < 0) {
						SetLabelMessage(this.lblMessage,
								"Specify Limit # of extract rows > 0", true);
						return;
					}
				}
			} catch (Exception ex) {
				SetLabelMessage(
						this.lblMessage,
						"Specify Limit # of extract rows to be either ALL or a number > 0",
						true);
				return;
			}

			String limitLoadRows = this.textLimitLoadRows.getText();
			if ((limitLoadRows == null) || (limitLoadRows.length() == 0)) {
				SetLabelMessage(
						this.lblMessage,
						"Specify Limit # of load rows to be either ALL or a number > 0",
						true);
				return;
			}

			try {
				if (!limitLoadRows.equalsIgnoreCase("ALL")) {
					int x = Integer.parseInt(limitLoadRows);
					if (x < 0) {
						SetLabelMessage(this.lblMessage,
								"Specify Limit # of load rows > 0", true);
						return;
					}
				}
			} catch (Exception ex) {
				SetLabelMessage(
						this.lblMessage,
						"Specify Limit # of load rows to be either ALL or a number > 0",
						true);
				return;
			}

			String outputDir = this.textfieldOutputDir.getText();
			if ((outputDir == null) || (outputDir.equals(""))) {
				SetLabelMessage(this.lblMessage, "Specify Output Directory",
						true);
				return;
			}
			if (IBMExtractUtilities.FileExists(outputDir)) {
				int n = JOptionPane
						.showConfirmDialog(
								this,
								"Output directory exists. Press YES to delete "
										+ outputDir
										+ "\nPress No if you do not want to delete output directory",
								"Option to delete output directory", 0);

				if (n == 0)
					IBMExtractUtilities.DeleteDir(new File(outputDir));
			}
			if ((!dataExtract) && (!ddlExtrcat)) {
				SetLabelMessage(this.lblMessage,
						"Select DDL or DATA extraction or both.", true);
				return;
			}
			if ((this.cfg.getDstDB2Instance() == null)
					|| (this.cfg.getDstDB2Instance().equals(""))
					|| (this.cfg.getDstDB2Instance().equals("null"))) {
				SetLabelMessage(this.lblMessage,
						"Please connect to DB2 first.", true);
				return;
			}
			if (generateScripts()) {
				this.executingScriptName = (IBMExtractUtilities.osType
						.equalsIgnoreCase("win") ? "unload.cmd" : "unload");
				RunGenerateExtract task = null;
				ExecutorService s = Executors.newFixedThreadPool(1);
				this.lblSplash.setVisible(true);
				task = new RunGenerateExtract(this.buffer,
						this.tailOutputActionListener, outputDir,
						this.executingScriptName);
				s.execute(task);
				s.shutdown();
				SetLabelMessage(this.lblMessage, "Extract started using '"
						+ new File(this.executingScriptName).getName()
						+ "' ...", false);
			}
		} else if (e.getSource().equals(this.btnDeploy)) {
			try {
				int choice = 0;
				if (this.lblDB2Instance.getText().equals("")) {
					choice = JOptionPane.showConfirmDialog(this,
							"Connect to DB2 first", "Connect to DB2 first", -1);

					return;
				}
				choice = 0;
				String wd = this.textfieldOutputDir.getText();
				if ((wd == null) || (wd.equals("")))
					wd = ".";
				if (!IBMExtractUtilities.FileExists(wd))
					wd = ".";
				this.executingScriptName = (wd
						+ System.getProperty("file.separator") + this.cfg
						.getDB2RutimeShellScriptName());
				if (IBMExtractUtilities.FileExists(this.executingScriptName)) {
					choice = JOptionPane.showConfirmDialog(this,
							"Ready to deploy ... \n" + this.executingScriptName
									+ "\nDo you want to run this?",
							"Confirm running of a script", 0);
				} else {
					this.executingScriptName = "";
				}
				if (choice == 1) {
					JFileChooser fc = new JFileChooser();
					File f = new File(new File(this.executingScriptName)
							.getCanonicalPath());
					fc.setCurrentDirectory(f);
					fc.setDialogTitle("Select Shell Script to run.");
					fc.setFileSelectionMode(2);
					fc.addChoosableFileFilter(new FileFilter() {
						public boolean accept(File f) {
							if (f.isDirectory()) {
								return true;
							}

							String s = f.getName();
							int pos = s.lastIndexOf('.');
							if (pos > 0) {
								String ext = s.substring(pos);

								return (ext.equalsIgnoreCase(".sh"))
										|| (ext.equalsIgnoreCase(".cmd"));
							}

							return false;
						}

						public String getDescription() {
							if (IBMExtractUtilities.osType
									.equalsIgnoreCase("win")) {
								return "*.cmd";
							}
							return "*.sh";
						}
					});
					fc.setAcceptAllFileFilterUsed(false);
					int result = fc.showOpenDialog(null);

					if (result == 1) {
						return;
					}

					File fileSelected = null;
					fileSelected = fc.getSelectedFile();
					if ((fileSelected == null)
							|| (fileSelected.getName().equals(""))) {
						this.executingScriptName = "";
					} else {
						this.executingScriptName = fileSelected
								.getAbsolutePath();
					}
				}

				if (!this.executingScriptName.equals("")) {
					if (generateScripts()) {
						RunScript task = null;
						ExecutorService s = Executors.newFixedThreadPool(1);
						this.lblSplash.setVisible(true);
						task = new RunScript(this.buffer,
								this.tailOutputActionListener, this.cfg
										.getDstDB2Instance(), this.cfg
										.getDstDB2Home(),
								this.executingScriptName);
						s.execute(task);
						s.shutdown();
						SetLabelMessage(this.lblMessage, "Script '"
								+ new File(this.executingScriptName).getName()
								+ "' started ...", false);
					}
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		} else if (e.getSource().equals(this.btnDropObjs)) {
			try {
				int choice = 0;
				String wd = this.textfieldOutputDir.getText();
				if ((wd == null) || (wd.equals("")))
					wd = ".";
				if (!IBMExtractUtilities.FileExists(wd))
					wd = ".";
				this.executingScriptName = (wd
						+ System.getProperty("file.separator") + this.cfg
						.getDB2DropObjectsScriptName());
				if (IBMExtractUtilities.FileExists(this.executingScriptName)) {
					choice = JOptionPane.showConfirmDialog(this,
							"Ready to run ... \n" + this.executingScriptName
									+ "\nDo you want to run this?",
							"Confirm running of a script", 0);
				} else {
					this.executingScriptName = "";
				}
				if (choice == 0) {
					RunScript task = null;
					ExecutorService s = Executors.newFixedThreadPool(1);
					this.lblSplash.setVisible(true);
					task = new RunScript(this.buffer,
							this.tailOutputActionListener, this.cfg
									.getDstDB2Instance(), this.cfg
									.getDstDB2Home(), this.executingScriptName);
					s.execute(task);
					s.shutdown();
					SetLabelMessage(this.lblMessage, "Script '"
							+ new File(this.executingScriptName).getName()
							+ "' started ...", false);
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		} else if (e.getSource().equals(this.btnCreateScript)) {
			validateSrcFields();
			validateDstFields();
			generateScripts();
		} else if (e.getSource().equals(this.btnMeetScript)) {
			int choice = 0;
			validateSrcFields();
			if (generateMeetScript()) {
				String wd = this.textfieldOutputDir.getText();
				if ((wd == null) || (wd.equals("")))
					wd = ".";
				if (!IBMExtractUtilities.FileExists(wd))
					wd = ".";
				this.executingScriptName = (wd
						+ System.getProperty("file.separator") + this.cfg
						.getMeetScriptName());
				if (IBMExtractUtilities.FileExists(this.executingScriptName)) {
					choice = JOptionPane.showConfirmDialog(this,
							"Ready to run ... \n" + this.executingScriptName
									+ "\nDo you want to run this?",
							"Confirm running of a script", 0);
				} else {
					this.executingScriptName = "";
				}
				if (choice == 0) {
					RunScript task = null;
					ExecutorService s = Executors.newFixedThreadPool(1);
					this.lblSplash.setVisible(true);
					task = new RunScript(this.buffer,
							this.tailOutputActionListener, this.cfg
									.getDstDB2Instance(), this.cfg
									.getDstDB2Home(), this.executingScriptName);
					s.execute(task);
					s.shutdown();
					SetLabelMessage(this.lblMessage, "Script '"
							+ new File(this.executingScriptName).getName()
							+ "' started ...", false);
				}
			}
		} else if (e.getSource().equals(this.btnDB2Script)) {
			try {
				String wd = this.textfieldOutputDir.getText();
				if ((wd == null) || (wd.equals("")))
					wd = ".";
				if (!IBMExtractUtilities.FileExists(wd))
					wd = ".";
				File f = new File(new File(wd).getCanonicalPath());
				JFileChooser fc = new JFileChooser();
				fc.setCurrentDirectory(f);
				fc.setDialogTitle("Select DB2 Script to run.");
				fc.setFileSelectionMode(0);
				fc.addChoosableFileFilter(new FileFilter() {
					public boolean accept(File f) {
						if (f.isDirectory()) {
							return false;
						}

						String s = f.getName();
						int pos = s.lastIndexOf('.');
						if (pos > 0) {
							String ext = s.substring(pos);

							return ext.equalsIgnoreCase(".db2");
						}

						return false;
					}

					public String getDescription() {
						return "*.db2";
					}
				});
				fc.setAcceptAllFileFilterUsed(false);
				int result = fc.showOpenDialog(null);

				if (result == 1) {
					return;
				}

				File fileSelected = null;
				fileSelected = fc.getSelectedFile();
				if ((fileSelected != null)
						&& (!fileSelected.getName().equals(""))) {
					RunDB2Script task = null;
					this.executingScriptName = fileSelected.getAbsolutePath();

					ExecutorService s = Executors.newFixedThreadPool(1);
					this.lblSplash.setVisible(true);
					task = new RunDB2Script(this.buffer,
							this.tailOutputActionListener, this.cfg
									.getDstDB2Home(), this.executingScriptName);

					s.execute(task);
					s.shutdown();
					SetLabelMessage(this.lblMessage, "Script '"
							+ fileSelected.getName() + "' started ...", false);
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	private void clearSchemaCheckBoxes() {
		for (int i = this.panelCheckBox.getComponentCount() - 1; i >= 0; i--) {
			String name = this.panelCheckBox.getComponent(i).getClass()
					.getName();
			if (name != "javax.swing.JCheckBox")
				continue;
			Component cont = this.panelCheckBox.getComponent(i);
			this.panelCheckBox.remove(cont);
		}
	}

	private String getSchemaList() {
		String schemaList = "";
		int component = this.panelCheckBox.getComponentCount();
		for (int i = 0; i < component; i++) {
			String name = this.panelCheckBox.getComponent(i).getClass()
					.getName();
			if (name != "javax.swing.JCheckBox")
				continue;
			if (i > 0)
				schemaList = schemaList + ":";
			JCheckBox nameCheckBox = (JCheckBox) this.panelCheckBox
					.getComponent(i);
			schemaList = schemaList + nameCheckBox.getText();
		}

		return schemaList;
	}

	private String getSelectedSchemaList() {
		String schemaList = "";
		int component = this.panelCheckBox.getComponentCount();
		for (int i = 0; i < component; i++) {
			String name = this.panelCheckBox.getComponent(i).getClass()
					.getName();
			if (name != "javax.swing.JCheckBox")
				continue;
			JCheckBox nameCheckBox = (JCheckBox) this.panelCheckBox
					.getComponent(i);
			if (!nameCheckBox.isSelected())
				continue;
			if (!schemaList.equals(""))
				schemaList = schemaList + ":";
			schemaList = schemaList + nameCheckBox.getText();
		}

		return schemaList;
	}

	private void setSchemaCheckBoxes(String schemaList) {
		clearSchemaCheckBoxes();
		boolean checkFlag = false;
		String dstSchemaList = this.cfg.getDstSchName();
		String[] tmp = schemaList.split(":");
		String[] tmp2 = dstSchemaList.split(":");
		if (tmp != null) {
			for (int i = 0; i < tmp.length; i++) {
				if (tmp2 != null) {
					checkFlag = false;
					for (int j = 0; j < tmp2.length; j++)
						if (tmp2[j].equals(tmp[i]))
							checkFlag = true;
				}
				if (tmp[i].equals(""))
					continue;
				JCheckBox cb = new JCheckBox(tmp[i], checkFlag);
				this.panelCheckBox.add(cb);
			}

			this.panelCheckBox.revalidate();
		}
	}

	private void setAccessFields(String vendor) {
		if (vendor.equals("access")) {
			this.btnSrcTestConn.setEnabled(false);
			this.btnSrcJDBC.setEnabled(false);
			this.btnExtract.setEnabled(true);
			this.btnCreateScript.setEnabled(true);
			this.btnMeetScript.setEnabled(false);
			this.textfieldSrcPortNum.setEnabled(false);
			this.textfieldSrcDatabase.setEnabled(false);
			this.textfieldSrcUserID.setEnabled(false);
			this.textfieldSrcPassword.setEnabled(false);
			this.textfieldSrcJDBC.setEnabled(false);
			this.cfg.setFetchSize("0");
		} else {
			this.btnSrcTestConn.setEnabled(true);
			this.btnSrcJDBC.setEnabled(true);
			this.btnExtract.setEnabled(false);
			this.btnCreateScript.setEnabled(false);
			this.btnMeetScript.setEnabled(false);
			this.textfieldSrcPortNum.setEnabled(true);
			this.textfieldSrcDatabase.setEnabled(true);
			this.textfieldSrcUserID.setEnabled(true);
			this.textfieldSrcPassword.setEnabled(true);
			this.textfieldSrcJDBC.setEnabled(true);
			this.cfg.setFetchSize("100");
		}
		if (this.cfg.isDataExtracted()) {
			this.btnDeploy.setEnabled(true);
			this.btnDropObjs.setEnabled(true);
			this.btnDB2Script.setEnabled(true);
		} else {
			this.btnDeploy.setEnabled(false);
			this.btnDropObjs.setEnabled(false);
			this.btnDB2Script.setEnabled(false);
		}
		clearSchemaCheckBoxes();
	}

	private void createTableScript(String outputDir, String schemaList) {
		String tableScriptFile = outputDir + IBMExtractUtilities.filesep
				+ this.cfg.getSrcDBName() + ".tables";

		if (IBMExtractUtilities.FileExists(tableScriptFile)) {
			int n = JOptionPane.showConfirmDialog(this, "File "
					+ tableScriptFile + " exists. Press YES to overwrite it."
					+ "\nPress No to use existing file",
					"Option to overwrite table script", 0);

			if (n == 1)
				return;
		}
		System.setProperty("OUTPUT_DIR", outputDir);
		IBMExtractUtilities.CreateTableScript(this.cfg.getSrcVendor(), "",
				schemaList, this.cfg.getSrcServer(), this.cfg.getSrcPort(),
				this.cfg.getSrcDBName(), this.cfg.getSrcUid(), this.cfg
						.getSrcPwd());

		if (IBMExtractUtilities.Message.equals("")) {
			SetLabelMessage(this.lblMessage, this.cfg.getSrcDBName()
					+ ".tables file created for extract.", false);
			SetLabelMessage(this.lblMessage, "Scripts generated successfully",
					false);
		} else {
			SetLabelMessage(this.lblMessage, IBMExtractUtilities.Message, true);
		}
	}

	private boolean generateMeetScript() {
		String srcVendor = (String) this.comboSrcVendor.getSelectedItem();

		String outputDir = this.textfieldOutputDir.getText();
		if ((outputDir == null) || (outputDir.equals(""))) {
			SetLabelMessage(this.lblMessage, "Specify Output Directory", true);
			return false;
		}
		String schemaList = getSelectedSchemaList();
		if (srcVendor.equals("access"))
			schemaList = "ADMIN";
		if (schemaList.equals("")) {
			SetLabelMessage(this.lblMessage, "No schema selected.", true);
			return false;
		}
		this.cfg.setDstSchName(schemaList);

		setValues();
		if (this.cfg.getDstSchName().equals("")) {
			SetLabelMessage(this.lblMessage, "No schema Selected.", true);
			return false;
		}
		try {
			this.cfg.writeMeetScript();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return true;
	}

	private boolean generateScripts() {
		String srcVendor = (String) this.comboSrcVendor.getSelectedItem();

		if (this.textfieldDstDatabase.getText().equals("")) {
			SetLabelMessage(this.lblMessage,
					"Please specify DB2 database name.", true);
			this.textfieldDstDatabase.requestFocusInWindow();
			return false;
		}

		if (this.textfieldDstJDBC.getText().equals("")) {
			SetLabelMessage(this.lblMessage, "Please specify DB2 JDBC name.",
					true);
			this.textfieldDstJDBC.requestFocusInWindow();
			return false;
		}

		String outputDir = this.textfieldOutputDir.getText();
		if ((outputDir == null) || (outputDir.equals(""))) {
			SetLabelMessage(this.lblMessage, "Specify Output Directory", true);
			return false;
		}

		String numThreads = this.textfieldNumTreads.getText();
		if ((numThreads == null) || (numThreads.equals(""))) {
			SetLabelMessage(this.lblMessage, "Specify Number of threads > 0",
					true);
			return false;
		}
		try {
			int num = Integer.valueOf(numThreads).intValue();
			if (num < 0) {
				SetLabelMessage(this.lblMessage,
						"Specify Number of threads > 0", true);
				return false;
			}
		} catch (Exception e) {
			SetLabelMessage(this.lblMessage,
					"Invalid value specified for number of threads.", true);
			return false;
		}

		String schemaList = getSelectedSchemaList();
		if (srcVendor.equals("access"))
			schemaList = "ADMIN";
		if (schemaList.equals("")) {
			SetLabelMessage(this.lblMessage,
					"No schema selected for migration.", true);
			return false;
		}
		this.cfg.setDstSchName(schemaList);

		setValues();
		if (this.cfg.getDstSchName().equals("")) {
			SetLabelMessage(this.lblMessage, "No schema Selected.", true);
			return false;
		}

		this.cfg.writeConfigFile();
		this.cfg.getParamValues();
		try {
			this.cfg.writeGeninput();
			this.cfg.writeUnload(this.cfg.unload);
			this.cfg.writeRowCount();
			createTableScript(outputDir, schemaList);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return true;
	}

	private void validateDstFields() {
		this.txtMessage = "";
		if (this.textfieldDstJDBC.getText().equals("")) {
			this.txtMessage = "Please specify db2 JDBC Drivers";
			return;
		}
		if (this.textfieldDstServer.getText().equals("")) {
			this.txtMessage = "Please specify destination server";
			return;
		}
		if (this.textfieldDstPortNum.getText().equals("")) {
			this.txtMessage = "Please specify destination port number";
			return;
		}

		try {
			Integer.parseInt(this.textfieldDstPortNum.getText());
		} catch (Exception e) {
			this.txtMessage = "Invalid number. Please specify destination port number";
			return;
		}

		if (this.textfieldDstDatabase.getText().equals("")) {
			this.txtMessage = "Please specify destination database";
			return;
		}
		if (this.textfieldDstUserID.getText().equals("")) {
			this.txtMessage = "Please specify destination user id";
			return;
		}
		if (this.textfieldDstPassword.getText().equals("")) {
			this.txtMessage = "Please specify destination password";
			return;
		}
	}

	private void setValues() {
		this.cfg.setSrcServer(this.textfieldSrcServer.getText());
		this.cfg.setSrcPort(this.textfieldSrcPortNum.getText());
		if ((((String) this.comboSrcVendor.getSelectedItem()).equals("db2"))
				|| (((String) this.comboSrcVendor.getSelectedItem())
						.equals("zdb2"))) {
			this.cfg.setSrcDBName(this.textfieldSrcDatabase.getText()
					.toUpperCase());
		} else {
			this.cfg.setSrcDBName(this.textfieldSrcDatabase.getText());
		}
		String tmpVendor = (String) this.comboDstVendor.getSelectedItem();
		this.cfg.setDB2Compatibility("false");
		if (tmpVendor.equals("DB2 With Compatibility Mode")) {
			this.cfg.setDB2Compatibility("true");
		}
		this.cfg.setSrcPwd(this.textfieldSrcPassword.getText());
		this.cfg.setSrcUid(this.textfieldSrcUserID.getText());
		this.cfg.setSrcVendor((String) this.comboSrcVendor.getSelectedItem());
		this.cfg.setSrcJDBC(this.textfieldSrcJDBC.getText());
		this.cfg.setExtractDDL(String.valueOf(this.checkboxDDL.isSelected()));
		this.cfg.setExtractData(String.valueOf(this.checkboxData.isSelected()));
		this.cfg.setLimitExtractRows(this.textLimitExtractRows.getText());
		this.cfg.setLimitLoadRows(this.textLimitLoadRows.getText());
		this.cfg.setNumThreads(this.textfieldNumTreads.getText());
		this.cfg.setDstServer(this.textfieldDstServer.getText());
		this.cfg.setDstPort(this.textfieldDstPortNum.getText());
		this.cfg
				.setDstDBName(this.textfieldDstDatabase.getText().toUpperCase());
		this.cfg.setDstPwd(this.textfieldDstPassword.getText());
		this.cfg.setDstUid(this.textfieldDstUserID.getText());
		this.cfg.setDstVendor(this.dstVendor);
		this.cfg.setDstJDBC(this.textfieldDstJDBC.getText());
		this.cfg.setOutputDirectory(this.textfieldOutputDir.getText());
		this.cfg.setSrcSchName(getSchemaList());
		this.cfg.setDstSchName(getSelectedSchemaList());
		this.cfg.setDbclob(this.optionCodes[MenuBarView.OPTION_DBCLOBS
				.intValue()][1]);
		this.cfg
				.setTrimTrailingSpaces(this.optionCodes[MenuBarView.OPTION_TRAILING_BLANKS
						.intValue()][1]);
		this.cfg.setGraphic(this.optionCodes[MenuBarView.OPTION_GRAPHICS
				.intValue()][1]);
		this.cfg
				.setRegenerateTriggers(this.optionCodes[MenuBarView.OPTION_SPLIT_TRIGGER
						.intValue()][1]);
		boolean isRemote = IBMExtractUtilities
				.isIPLocal(this.textfieldDstServer.getText());
		this.cfg.setRemoteLoad(isRemote ? "false" : "true");
		this.cfg
				.setCompressTable(this.optionCodes[MenuBarView.OPTION_COMPRESS_TABLE
						.intValue()][1]);
		this.cfg
				.setCompressIndex(this.optionCodes[MenuBarView.OPTION_COMPRESS_INDEX
						.intValue()][1]);
		this.cfg
				.setExtractPartitions(this.optionCodes[MenuBarView.OPTION_EXTRACT_PARTITIONS
						.intValue()][1]);
		this.cfg
				.setExtractHashPartitions(this.optionCodes[MenuBarView.OPTION_EXTRACT_HASH_PARTITIONS
						.intValue()][1]);
	}

	private void validateSrcFields() {
		this.txtMessage = "";
		if (this.textfieldSrcJDBC.getText().equals("")) {
			this.txtMessage = "Please specify source JDBC Driver name";
			return;
		}
		if (this.textfieldSrcServer.getText().equals("")) {
			this.txtMessage = "Please specify source server";
			return;
		}
		if (this.textfieldSrcPortNum.getText().equals("")) {
			this.txtMessage = "Please specify source port number";
			return;
		}

		try {
			Integer.parseInt(this.textfieldSrcPortNum.getText());
		} catch (Exception e) {
			this.txtMessage = "Invalid number. Please specify source port number";
			return;
		}

		if (this.textfieldSrcDatabase.getText().equals("")) {
			this.txtMessage = "Please specify source database";
			return;
		}
		if (this.textfieldSrcUserID.getText().equals("")) {
			this.txtMessage = "Please specify source user id";
			return;
		}
		if (this.textfieldSrcPassword.getText().equals("")) {
			this.txtMessage = "Please specify source password";
			return;
		}
	}

	private void getValues() {
		String tmpVendor = "";
		this.comboSrcVendor.setSelectedItem(this.cfg.getSrcVendor());
		tmpVendor = (String) this.comboSrcVendor.getSelectedItem();
		if (this.cfg.getSrcVendor().equals("oracle")) {
			this.comboDstVendor.setEnabled(true);
			if (this.cfg.getDB2Compatibility().equals("true"))
				this.comboDstVendor.setSelectedIndex(0);
			else
				this.comboDstVendor.setSelectedIndex(1);
		} else {
			this.comboDstVendor.setEnabled(false);
			this.comboDstVendor.setSelectedIndex(1);
		}
		this.textfieldSrcServer.setText(this.cfg.getSrcServer());
		this.textfieldDstServer.setText(this.cfg.getDstServer());
		if (this.cfg.getSrcPort() == 0)
			this.textfieldSrcPortNum.setText(""
					+ this.cfg.getDefaultVendorPort(tmpVendor));
		else
			this.textfieldSrcPortNum.setText("" + this.cfg.getSrcPort());
		if (this.cfg.getDstPort() == 0)
			this.textfieldDstPortNum.setText(""
					+ this.cfg.getDefaultVendorPort("db2"));
		else
			this.textfieldDstPortNum.setText("" + this.cfg.getDstPort());
		this.textfieldSrcDatabase.setText(this.cfg.getSrcDBName());
		this.textfieldDstDatabase.setText(this.cfg.getDstDBName());
		this.textfieldSrcPassword.setText(this.cfg.getSrcPwd());
		this.textfieldDstPassword.setText(this.cfg.getDstPwd());
		this.textfieldSrcUserID.setText(this.cfg.getSrcUid());
		this.textfieldDstUserID.setText(this.cfg.getDstUid());
		this.btnSrcTestConn.setText("Connect to " + tmpVendor.toUpperCase());
		this.checkboxDDL.setSelected(Boolean.valueOf(this.cfg.getExtractDDL())
				.booleanValue());
		this.checkboxData.setSelected(Boolean
				.valueOf(this.cfg.getExtractData()).booleanValue());
		this.textLimitExtractRows.setText(this.cfg.getLimitExtractRows());
		this.textLimitLoadRows.setText(this.cfg.getLimitLoadRows());
		this.textfieldNumTreads.setText(this.cfg.getNumThreads());
		this.textfieldSrcJDBC.setText(this.cfg.getSrcJDBC());
		this.textfieldDstJDBC.setText(this.cfg.getDstJDBC());
		this.textfieldOutputDir.setText(this.cfg.getOutputDirectory());

		this.optionCodes[MenuBarView.OPTION_DBCLOBS.intValue()][1] = this.cfg
				.getDbclob();
		this.optionCodes[MenuBarView.OPTION_TRAILING_BLANKS.intValue()][1] = this.cfg
				.getTrimTrailingSpaces();
		this.optionCodes[MenuBarView.OPTION_GRAPHICS.intValue()][1] = this.cfg
				.getGraphic();
		this.optionCodes[MenuBarView.OPTION_SPLIT_TRIGGER.intValue()][1] = this.cfg
				.getRegenerateTriggers();
		this.optionCodes[MenuBarView.OPTION_COMPRESS_TABLE.intValue()][1] = this.cfg
				.getCompressTable();
		this.optionCodes[MenuBarView.OPTION_COMPRESS_INDEX.intValue()][1] = this.cfg
				.getCompressIndex();
		this.optionCodes[MenuBarView.OPTION_EXTRACT_PARTITIONS.intValue()][1] = this.cfg
				.getExtractPartitions();
		this.optionCodes[MenuBarView.OPTION_EXTRACT_HASH_PARTITIONS.intValue()][1] = this.cfg
				.getExtractHashPartitions();
		setAccessFields(tmpVendor);
		setSchemaCheckBoxes(this.cfg.getSrcSchName());
	}

	public String getDB2InstanceName() {
		return this.lblDB2Instance.getText();
	}

	public String getTextfieldOutputDir() {
		return this.textfieldOutputDir.getText();
	}
}