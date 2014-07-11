package ibm;

import com.jgoodies.forms.factories.Borders;
import com.jgoodies.looks.LookUtils;
import com.jgoodies.uif_lite.component.Factory;
import com.jgoodies.uif_lite.component.UIFSplitPane;
import com.jgoodies.uif_lite.panel.SimpleInternalFrame;
import java.awt.*;
import java.awt.event.*;
import java.net.URL;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.swing.*;
import javax.swing.Timer;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.table.*;
import javax.swing.tree.*;
import jsyntaxpane.DefaultSyntaxKit;
import jsyntaxpane.SyntaxDocument;
import jsyntaxpane.actions.ActionUtils;

public class SplitExtractTab extends JFrame implements TreeSelectionListener {
	private static final long serialVersionUID = 1876263871408702989L;
	public static String linesep = System.getProperty("line.separator");
	private IBMExtractConfig cfg;
	private Connection mainConn = null;
	private String outputDirectory;
	private String sqlTerminator;
	private JLabel lblSplash = IBMExtractGUI2.lblSplash;
	private ActionListener executeActionListener;
	private ActionListener revalidateActionListener;
	private ActionListener executeAllActionListener;
	private ActionListener revalidateAllActionListener;
	private ActionListener discardActionListener;
	private JTree treeStatements;
	private JTable table = new JTable();

	private CustomTreeCellRenderer ctcr = new CustomTreeCellRenderer();
	private JTabbedPane tabbedPane;
	private DefaultMutableTreeNode rootStatementTree;
	private JComponent lowerRight;
	private Timer busy = null;
	private RunDeployObjects task = null;
	private JEditorPane topArea;
	private JPopupMenu popup;
	private Hashtable<String, PLSQLInfo> hashPLSQLSource = new Hashtable<String, PLSQLInfo>();

	ImageIcon errorIcon = readImageIcon("error.png");
	ImageIcon passedIcon = readImageIcon("passed.gif");
	ImageIcon failedIcon = readImageIcon("gol.gif");
	ImageIcon removeIcon = readImageIcon("remove.gif");

	public SplitExtractTab(IBMExtractConfig cfg,
			ActionListener executeAllActionListener,
			ActionListener revalidateActionListener,
			ActionListener executeActionListener,
			ActionListener revalidateAllActionListener,
			ActionListener discardActionListener) {
		this.cfg = cfg;
		this.sqlTerminator = (cfg.getDB2Compatibility().equals("true") ? "/"
				: "@");
		this.executeAllActionListener = executeAllActionListener;
		this.revalidateActionListener = revalidateActionListener;
		this.executeActionListener = executeActionListener;
		this.revalidateAllActionListener = revalidateAllActionListener;
		this.discardActionListener = discardActionListener;

		this.popup = new JPopupMenu();

		JMenuItem mi = new JMenuItem("Get Source");
		mi.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				SplitExtractTab.this.getSource();
			}
		});
		mi.setActionCommand("Source");
		this.popup.add(mi);

		mi = new JMenuItem("Get Detailed Error Message");
		mi.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				SplitExtractTab.this.getDetailedErrorMessage();
			}
		});
		mi.setActionCommand("ErrorMessage");
		this.popup.add(mi);
		SetTimer();
	}

	private void getDetailedErrorMessage() {
		int rowIndex = this.table.getSelectedRow();

		String type = this.table.getValueAt(rowIndex, 0).toString();
		String schema = this.table.getValueAt(rowIndex, 1).toString();
		String objectName = this.table.getValueAt(rowIndex, 2).toString();
		String errorCode = this.table.getValueAt(rowIndex, 4).toString();
		String lineNum = this.table.getValueAt(rowIndex, 5).toString();
		String message = this.table.getValueAt(rowIndex, 6).toString();
		this.topArea.setText("SQL Error Code = " + errorCode + linesep
				+ "Object Type    = " + type + linesep + "Object Name    = "
				+ objectName + linesep + "Schema         = " + schema + linesep
				+ "Line Num       = " + lineNum + linesep + "Message        = "
				+ message + linesep);

		this.topArea.setCaretPosition(0);
	}

	private void getSource() {
		int rowIndex = this.table.getSelectedRow();

		String type = this.table.getValueAt(rowIndex, 0).toString();
		String schema = this.table.getValueAt(rowIndex, 1).toString();
		String object = this.table.getValueAt(rowIndex, 2).toString();
		String code = this.table.getValueAt(rowIndex, 3).toString();
		String lineNum = this.table.getValueAt(rowIndex, 5).toString();
		this.topArea.setText(getNode(type, schema, code + object));

		if ((lineNum != null) && (!lineNum.equals(""))) {
			int pos = ActionUtils.getDocumentPosition(this.topArea, Integer
					.parseInt(lineNum), 0);
			this.topArea.setCaretPosition(pos);
		} else {
			this.topArea.setCaretPosition(0);
		}
	}

	JComponent build() {
		JPanel panel = new JPanel(new BorderLayout());
		panel.setOpaque(false);
		panel.setBorder(Borders.DIALOG_BORDER);
		panel.add(buildHorizontalSplit());
		return panel;
	}

	private void SetTimer() {
		if (this.busy == null) {
			this.busy = new Timer(1000, new ActionListener() {
				public void actionPerformed(ActionEvent a) {
					if (IBMExtractUtilities.DeployCompleted) {
						IBMExtractUtilities.DeployCompleted = false;
						SplitExtractTab.this.lblSplash.setVisible(false);
						SplitExtractTab.this
								.refreshTable(SplitExtractTab.this.task
										.getTabData());
					}
				}
			});
			this.busy.start();
		}
	}

	private void connectToDB2() {
		this.mainConn = IBMExtractUtilities.OpenConnection(this.cfg
				.getDstVendor(), this.cfg.getDstServer(),
				this.cfg.getDstPort(), this.cfg.getDstDBName(), this.cfg
						.getDstUid(), this.cfg.getDstPwd());

		Object[][] tabData = new Object[1][7];
		if (this.mainConn == null) {
			tabData[0][0] = "Connection";
			tabData[0][1] = "Failure";
			tabData[0][2] = "";
			tabData[0][3] = "0";
			tabData[0][4] = "";
			tabData[0][5] = "";
			tabData[0][6] = IBMExtractUtilities.Message;
		} else {
			tabData[0][0] = "Connection";
			tabData[0][1] = "Success";
			tabData[0][2] = "";
			tabData[0][3] = "1";
			tabData[0][4] = "";
			tabData[0][5] = "";
			tabData[0][6] = "Connection to DB2 succeeded.";
		}
		refreshTable(tabData);
	}

	private JComponent buildHorizontalSplit() {
		JComponent left = new JScrollPane(buildMainLeftPanel());
		left.setPreferredSize(new Dimension(200, 100));
		DefaultSyntaxKit.initKit();
		topArea = new JEditorPane();
		JComponent upperRight = new JScrollPane(topArea);
		upperRight.setPreferredSize(new Dimension(100, 400));
		((JScrollPane) upperRight).setVerticalScrollBarPolicy(20);
		((JScrollPane) upperRight).setHorizontalScrollBarPolicy(30);
		topArea.setContentType("text/sql");
		topArea.setFont(new Font("Monospaced", 0, 13));
		((JScrollPane) upperRight).setViewportView(topArea);
		SyntaxDocument sDoc = null;
		try {
			sDoc = (SyntaxDocument) topArea.getDocument();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		sDoc.clearUndos();
		lowerRight = new JScrollPane(
				refreshTable(new Object[][] { new Object[] { "", "", "", "",
						"", "", "" } }));
		lowerRight.setPreferredSize(new Dimension(100, 40));
		JSplitPane verticalSplit = UIFSplitPane.createStrippedSplitPane(0,
				upperRight, lowerRight);
		verticalSplit.setOpaque(false);
		JSplitPane horizontalSplit = UIFSplitPane.createStrippedSplitPane(1,
				left, verticalSplit);
		horizontalSplit.setOpaque(false);
		return horizontalSplit;
	}

	private JTable refreshTable(Object[][] TableData) {
		TableModel model = new SampleTableModel(TableData, new String[] {
				"Type", "Schema", "Object Name", "Status", "SQL Code",
				"Line #", "Message" });

		this.table.setModel(model);
		this.table.setSelectionMode(0);
		this.table.setAutoResizeMode(4);
		this.table.getColumnModel().getColumn(0).setPreferredWidth(65);
		this.table.getColumnModel().getColumn(1).setPreferredWidth(65);
		this.table.getColumnModel().getColumn(2).setPreferredWidth(115);
		this.table.getColumnModel().getColumn(3).setPreferredWidth(50);
		this.table.getColumnModel().getColumn(3).setCellRenderer(
				new DefaultTableCellRenderer() {
					/**
					 * 2011-3-25
					 */
					private static final long serialVersionUID = 1L;

					public Component getTableCellRendererComponent(
							JTable table, Object value, boolean isSelected,
							boolean hasFocus, int row, int column) {
						if (value.equals("0"))
							setIcon(SplitExtractTab.this.errorIcon);
						else if (value.equals("1"))
							setIcon(SplitExtractTab.this.passedIcon);
						else if (value.equals("2"))
							setIcon(SplitExtractTab.this.failedIcon);
						else
							setIcon(SplitExtractTab.this.removeIcon);
						setHorizontalAlignment(0);
						return this;
					}
				});
		this.table.getColumnModel().getColumn(4).setPreferredWidth(40);
		this.table.getColumnModel().getColumn(5).setPreferredWidth(40);
		this.table.getColumnModel().getColumn(6).setPreferredWidth(220);
		this.table.getColumnModel().getColumn(6).setCellRenderer(
				new DefaultTableCellRenderer() {
					/**
					 * 2011-3-25
					 */
					private static final long serialVersionUID = 1L;

					public Component getTableCellRendererComponent(
							JTable table, Object value, boolean isSelected,
							boolean hasFocus, int row, int column) {
						setText(value.toString());

						return this;
					}
				});
		int tableFontSize = this.table.getFont().getSize();
		int minimumRowHeight = tableFontSize + 6;
		int defaultRowHeight = LookUtils.IS_LOW_RESOLUTION ? 17 : 18;
		this.table.setRowHeight(Math.max(minimumRowHeight, defaultRowHeight));
		this.table.addMouseListener(new MouseAdapter() {
			public void mouseClicked(MouseEvent e) {
				JTable target = (JTable) e.getSource();
				if ((SwingUtilities.isRightMouseButton(e) == true)
						&& (e.getClickCount() == 1)) {
					int row = target.rowAtPoint(e.getPoint());
					target.clearSelection();
					target.addRowSelectionInterval(row, row);

					SplitExtractTab.this.popup.show(e.getComponent(), e.getX(),
							e.getY());
				} else if (e.getClickCount() == 2) {
					SplitExtractTab.this.getSource();
				}
			}
		});
		this.table.updateUI();
		return this.table;
	}

	public void refreshTable() {
		Object[][] obj = { { "FUNCTION", "CIGWMS", "CIG_ROUND_QTY_FN", "0",
				"100", "-1", "Ok Compile" } };
		refreshTable(obj);
	}

	private JComponent buildMainLeftPanel() {
		SimpleInternalFrame sif = new SimpleInternalFrame("Select DB2 Objects");
		sif.setPreferredSize(new Dimension(150, 100));

		this.tabbedPane = new JTabbedPane(3);
		this.tabbedPane
				.putClientProperty("jgoodies.embeddedTabs", Boolean.TRUE);
		this.tabbedPane.addTab("DB2 Objects", Factory
				.createStrippedScrollPane(buildStatementsTree()));

		sif.add(this.tabbedPane);
		return sif;
	}

	private JTree buildStatementsTree() {
		this.rootStatementTree = new DefaultMutableTreeNode(
				" Deploy Objects in DB2");
		this.treeStatements = new CustomJTree(this.rootStatementTree,
				this.executeAllActionListener,
				this.revalidateAllActionListener, this.executeActionListener,
				this.revalidateActionListener, this.discardActionListener);

		this.treeStatements.addTreeSelectionListener(this);

		this.treeStatements.setCellRenderer(this.ctcr);
		return this.treeStatements;
	}

	public void executeAllObjects() {
		ExecutorService s = Executors.newFixedThreadPool(1);
		this.lblSplash.setVisible(true);
		this.task = new RunDeployObjects(this.sqlTerminator,
				this.outputDirectory, this.treeStatements, true, null);
		this.lblSplash.setVisible(true);
		s.execute(this.task);
		s.shutdown();
	}

	public void executeSelectedObjects() {
		ExecutorService s = Executors.newFixedThreadPool(1);
		this.lblSplash.setVisible(true);
		this.task = new RunDeployObjects(this.sqlTerminator,
				this.outputDirectory, this.treeStatements, false, this.topArea);
		this.lblSplash.setVisible(true);
		s.execute(this.task);
		s.shutdown();
	}

	public void discardSelectedObjects() {
		TreePath[] paths = this.treeStatements.getSelectionPaths();
		if (paths == null)
			return;

		int count = 0;
		for (int i = 0; i < paths.length; i++) {
			DefaultMutableTreeNode node = (DefaultMutableTreeNode) paths[i]
					.getLastPathComponent();
			if (node == null)
				continue;
			if (!node.isLeaf())
				continue;
			count++;
		}

		Object[][] tabData = new Object[count][7];
		count = 0;
		for (int i = 0; i < paths.length; i++) {
			DefaultMutableTreeNode node = (DefaultMutableTreeNode) paths[i]
					.getLastPathComponent();
			if (node == null)
				return;

			Object nodeInfo = node.getUserObject();
			if (!node.isLeaf())
				continue;
			PLSQLInfo plsql = (PLSQLInfo) nodeInfo;
			plsql.codeStatus = "3";
			node.setUserObject(plsql);
			((DefaultTreeModel) this.treeStatements.getModel())
					.nodeStructureChanged(node);
			tabData[count][0] = plsql.type;
			tabData[count][1] = plsql.schema;
			tabData[count][2] = plsql.object;
			tabData[count][3] = plsql.codeStatus;
			tabData[count][4] = "-1";
			tabData[count][5] = "";
			tabData[count][6] = "Object was not chosen to be deployed.";
			count++;
		}

		if (count > 0)
			refreshTable(tabData);
	}

	public void refreshStatementsTree(Hashtable<String, String> hashTree,
			Hashtable<String, PLSQLInfo> hashPLSQLSource,
			String outputDirectory, String[][] typeCodes) {
		this.hashPLSQLSource = hashPLSQLSource;
		this.outputDirectory = outputDirectory;
		DefaultTreeModel model = (DefaultTreeModel) this.treeStatements
				.getModel();

		this.rootStatementTree.removeAllChildren();
		model.reload();
		this.rootStatementTree.add(processHashTable(hashTree, typeCodes));
		int row = 0;
		while (row < this.treeStatements.getRowCount()) {
			this.treeStatements.expandRow(row);
			row++;
		}
		connectToDB2();
	}

	private TreePath find2(JTree tree, TreePath parent, Object[] nodes,
			int depth, boolean byName) {
		TreeNode node = (TreeNode) parent.getLastPathComponent();
		Object o = node;

		if (byName) {
			o = o.toString();
		}
		Enumeration<TreeNode> e;
		if (o.equals(nodes[depth])) {
			if (depth == nodes.length - 1) {
				return parent;
			}

			if (node.getChildCount() >= 0) {
				for (e = node.children(); e.hasMoreElements();) {
					TreeNode n = (TreeNode) e.nextElement();
					TreePath path = parent.pathByAddingChild(n);
					TreePath result = find2(tree, path, nodes, depth + 1,
							byName);
					if (result != null) {
						return result;
					}
				}
			}
		}
		return null;
	}

	private TreePath find(JTree tree, Object[] nodes) {
		TreeNode root = (TreeNode) tree.getModel().getRoot();
		return find2(tree, new TreePath(root), nodes, 0, false);
	}

	private TreePath findByName(JTree tree, String[] names) {
		TreeNode root = (TreeNode) tree.getModel().getRoot();
		return find2(tree, new TreePath(root), names, 0, true);
	}

	private String getNode(String type, String schema, String object) {
		TreePath path = findByName(this.treeStatements, new String[] {
				" Deploy Objects in DB2", "Objects", type, schema, object });
		if (path == null)
			return "";
		this.treeStatements.setSelectionPath(path);
		this.treeStatements.scrollPathToVisible(path);
		Object obj = path.getLastPathComponent();
		DefaultMutableTreeNode node = (DefaultMutableTreeNode) obj;
		Object nodeInfo = node.getUserObject();
		PLSQLInfo plsql = (PLSQLInfo) nodeInfo;
		return plsql.plSQLCode;
	}

	private DefaultMutableTreeNode processHashTable(
			Hashtable<String, String> hash, String[][] typeCodes) {
		DefaultMutableTreeNode node = new DefaultMutableTreeNode("Objects");
		DefaultMutableTreeNode child = null;
		for (int idx = 0; idx < typeCodes.length; idx++) {
			if (!hash.containsKey(typeCodes[idx][0]))
				continue;
			if (!typeCodes[idx][1].equals("true"))
				continue;
			String oldSchema = "";
			String key = typeCodes[idx][0];
			String[] vals = ((String) hash.get(key)).split(":");

			DefaultMutableTreeNode parent = new DefaultMutableTreeNode(key);
			node.add(parent);
			Arrays.sort(vals);
			for (int j = 0; j < vals.length; j++) {
				String[] nameArray = vals[j].split("\\.");
				String newSchema = nameArray[0];
				if (!oldSchema.equalsIgnoreCase(newSchema)) {
					child = new DefaultMutableTreeNode(nameArray[0]);
					parent.add(child);
					oldSchema = newSchema;
				}
				PLSQLInfo plsql = (PLSQLInfo) this.hashPLSQLSource.get(key
						+ ":" + vals[j].replace('.', ':'));
				if (plsql == null)
					IBMExtractUtilities.log("plsql is null, Check " + key
							+ " for " + vals[j]);
				else {
					child.add(new DefaultMutableTreeNode(new PLSQLInfo(
							plsql.codeStatus, key, newSchema, nameArray[1],
							plsql.lineNumber, plsql.plSQLCode)));
				}
			}

		}

		return node;
	}

	protected static ImageIcon readImageIcon(String filename) {
		URL url = IBMExtractGUI2.class.getResource("resources/images/"
				+ filename);
		return new ImageIcon(url);
	}

	public void valueChanged(TreeSelectionEvent e) {
		Object obj = this.treeStatements.getLastSelectedPathComponent();

		if (obj == null) {
			return;
		}

		DefaultMutableTreeNode node = (DefaultMutableTreeNode) obj;
		Object nodeInfo = node.getUserObject();
		if (node.isLeaf()) {
			if ((nodeInfo instanceof PLSQLInfo)) {
				PLSQLInfo plsql = (PLSQLInfo) nodeInfo;
				this.topArea.setText(plsql.plSQLCode);
			}
		}
	}

	class CustomTreeCellRenderer extends DefaultTreeCellRenderer {
		private static final long serialVersionUID = -8502054967098094234L;

		CustomTreeCellRenderer() {
		}

		public Component getTreeCellRendererComponent(JTree tree, Object value,
				boolean sel, boolean expanded, boolean leaf, int row,
				boolean hasFocus) {
			Component c = super.getTreeCellRendererComponent(tree, value, sel,
					expanded, leaf, row, hasFocus);

			Object nodeObj = ((DefaultMutableTreeNode) value).getUserObject();
			if (leaf) {
				if ((c instanceof JLabel)) {
					String str = nodeObj.toString();
					((JLabel) c).setText(str.substring(1));
					if (str.charAt(0) == '0') {
						setIcon(SplitExtractTab.this.errorIcon);
					} else if (str.charAt(0) == '1') {
						setIcon(SplitExtractTab.this.passedIcon);
					} else if (str.charAt(0) == '2') {
						setIcon(SplitExtractTab.this.failedIcon);
					} else {
						setIcon(SplitExtractTab.this.removeIcon);
					}
				}
			}
			return this;
		}
	}

	private static final class SampleTableModel extends AbstractTableModel {
		private static final long serialVersionUID = 3789421824163250805L;
		private final String[] columnNames;
		private final Object[][] rowData;

		SampleTableModel(Object[][] rowData, String[] columnNames) {
			this.columnNames = columnNames;
			this.rowData = rowData;
		}

		public String getColumnName(int column) {
			return this.columnNames[column].toString();
		}

		public int getRowCount() {
			return this.rowData.length;
		}

		public int getColumnCount() {
			return this.columnNames.length;
		}

		public Class<?> getColumnClass(int column) {
			return column == 3 ? super.getColumnClass(column) : super
					.getColumnClass(column);
		}

		public Object getValueAt(int row, int col) {
			return this.rowData[row][col];
		}

		public boolean isCellEditable(int row, int column) {
			return false;
		}

		public void setValueAt(Object value, int row, int col) {
			this.rowData[row][col] = value;
			fireTableCellUpdated(row, col);
		}
	}
}