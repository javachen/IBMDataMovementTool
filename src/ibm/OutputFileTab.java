package ibm;

import com.jgoodies.forms.builder.PanelBuilder;
import com.jgoodies.forms.layout.CellConstraints;
import com.jgoodies.forms.layout.FormLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;
import java.io.IOException;
import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;
import javax.swing.border.CompoundBorder;
import javax.swing.border.EmptyBorder;
import javax.swing.border.LineBorder;
import javax.swing.filechooser.FileFilter;

public class OutputFileTab extends JFrame implements ActionListener {
	private static final long serialVersionUID = 301580586163894447L;
	private String fileName;
	private String dirName;
	private JTextArea textArea;
	private JPopupMenu popup;
	private StringBuffer outputBuffer;

	public JTextArea getTextArea() {
		return this.textArea;
	}

	public void setTextArea(JTextArea textArea) {
		this.textArea = textArea;
	}

	public OutputFileTab(StringBuffer outputBuffer) {
		this.outputBuffer = outputBuffer;
	}

	private JScrollPane createArea(String text, boolean lineWrap, int columns,
			Dimension minimumSize) {
		this.textArea = new JTextArea(text);
		this.textArea.setBorder(new CompoundBorder(new LineBorder(Color.GRAY),
				new EmptyBorder(1, 3, 1, 1)));

		this.textArea.setLineWrap(lineWrap);
		this.textArea.setWrapStyleWord(true);
		this.textArea.setColumns(columns);

		JScrollPane scrollPane = new JScrollPane(this.textArea);
		scrollPane.setVerticalScrollBarPolicy(20);
		scrollPane.setHorizontalScrollBarPolicy(30);

		Font font = new Font("monospaced", 1, 15);
		this.textArea.setFont(font);
		this.textArea.setForeground(Color.BLUE);

		if (minimumSize != null) {
			this.textArea.setMinimumSize(new Dimension(100, 32));
		}

		this.popup = new JPopupMenu();

		JMenuItem mi = new JMenuItem("Clear output");
		mi.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				OutputFileTab.this.outputBuffer.setLength(0);
				OutputFileTab.this.textArea.setText("");
			}
		});
		mi.setActionCommand("Source");
		this.popup.add(mi);

		this.textArea.addMouseListener(new MouseAdapter() {
			public void mouseClicked(MouseEvent e) {
				if ((SwingUtilities.isRightMouseButton(e) == true)
						&& (e.getClickCount() == 1)) {
					OutputFileTab.this.popup.show(e.getComponent(), e.getX(), e
							.getY());
				} else if (e.getClickCount() == 2) {
					OutputFileTab.this.outputBuffer.setLength(0);
					OutputFileTab.this.textArea.setText("");
				}
			}
		});
		return scrollPane;
	}

	private JComponent buildTab(JScrollPane area) {
		FormLayout layout = new FormLayout("fill:200dlu:grow",
				"fill:default:grow");

		PanelBuilder builder = new PanelBuilder(layout);
		builder.setDefaultDialogBorder();
		CellConstraints cc = new CellConstraints();
		builder.add(area, cc.xy(1, 1));
		return builder.getPanel();
	}

	JComponent build() {
		String str = "";

		return buildTab(createArea(str, true, 0, null));
	}

	private void openDialog() {
		JFileChooser fc = new JFileChooser();
		fc.setFileSelectionMode(2);
		fc.addChoosableFileFilter(new MyFilter());
		fc.setCurrentDirectory(new File(this.dirName));
		fc.setAcceptAllFileFilterUsed(false);

		int result = fc.showOpenDialog(this);

		if (result == 1) {
			return;
		}

		File fp = fc.getSelectedFile();
		if ((fp == null) || (fp.getName().equals(""))) {
			JOptionPane.showMessageDialog(null, "Error", "Error", 0);
		} else
			try {
				setTitle(fp.getAbsolutePath());
				this.textArea.setText(IBMExtractUtilities.FileContents(fp
						.getAbsolutePath()));
				this.textArea.setCaretPosition(0);
				this.textArea.setEditable(false);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
	}

	public void FillTextAreaFromOutput(String scriptName) {
		this.fileName = scriptName;
		if (IBMExtractUtilities.osType.equalsIgnoreCase("win"))
			this.fileName = this.fileName.replaceAll("\\.cmd", "_OUTPUT.TXT");
		else {
			this.fileName = this.fileName.replaceAll("\\.sh", ".log");
		}
		if (IBMExtractUtilities.FileExists(this.fileName)) {
			int n = JOptionPane.showConfirmDialog(this,
					"Do you want to view the output log file?",
					"View output log file", 0);

			if (n == 0)
				try {
					setTitle(this.fileName);
					this.textArea.setText(IBMExtractUtilities
							.FileContents(this.fileName));
					this.textArea.setCaretPosition(0);
				} catch (IOException e1) {
					e1.printStackTrace();
				}
		}
	}

	public void FillTextAreaFromFile(String dirName) {
		if ((dirName == null) || (dirName.equals("")))
			this.dirName = ".";
		else {
			this.dirName = dirName;
		}
		openDialog();
	}

	public void actionPerformed(ActionEvent e) {
	}

	private static class MyFilter extends FileFilter {
		public boolean accept(File f) {
			if (f.isDirectory()) {
				return true;
			}

			String s = f.getName();
			int pos = s.lastIndexOf('.');
			if (pos > 0) {
				String ext = s.substring(pos);

				return (ext.equalsIgnoreCase(".sql"))
						|| (ext.equalsIgnoreCase(".db2"))
						|| (ext.equalsIgnoreCase(".sh"))
						|| (ext.equals(".TXT")) || (ext.equals(".log"))
						|| (ext.equalsIgnoreCase(".cmd"));
			}

			return false;
		}

		public String getDescription() {
			if (IBMExtractUtilities.osType.equalsIgnoreCase("win")) {
				return "*.db2;*.sql;*.TXT;*.cmd";
			}
			return "*.db2;*.sql;*.log;*.sh";
		}
	}
}