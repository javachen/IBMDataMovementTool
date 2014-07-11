package ibm;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Container;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.filechooser.FileFilter;

public class FileViewer extends JFrame {
	private static final long serialVersionUID = -2670041685718816055L;
	JTextArea fileViewerTextArea;
	String dirName = ".";

	JButton btnOpen = new JButton("Open");
	JButton btnCancel = new JButton("Cancel");

	public FileViewer(String dirName, String fileName) {
		super("View Files");
		if ((dirName == null) || (dirName.equals("")))
			this.dirName = ".";
		else {
			this.dirName = dirName;
		}
		Container container = getContentPane();

		container.setLayout(new BorderLayout());

		Listener listener = new Listener();
		this.btnOpen.addActionListener(listener);
		this.btnCancel.addActionListener(listener);

		this.fileViewerTextArea = new JTextArea(25, 100);
		Font font = new Font("monospaced", 1, 15);
		this.fileViewerTextArea.setFont(font);
		this.fileViewerTextArea.setForeground(Color.BLUE);
		JScrollPane scrollPane = new JScrollPane(this.fileViewerTextArea);
		scrollPane.setVerticalScrollBarPolicy(20);
		scrollPane.setHorizontalScrollBarPolicy(30);

		JPanel middlePanel = new JPanel();
		middlePanel.setLayout(new FlowLayout(1, 0, 0));
		middlePanel.add(scrollPane);

		JPanel topPanel = new JPanel();

		topPanel.setLayout(new FlowLayout(2));
		topPanel.add(this.btnOpen);
		topPanel.add(this.btnCancel);

		container.add("North", topPanel);
		container.add("Center", middlePanel);

		pack();
		setVisible(true);
		setResizable(true);
		setLocationRelativeTo(null);
		if ((fileName == null) || (fileName.equals(""))) {
			openDialog();
		} else
			try {
				setTitle(fileName);
				this.fileViewerTextArea.setText(IBMExtractUtilities
						.FileContents(fileName));
				this.fileViewerTextArea.setCaretPosition(0);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
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
				this.fileViewerTextArea.setText(IBMExtractUtilities
						.FileContents(fp.getAbsolutePath()));
				this.fileViewerTextArea.setCaretPosition(0);
				this.fileViewerTextArea.setEditable(false);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
	}

	public class MyFilter extends FileFilter {
		public MyFilter() {
		}

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

	private class Listener implements ActionListener {
		private Listener() {
		}

		public void actionPerformed(ActionEvent e) {
			Object source = e.getSource();

			if (source.equals(FileViewer.this.btnOpen)) {
				FileViewer.this.openDialog();
			}

			if (source.equals(FileViewer.this.btnCancel)) {
				FileViewer.this.setVisible(false);
				FileViewer.this.dispose();
			}
		}
	}
}