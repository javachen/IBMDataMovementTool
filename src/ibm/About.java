package ibm;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Container;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

public class About extends JFrame {
	private static final long serialVersionUID = -3117414075377274935L;
	JTextArea fileViewerTextArea;
	JButton OkBtn = new JButton("Ok");

	public About() {
		super("About");

		Container container = getContentPane();
		container.setLayout(new BorderLayout());
		Listener listener = new Listener();
		this.OkBtn.addActionListener(listener);

		this.fileViewerTextArea = new JTextArea(25, 100);
		Font font = new Font("monospaced", 1, 15);
		this.fileViewerTextArea.setFont(font);
		this.fileViewerTextArea.setForeground(Color.BLUE);
		this.fileViewerTextArea.setLineWrap(true);
		this.fileViewerTextArea.setText(IBMExtractUtilities
				.readJarFile("About.txt"));
		this.fileViewerTextArea.setCaretPosition(0);
		JScrollPane scrollPane = new JScrollPane(this.fileViewerTextArea);
		scrollPane.setVerticalScrollBarPolicy(20);
		scrollPane.setHorizontalScrollBarPolicy(31);

		JPanel middlePanel = new JPanel();
		middlePanel.setLayout(new FlowLayout(1, 0, 0));
		middlePanel.add(scrollPane);

		JPanel topPanel = new JPanel();

		topPanel.setLayout(new FlowLayout(2));
		topPanel.add(this.OkBtn);

		container.add("North", topPanel);
		container.add("Center", middlePanel);

		pack();
		setVisible(true);
		setResizable(false);
		setLocationRelativeTo(null);
	}

	private class Listener implements ActionListener {
		private Listener() {
		}

		public void actionPerformed(ActionEvent e) {
			Object source = e.getSource();
			if (source.equals(About.this.OkBtn)) {
				About.this.setVisible(false);
				About.this.dispose();
			}
		}
	}
}