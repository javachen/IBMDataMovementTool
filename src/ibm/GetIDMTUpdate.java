package ibm;

import java.awt.Dimension;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.UIManager;

public class GetIDMTUpdate extends JDialog implements ActionListener {
	private JTextArea area = new JTextArea();

	public GetIDMTUpdate(JFrame parent, String title, String message) {
		super(parent, title, true);

		this.area.setEditable(false);
		this.area.setBorder(null);
		this.area.setForeground(UIManager.getColor("Label.foreground"));
		this.area.setFont(UIManager.getFont("Label.font"));
		this.area.setText(message);
		Dimension parentSize = parent.getSize();
		Point p = parent.getLocation();
		setLocation(p.x + parentSize.width / 4, p.y + parentSize.height / 4);
		JPanel messagePane = new JPanel();
		messagePane.add(this.area);
		getContentPane().add(messagePane);
		JPanel buttonPane = new JPanel();
		JButton button = new JButton("OK");
		buttonPane.add(button);
		button.addActionListener(this);
		getContentPane().add(buttonPane, "South");
		setDefaultCloseOperation(2);
		pack();
		setVisible(true);
	}

	public void actionPerformed(ActionEvent e) {
		setVisible(false);
		dispose();
	}
}