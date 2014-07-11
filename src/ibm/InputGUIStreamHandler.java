package ibm;

import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class InputGUIStreamHandler extends Thread {
	private static String linesep = System.getProperty("line.separator");
	private ActionListener tailOutputActionListener;
	private StringBuffer buffer;
	private BufferedReader reader = null;

	public InputGUIStreamHandler(StringBuffer buffer,
			ActionListener tailOutputActionListener, InputStream stream) {
		this.tailOutputActionListener = tailOutputActionListener;
		this.buffer = buffer;
		this.reader = new BufferedReader(new InputStreamReader(stream));
		start();
	}

	public void run() {
		try {
			String line;
			while ((line = this.reader.readLine()) != null) {
				if (line.equals(""))
					continue;
				this.buffer.append(line + linesep);
				this.tailOutputActionListener.actionPerformed(null);
			}
		} catch (IOException e) {
		}
	}
}