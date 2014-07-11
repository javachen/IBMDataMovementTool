package ibm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

class InputStreamHandler extends Thread {
	private static String osType = System.getProperty("os.name").toUpperCase()
			.startsWith("Z/OS") ? "z/OS" : System.getProperty("os.name")
			.toUpperCase().startsWith("WIN") ? "WIN" : "OTHER";
	private static final SimpleDateFormat timestampFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH.mm.ss.SSS");
	private BufferedReader reader = null;

	public InputStreamHandler(InputStream stream) {
		this.reader = new BufferedReader(new InputStreamReader(stream));
		start();
	}

	private void log(String msg) {
		if (osType.equals("z/OS")) {
			System.out.println(timestampFormat.format(new Date()) + ":" + msg);
		} else
			System.out.println("[" + timestampFormat.format(new Date()) + "] "
					+ msg);
	}

	public void run() {
		try {
			String line;
			while ((line = this.reader.readLine()) != null) {
				if (line.equals(""))
					continue;
				log(line);
			}
		} catch (IOException e) {
		}
	}
}