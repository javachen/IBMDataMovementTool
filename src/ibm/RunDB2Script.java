package ibm;

import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

public class RunDB2Script implements Runnable {
	private static final SimpleDateFormat timestampFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH.mm.ss.SSS");
	private static String osType = System.getProperty("os.name").toUpperCase()
			.startsWith("Z/OS") ? "z/OS" : System.getProperty("os.name")
			.toUpperCase().startsWith("WIN") ? "WIN" : "OTHER";
	private static String filesep = System.getProperty("file.separator");
	private static String linesep = System.getProperty("line.separator");
	private String db2ScriptName = "";
	private String cmdPath = "";
	private ActionListener tailOutputActionListener;
	private StringBuffer buffer;

	public RunDB2Script(StringBuffer buffer,
			ActionListener tailOutputActionListener, String cmdPath,
			String db2ScriptName) {
		this.cmdPath = cmdPath;
		this.db2ScriptName = db2ScriptName;
		this.buffer = buffer;
		this.tailOutputActionListener = tailOutputActionListener;
	}

	public void run() {
		String line = null;
		Process p = null;
		BufferedReader stdInput = null;
		BufferedReader stdError = null;
		String[] cmd = null;
		try {
			File db2ScriptFile = new File(this.db2ScriptName);
			String dirName = db2ScriptFile.getParent();
			if (osType.equalsIgnoreCase("win")) {
				if (!this.cmdPath.equals("")) {
					this.cmdPath = (this.cmdPath + filesep + "BIN" + filesep + "db2cmd");
				}
				cmd = new String[] {
						this.cmdPath,
						"/c",
						"/i",
						"/w",
						"cd \"" + dirName + "\" && db2 -tvf "
								+ db2ScriptFile.getName() };
			} else {
				cmd = new String[] {
						"/bin/ksh",
						"-c",
						"cd " + dirName + " ; db2 -tvf "
								+ db2ScriptFile.getName() };
			}
			p = Runtime.getRuntime().exec(cmd);
			stdInput = new BufferedReader(new InputStreamReader(p
					.getInputStream()));
			stdError = new BufferedReader(new InputStreamReader(p
					.getErrorStream()));
			while ((line = stdInput.readLine()) != null) {
				if (!line.equals(""))
					logW(line);
			}
			while ((line = stdError.readLine()) != null) {
				if (!line.equals("")) {
					logW(line);
				}
			}
			stdInput.close();
			stdError.close();
			p.getInputStream().close();
			p.getErrorStream().close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		IBMExtractUtilities.db2ScriptCompleted = true;
	}

	private void log(String msg) {
		if (osType.equals("z/OS")) {
			System.out.println(timestampFormat.format(new Date()) + ":" + msg);
		} else
			System.out.println("[" + timestampFormat.format(new Date()) + "] "
					+ msg);
	}

	private void logW(String msg) {
		if (this.buffer == null) {
			log(msg);
		} else {
			this.buffer.append(msg + linesep);
			this.tailOutputActionListener.actionPerformed(null);
		}
	}
}