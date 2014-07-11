package ibm;

import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

public class RunScript implements Runnable {
	private static final SimpleDateFormat timestampFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH.mm.ss.SSS");
	private static String osType = System.getProperty("os.name").toUpperCase()
			.startsWith("Z/OS") ? "z/OS" : System.getProperty("os.name")
			.toUpperCase().startsWith("WIN") ? "WIN" : "OTHER";
	private static String filesep = System.getProperty("file.separator");
	private static String linesep = System.getProperty("line.separator");
	private String shellCommandFileName = "";
	private String cmdPath = "";
	private String db2Instance = "";
	private ActionListener tailOutputActionListener;
	private StringBuffer buffer;
	private String windowsDrive;

	public RunScript(StringBuffer buffer,
			ActionListener tailOutputActionListener, String db2Instance,
			String cmdPath, String shellCommandFileName) {
		this.db2Instance = db2Instance;
		this.buffer = buffer;
		this.tailOutputActionListener = tailOutputActionListener;
		this.cmdPath = cmdPath;
		this.shellCommandFileName = shellCommandFileName;

		if ((osType.equalsIgnoreCase("win")) && (shellCommandFileName != null)) {
			this.windowsDrive = shellCommandFileName.trim();
			if (((this.windowsDrive.charAt(0) >= 'C') && (this.windowsDrive
					.charAt(0) <= 'Z'))
					|| ((this.windowsDrive.charAt(0) >= 'c') && (this.windowsDrive
							.charAt(0) <= 'z'))) {
				this.windowsDrive = (this.windowsDrive.charAt(0) + ":");
			}
		}
		if ((db2Instance == null) || (db2Instance.equals(""))) {
			db2Instance = "";
		} else
			db2Instance = "DB2INSTANCE=" + db2Instance;
	}

	public void run() {
		String line = null;
		Process p = null;
		BufferedReader stdInput = null;
		BufferedReader stdError = null;
		String[] cmd = null;
		try {
			File shellScriptFile = new File(this.shellCommandFileName);
			String script = shellScriptFile.getName();
			String dirName = shellScriptFile.getParent();

			if (osType.equalsIgnoreCase("win")) {
				if (!this.cmdPath.equals("")) {
					this.cmdPath = (this.cmdPath + filesep + "BIN" + filesep + "db2cmd");
				}
				String cmdForDb2 = this.windowsDrive + " && cd \"" + dirName
						+ "\" && SET DB2INSTANCE=" + this.db2Instance + " && "
						+ script;
				cmd = new String[] { this.cmdPath, "/c", "/i", "/w", cmdForDb2 };
			} else {
				if (!script.startsWith("./"))
					script = "./" + script;
				cmd = new String[] { "/bin/ksh", "-c",
						"cd \"" + dirName + "\" ; " + script };
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
		IBMExtractUtilities.ScriptExecutionCompleted = true;
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