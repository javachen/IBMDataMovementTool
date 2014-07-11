package ibm;

import java.awt.event.ActionListener;
import java.io.InputStream;

public class RunGenerateExtract implements Runnable {
	private static String osType = System.getProperty("os.name").toUpperCase()
			.startsWith("Z/OS") ? "z/OS" : System.getProperty("os.name")
			.toUpperCase().startsWith("WIN") ? "WIN" : "OTHER";
	private String shellCommandFileName = "";
	private String dirName = "";
	private String windowsDrive = "C:";
	private ActionListener tailOutputActionListener;
	private StringBuffer buffer;

	public RunGenerateExtract(StringBuffer buffer,
			ActionListener tailOutputActionListener, String dirName,
			String shellCommandFileName) {
		this.buffer = buffer;
		this.tailOutputActionListener = tailOutputActionListener;
		this.dirName = dirName;
		this.shellCommandFileName = shellCommandFileName;
		if ((osType.equalsIgnoreCase("win")) && (dirName != null)) {
			dirName = dirName.trim();
			if (((dirName.charAt(0) >= 'C') && (dirName.charAt(0) <= 'Z'))
					|| ((dirName.charAt(0) >= 'c') && (dirName.charAt(0) <= 'z'))) {
				this.windowsDrive = (dirName.charAt(0) + ":");
			}
		}
	}

	private void consoleRun() {
		Process p = null;
		InputStream stdInput = null;
		InputStream stdError = null;
		try {
			if (osType.equalsIgnoreCase("win")) {
				p = Runtime.getRuntime().exec(
						"cmd /c " + this.windowsDrive + " && cd \""
								+ this.dirName + "\"" + " && "
								+ this.shellCommandFileName);
			} else {
				if (!this.shellCommandFileName.startsWith("./"))
					this.shellCommandFileName = ("./" + this.shellCommandFileName);
				String[] cmd = {
						"/bin/ksh",
						"-c",
						"cd \"" + this.dirName + "\" ; "
								+ this.shellCommandFileName };

				p = Runtime.getRuntime().exec(cmd);
			}
			stdInput = p.getInputStream();
			stdError = p.getErrorStream();
			new InputStreamHandler(stdInput);
			new InputStreamHandler(stdError);
			p.waitFor();
			stdInput.close();
			stdError.close();
			p.getInputStream().close();
			p.getErrorStream().close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		IBMExtractUtilities.DataExtracted = true;
	}

	private void guiRun() {
		Process p = null;
		InputStream stdInput = null;
		InputStream stdError = null;
		try {
			if (osType.equalsIgnoreCase("win")) {
				p = Runtime.getRuntime().exec(
						"cmd /c " + this.windowsDrive + " && cd \""
								+ this.dirName + "\"" + " && "
								+ this.shellCommandFileName);
			} else {
				String[] cmd = {
						"/bin/ksh",
						"-c",
						"cd \"" + this.dirName + "\" ; "
								+ this.shellCommandFileName };
				p = Runtime.getRuntime().exec(cmd);
			}
			stdInput = p.getInputStream();
			stdError = p.getErrorStream();
			new InputGUIStreamHandler(this.buffer,
					this.tailOutputActionListener, stdInput);
			new InputGUIStreamHandler(this.buffer,
					this.tailOutputActionListener, stdError);
			p.waitFor();
			stdInput.close();
			stdError.close();
			p.getInputStream().close();
			p.getErrorStream().close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		IBMExtractUtilities.DataExtracted = true;
	}

	public void run() {
		if (this.buffer == null) {
			consoleRun();
		} else
			guiRun();
	}

	public static void main(String[] args) {
		RunGenerateExtract gen = new RunGenerateExtract(null, null,
				"D:\\IBMDataMovementTool\\FA", "unload.cmd");
		gen.run();
	}
}