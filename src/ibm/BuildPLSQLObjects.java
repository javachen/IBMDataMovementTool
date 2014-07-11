package ibm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

public class BuildPLSQLObjects {
	private String outputDirectory = ".";
	private String srcVendor;
	private Properties deployedObjectsList;
	private String db2Compatibility;
	private String DEPLOYED_OBJECT_FILE = "DeployedObjects.properties";

	public BuildPLSQLObjects(String db2Compatibility, String srcVendor,
			String outputDirectory) {
		this.db2Compatibility = db2Compatibility;
		this.deployedObjectsList = new Properties();
		this.srcVendor = srcVendor;
		this.outputDirectory = outputDirectory;

		String file = outputDirectory + "/savedobjects/"
				+ this.DEPLOYED_OBJECT_FILE;
		if (IBMExtractUtilities.FileExists(file)) {
			try {
				FileInputStream inputStream = new FileInputStream(file);
				this.deployedObjectsList.load(inputStream);
				inputStream.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public String getOutputDirectory() {
		return this.outputDirectory;
	}

	public void setOutputDirectory(String outputDirectory) {
		this.outputDirectory = outputDirectory;
	}

	private File[] listSavedObjects(String outputDirectory) {
		File[] fp = null;
		if (IBMExtractUtilities.FileExists(outputDirectory + "/savedobjects")) {
			File dir = new File(outputDirectory + "/savedobjects");
			FileFilter fileFilter = new FileFilter() {
				public boolean accept(File file) {
					return file.getName().endsWith(".sql");
				}
			};
			fp = dir.listFiles(fileFilter);
		}
		return fp;
	}

	private File[] listFiles(String outputDirectory) {
		File[] savedObjects = listSavedObjects(outputDirectory);

		int sizeSavedObjects = savedObjects == null ? 0 : savedObjects.length;
		int sizecompatibilityFiles = 0;

		String fileList = IBMExtractUtilities.getDeployFiles(this.srcVendor);
		if (this.db2Compatibility.equalsIgnoreCase("true")) {
			String compatibilityFileList = IBMExtractUtilities
					.getCompatibilityFiles(this.srcVendor);
			String[] compatibilityFiles = compatibilityFileList.split(",");
			sizecompatibilityFiles = compatibilityFiles.length;
		}
		String[] files = fileList.split(",");
		File[] fp = new File[files.length + sizecompatibilityFiles
				+ sizeSavedObjects];
		for (int i = 0; i < files.length; i++) {
			fp[i] = new File(outputDirectory + "/" + files[i]);
		}
		if (this.db2Compatibility.equalsIgnoreCase("true")) {
			String compatibilityFileList = IBMExtractUtilities
					.getCompatibilityFiles(this.srcVendor);
			String[] compatibilityFiles = compatibilityFileList.split(",");
			int j = files.length;
			for (int i = 0; i < sizecompatibilityFiles; i++) {
				fp[j] = new File(outputDirectory + "/" + compatibilityFiles[i]);

				j++;
			}

		}

		int j = files.length + sizecompatibilityFiles;
		for (int i = 0; i < sizeSavedObjects; i++) {
			fp[j] = savedObjects[i];

			j++;
		}

		return fp;
	}

	private boolean isTerminatorSign(String type, String line) {
		String[] typeKeys = { "TYPE", "FUNCTION", "VIEW", "MQT", "TRIGGER",
				"PROCEDURE", "PACKAGE", "PACKAGE_BODY" };

		for (int i = 0; i < typeKeys.length; i++) {
			if (typeKeys[i].equals(type)) {
				return line.equals("/");
			}
		}
		return line.equals(";");
	}

	public Hashtable<String, PLSQLInfo> getPLSQLHash() {
		String linesep = IBMExtractUtilities.linesep;
		Hashtable<String,PLSQLInfo> hash = new Hashtable<String,PLSQLInfo>();

		String key = null;
		StringBuffer buffer = new StringBuffer();
		try {
			File[] fp = listFiles(this.outputDirectory);
			for (int i = 0; i < fp.length; i++) {
				try {
					BufferedReader in = new BufferedReader(
							new FileReader(fp[i]));

					boolean collectCode = false;
					String[] keys = { "", "", "" };
					String line;
					while ((line = in.readLine()) != null) {
						if (line.startsWith("--#SET :")) {
							collectCode = true;
						}
						if (!collectCode)
							continue;
						if (line.startsWith("--#SET :")) {
							key = line.substring(line.indexOf(":") + 1);
							keys = key.split(":");
							collectCode = true;
							continue;
						}
						if (isTerminatorSign(keys[0], line)) {
							String code = this.deployedObjectsList
									.getProperty(key);
							if (code == null)
								code = "0";
							hash.put(key, new PLSQLInfo(code, keys[0], keys[1],
									keys[2], "", buffer.toString()));
							collectCode = false;
							buffer.setLength(0);
							continue;
						}
						buffer.append(line + linesep);
					}

					in.close();
				} catch (FileNotFoundException e) {
					IBMExtractUtilities.log("File " + fp[i].getName()
							+ " was not found. Skipping it");
				}
			}

		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return hash;
	}

	public Hashtable<String, String> getTreeHash() {
		Hashtable<String, String> hash = new Hashtable<String, String>();

		String[] strArray = null;
		try {
			File[] fp = listFiles(this.outputDirectory);
			for (int i = 0; i < fp.length; i++) {
				try {
					BufferedReader in = new BufferedReader(
							new FileReader(fp[i]));
					String line;
					while ((line = in.readLine()) != null) {
						if (!line.startsWith("--#SET :"))
							continue;
						strArray = line.substring(line.indexOf(":") + 1).split(
								":");
						String type = strArray[0];
						String schema = strArray[1];
						String name = strArray[2];
						String key = schema + "." + name + ":";

						if (hash.containsKey(type)) {
							String val = (String) hash.get(type);
							int ix = val.indexOf(key);
							if (ix < 0)
								val = val + key;
							hash.put(type, val);
							continue;
						}
						hash.put(type, key);
					}

					in.close();
				} catch (FileNotFoundException e) {
					IBMExtractUtilities.log("File " + fp[i].getName()
							+ " was not found for reading. Skipping it.");
				}

			}

		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return hash;
	}

	public static void main(String[] args) {
		String outputDir = "C:\\Vikram\\Prospects\\DB2Cobra\\testcase";
		BuildPLSQLObjects bp = new BuildPLSQLObjects("true", "oracle",
				outputDir);
		Hashtable<String, PLSQLInfo> hashSource = bp.getPLSQLHash();
		Set<String> set = hashSource.keySet();
		Iterator<String> itr = set.iterator();
		while (itr.hasNext()) {
			String key = (String) itr.next();
			System.out.println("Key = " + key);
			System.out.println("Source\n"
					+ ((PLSQLInfo) hashSource.get(key)).codeStatus);
		}
	}
}