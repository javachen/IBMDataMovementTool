package ibm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class IBMExtractConfig {
	private static String osType = IBMExtractUtilities.osType;
	private static String sep = osType.equalsIgnoreCase("Win") ? ";" : ":";
	private static String filesep = System.getProperty("file.separator");
	private static final SimpleDateFormat timestampFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH.mm.ss.SSS");
	private static String linesep = System.getProperty("line.separator");
	private String javaHome = System.getProperty("java.home");
	private String currHome = System.getProperty("user.dir");
	static String INPUT_DIR = null;

	private String srcJDBC = "";
	private String dstJDBC = "";
	private String appJAR = "";
	private String srcVendor = "oracle";
	private String dstVendor = "db2";
	private String srcJDBCHome = "";
	private String dstJDBCHome = "";
	private String srcDBName = "";
	private String dstDBName = "";
	private String srcDB2Home = "";
	private String dstDB2Home = "";
	private String srcDB2Instance = "";
	private String dstDB2Instance = "";
	private String srcDB2Release = "";
	private String dstDB2Release = "";
	private String srcServer = "localhost";
	private String dstServer = "localhost";
	private String srcUid = "";
	private String dstUid = "";
	private String srcPwd = "";
	private String dstPwd = "";
	private int srcPort = 0;
	private int dstPort = 0;
	private int numThreads = 5;
	private String srcSchName = "";
	private String dstSchName = "";
	private String extractDDL = "true";
	private String extractData = "true";
	private String rowcount;
	private String regenerateTriggers = "false";
	private String dbclob = "false";
	private String graphic = "false";
	private String trimTrailingSpaces = "false";
	private String debug = "false";
	private String remoteLoad = "false";
	private String compressTable = "false";
	private String compressIndex = "false";
	private String encoding = "UTF-8";
	private String customMapping = "false";
	private String extractPartitions = "true";
	private String extractHashPartitions = "false";
	private String oracleNumberMapping = "false";
	private String oracleNumb31Mapping = "false";
	private String db2Compatibility = null;
	private String retainConstraintsName = "false";
	private String useBestPracticeTSNames = "true";
	public String Message = "";
	public String limitExtractRows = "ALL";
	public String limitLoadRows = "ALL";
	public String geninput;
	public String genddl;
	public String unload;
	public String meet;
	public String zdb2tableseries = "Q";
	public String znocopypend = "true";
	public String zoveralloc = "1.3636";
	public String zsecondary = "0";
	public String storclas = "none";

	private String PARAM_PROP_FILE = "IBMExtract.properties";
	private String JDBC_PROP_FILE = "jdbcdriver.properties";
	private BufferedWriter genInputWriter;
	private BufferedWriter unloadWriter;
	private BufferedWriter rowCountWriter;
	private BufferedWriter genMeetWriter;
	private Properties propParams;
	private Properties propJDBC;
	private String outputDirectory = System.getProperty("user.dir") + filesep
			+ "migr";
	private String jarFiles;
	private String jarFiles2;
	private String fetchSize = "100";
	private boolean found = false;
	public boolean paramPropFound = true;
	private BufferedReader stdin = new BufferedReader(new InputStreamReader(
			System.in));

	public String getOracleNumberMapping() {
		return this.oracleNumberMapping;
	}

	public String getOracleNumb31Mapping() {
		return this.oracleNumb31Mapping;
	}

	public void setOracleNumberMapping(String oracleNumberMapping) {
		oracleNumberMapping = oracleNumberMapping;
	}

	public void setOracleNumb31Mapping(String oracleNumb31Mapping) {
		oracleNumb31Mapping = oracleNumb31Mapping;
	}

	public String getNumThreads() {
		return "" + this.numThreads;
	}

	public void setNumThreads(String numThreads) {
		try {
			this.numThreads = Integer.valueOf(numThreads).intValue();
		} catch (Exception e) {
			this.numThreads = 5;
		}
	}

	public String getEncoding() {
		return this.encoding;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public String getZdb2tableseries() {
		return this.zdb2tableseries;
	}

	public String getZnocopypend() {
		return this.znocopypend;
	}

	public String getZoveralloc() {
		return this.zoveralloc;
	}

	public String getZsecondary() {
		return this.zsecondary;
	}

	public void setZdb2tableseries(String zdb2tableseries) {
		this.zdb2tableseries = zdb2tableseries;
	}

	public void setZnocopypend(String znocopypend) {
		this.znocopypend = znocopypend;
	}

	public void setZoveralloc(String zoveralloc) {
		this.zoveralloc = zoveralloc;
	}

	public void setZsecondary(String zsecondary) {
		this.zsecondary = zsecondary;
	}

	public String getLimitExtractRows() {
		return this.limitExtractRows;
	}

	public String getLimitLoadRows() {
		return this.limitLoadRows;
	}

	public void setLimitExtractRows(String limitExtractRows) {
		this.limitExtractRows = limitExtractRows;
	}

	public void setLimitLoadRows(String limitLoadRows) {
		this.limitLoadRows = limitLoadRows;
	}

	public String getUseBestPracticeTSNames() {
		return this.useBestPracticeTSNames;
	}

	public void setUseBestPracticeTSNames(String useBestPracticeTSNames) {
		this.useBestPracticeTSNames = useBestPracticeTSNames;
	}

	public String getRetainConstraintsName() {
		return this.retainConstraintsName;
	}

	public void setRetainConstraintsName(String retainConstraintsName) {
		this.retainConstraintsName = retainConstraintsName;
	}

	public String getExtractPartitions() {
		return this.extractPartitions;
	}

	public String getExtractHashPartitions() {
		return this.extractHashPartitions;
	}

	public void setExtractHashPartitions(String extractHashPartitions) {
		this.extractHashPartitions = extractHashPartitions;
	}

	public void setExtractPartitions(String extractPartitions) {
		this.extractPartitions = extractPartitions;
	}

	public String getSrcDB2Release() {
		return this.srcDB2Release;
	}

	public String getDstDB2Release() {
		return this.dstDB2Release;
	}

	public void setSrcDB2Release(String srcDB2Release) {
		this.srcDB2Release = srcDB2Release;
	}

	public void setDstDB2Release(String dstDB2Release) {
		this.dstDB2Release = dstDB2Release;
	}

	public IBMExtractConfig() {
		System.setProperty("OUTPUT_DIR", this.outputDirectory);
		this.dstVendor = (osType.equalsIgnoreCase("z/OS") ? "zdb2" : "db2");
		INPUT_DIR = System.getProperty("INPUT_DIR");
		if (INPUT_DIR == null)
			INPUT_DIR = ".";
		if (!INPUT_DIR.equalsIgnoreCase(".")) {
			if ((!INPUT_DIR.endsWith("\\")) && (!INPUT_DIR.endsWith("/"))) {
				INPUT_DIR += "/";
			}
		}
		log("INPUT Directory = " + INPUT_DIR);
		new File(INPUT_DIR).mkdirs();
		if (osType.equalsIgnoreCase("Win")) {
			this.geninput = "geninput.cmd";
			this.genddl = "genddl.cmd";
			this.unload = "unload.cmd";
			this.rowcount = "rowcount.cmd";
			this.meet = "GenInputForMEET.cmd";
		} else {
			this.geninput = "geninput";
			this.genddl = "genddl";
			this.unload = "unload";
			this.rowcount = "rowcount";
			this.meet = "GenInputForMEET";
		}
	}

	public String getDbclob() {
		return this.dbclob;
	}

	public String getGraphic() {
		return this.graphic;
	}

	public String getTrimTrailingSpaces() {
		return this.trimTrailingSpaces;
	}

	public void setDbclob(String dbclob) {
		this.dbclob = dbclob;
	}

	public void setGraphic(String graphic) {
		this.graphic = graphic;
	}

	public void setTrimTrailingSpaces(String trimTrailingSpaces) {
		this.trimTrailingSpaces = trimTrailingSpaces;
	}

	public String getRegenerateTriggers() {
		return this.regenerateTriggers;
	}

	public void setRegenerateTriggers(String regenerateTriggers) {
		this.regenerateTriggers = regenerateTriggers;
	}

	public String getSrcDB2Instance() {
		return this.srcDB2Instance;
	}

	public String getDstDB2Instance() {
		return this.dstDB2Instance;
	}

	public String getSrcDB2Home() {
		return this.srcDB2Home;
	}

	public String getDstDB2Home() {
		return this.dstDB2Home;
	}

	public String getExtractDDL() {
		return this.extractDDL;
	}

	public String getFetchSize() {
		return this.fetchSize;
	}

	public void setExtractDDL(String extractDDL) {
		this.extractDDL = extractDDL;
	}

	public String getExtractData() {
		return this.extractData;
	}

	public void setExtractData(String extractData) {
		this.extractData = extractData;
	}

	public String getSrcJDBC() {
		return this.srcJDBC;
	}

	public String getDstJDBC() {
		return this.dstJDBC;
	}

	public String getSrcVendor() {
		return this.srcVendor;
	}

	public String getDstVendor() {
		return this.dstVendor;
	}

	public String getSrcDBName() {
		return this.srcDBName;
	}

	public String getDstDBName() {
		return this.dstDBName;
	}

	public String getSrcServer() {
		return this.srcServer;
	}

	public String getDstServer() {
		return this.dstServer;
	}

	public String getSrcUid() {
		return this.srcUid;
	}

	public String getDstUid() {
		return this.dstUid;
	}

	public String getSrcPwd() {
		return this.srcPwd;
	}

	public String getDstPwd() {
		return this.dstPwd;
	}

	public int getSrcPort() {
		return this.srcPort;
	}

	public int getDstPort() {
		return this.dstPort;
	}

	public String getSrcSchName() {
		return this.srcSchName;
	}

	public String getDstSchName() {
		return this.dstSchName;
	}

	public String getJavaHome() {
		return this.javaHome;
	}

	public String getSrcJDBCHome() {
		return this.srcJDBCHome;
	}

	public String getDstJDBCHome() {
		return this.dstJDBCHome;
	}

	public String getRemoteLoad() {
		return this.remoteLoad;
	}

	public String getCompressTable() {
		return this.compressTable;
	}

	public String getCompressIndex() {
		return this.compressIndex;
	}

	public void setRemoteLoad(String remoteLoad) {
		this.remoteLoad = remoteLoad;
	}

	public void setCompressTable(String compressTable) {
		this.compressTable = compressTable;
	}

	public void setCompressIndex(String compressIndex) {
		this.compressIndex = compressIndex;
	}

	public String getCurrHome() {
		return this.currHome;
	}

	public String getAppJAR() {
		return this.appJAR;
	}

	public String getOutputDirectory() {
		return this.outputDirectory;
	}

	public String getDB2Compatibility() {
		if (this.db2Compatibility == null) {
			this.db2Compatibility = "false";
		}
		return this.db2Compatibility;
	}

	public String getpFile() {
		return "-DIBMExtractPropFile=" + getIBMPropFile();
	}

	public String getIBMPropFile() {
		return IBMExtractUtilities.putQuote(this.currHome + filesep
				+ this.PARAM_PROP_FILE, sep);
	}

	public boolean isDataExtracted() {
		String scriptName = "";
		boolean bool = false;
		if ((this.outputDirectory == null) || (this.outputDirectory.equals(""))) {
			bool = false;
		} else {
			scriptName = this.outputDirectory
					+ System.getProperty("file.separator")
					+ getDB2RutimeShellScriptName();

			if (IBMExtractUtilities.FileExists(scriptName))
				bool = true;
		}
		return bool;
	}

	public String getDB2RutimeShellScriptName() {
		String scriptName = "";
		String ext = osType.equalsIgnoreCase("win") ? ".cmd" : ".sh";
		if ((this.extractDDL.equals("true"))
				&& (this.extractData.equals("true")))
			scriptName = "db2gen" + ext;
		else if ((!this.extractDDL.equals("true"))
				&& (this.extractData.equals("true")))
			scriptName = "db2load" + ext;
		else if ((this.extractDDL.equals("true"))
				&& (!this.extractData.equals("true")))
			scriptName = "db2ddl" + ext;
		return scriptName;
	}

	public String getDB2DropObjectsScriptName() {
		String scriptName = "";
		String ext = osType.equalsIgnoreCase("win") ? ".cmd" : ".sh";
		scriptName = "db2dropobjects" + ext;
		return scriptName;
	}

	public String getMeetScriptName() {
		return this.meet;
	}

	public void setFetchSize(String fetchSize) {
		this.fetchSize = fetchSize;
	}

	public void setOutputDirectory(String outputDirectory) {
		this.outputDirectory = outputDirectory;
	}

	public void setDB2Compatibility(String db2Compatibility) {
		this.db2Compatibility = db2Compatibility;
	}

	public void setSrcJDBC(String srcJDBC) {
		this.srcJDBC = srcJDBC;
	}

	public void setDstJDBC(String dstJDBC) {
		this.dstJDBC = dstJDBC;
	}

	public void setSrcVendor(String srcVendor) {
		this.srcVendor = srcVendor;
	}

	public void setDstVendor(String dstVendor) {
		this.dstVendor = dstVendor;
	}

	public void setSrcDBName(String srcDBName) {
		this.srcDBName = srcDBName;
	}

	public void setDstDBName(String dstDBName) {
		this.dstDBName = dstDBName;
	}

	public void setSrcServer(String srcServer) {
		this.srcServer = srcServer;
	}

	public void setDstServer(String dstServer) {
		this.dstServer = dstServer;
	}

	public void setSrcUid(String srcUid) {
		this.srcUid = srcUid;
	}

	public void setDstUid(String dstUid) {
		this.dstUid = dstUid;
	}

	public void setSrcPwd(String srcPwd) {
		this.srcPwd = srcPwd;
	}

	public void setDstPwd(String dstPwd) {
		this.dstPwd = dstPwd;
	}

	public void setSrcPort(String srcPort) {
		this.srcPort = Integer.parseInt(srcPort);
	}

	public void setDstPort(String dstPort) {
		try {
			this.dstPort = Integer.parseInt(dstPort);
		} catch (Exception e) {
		}
	}

	public void setSrcSchName(String srcSchName) {
		this.srcSchName = srcSchName;
	}

	public void setDstSchName(String dstSchName) {
		this.dstSchName = dstSchName;
	}

	public void setJavaHome(String javaHome) {
		this.javaHome = javaHome;
	}

	public void setSrcJDBCHome(String srcJDBCHome) {
		this.srcJDBCHome = srcJDBCHome;
	}

	public void setDstJDBCHome(String dstJDBCHome) {
		this.dstJDBCHome = dstJDBCHome;
	}

	public void setCurrHome(String currHome) {
		this.currHome = currHome;
	}

	public void setAppJAR(String appJAR) {
		this.appJAR = appJAR;
	}

	public void setSrcDB2Home(String srcDB2Home) {
		this.srcDB2Home = srcDB2Home;
	}

	public void setDstDB2Home(String dstDB2Home) {
		this.dstDB2Home = dstDB2Home;
	}

	public void setSrcDB2Instance(String srcDB2Instance) {
		this.srcDB2Instance = srcDB2Instance;
	}

	public void setDstDB2Instance(String dstDB2Instance) {
		this.dstDB2Instance = dstDB2Instance;
	}

	private String FormatENVString(String inputStr) {
		return inputStr.contains(" ") ? "\"" + inputStr + "\"" : inputStr;
	}

	public void writeMeetScript() throws IOException {
		new File(this.outputDirectory).mkdirs();
		String tmpFileName = this.outputDirectory + filesep + this.meet;
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);
		if (osType.equals("WIN")) {
			buffer.append(":: Copyright(r) IBM Corporation" + linesep);
			buffer.append("::" + linesep);
			buffer.append(":: Vikram Khatri (vikram.khatri@us.ibm.com)"
					+ linesep);
			buffer.append("::" + linesep + linesep);
			buffer
					.append(":: You can run this script to generate an input file for MEET."
							+ linesep);
			buffer.append("::" + linesep + linesep);

			buffer.append("@echo off" + linesep);
			buffer.append("cls" + linesep + linesep);

			buffer
					.append("ECHO Executed by: %USERNAME% Machine: %COMPUTERNAME% On %OS% %DATE% %TIME%"
							+ linesep);
			buffer.append("ECHO." + linesep);
			buffer
					.append("ECHO -------------------------------------------------------------------"
							+ linesep);
			buffer
					.append("ECHO Generate MEET output for analysis to be done by IBM Representative"
							+ linesep);
			buffer
					.append("ECHO -------------------------------------------------------------------"
							+ linesep);
			buffer.append("ECHO." + linesep + linesep);

			buffer.append("SET JAVA_HOME=" + this.javaHome + linesep);
			buffer.append("SET CLASSPATH="
					+ IBMExtractUtilities.putQuote(this.appJAR, sep) + ";"
					+ IBMExtractUtilities.putQuote(this.srcJDBC, sep) + linesep
					+ linesep);

			buffer.append("SET SRCSCHEMA=" + FormatENVString(this.dstSchName)
					+ linesep);
			buffer.append("SET SERVER=" + FormatENVString(this.srcServer)
					+ linesep);
			buffer.append("SET DATABASE=" + FormatENVString(this.srcDBName)
					+ linesep);
			buffer.append("SET PORT=" + this.srcPort + linesep);
			buffer
					.append("SET DBUID=" + FormatENVString(this.srcUid)
							+ linesep);
			buffer
					.append("REM Password was encrypted. You can replace this with clear text password, if required."
							+ linesep);
			buffer.append("SET DBPWD="
					+ IBMExtractUtilities.Encrypt(this.srcPwd) + linesep
					+ linesep);

			buffer
					.append("\"%JAVA_HOME%\\bin\\java\" -DOUTPUT_DIR=. -cp %CLASSPATH% ibm.GenerateMeet %SERVER% %DATABASE% %PORT% %DBUID% %DBPWD% %SRCSCHEMA%"
							+ linesep);
			this.genMeetWriter = new BufferedWriter(new FileWriter(tmpFileName,
					false));
		} else {
			if (this.dstVendor.equals("zdb2"))
				buffer.append("#!/bin/sh" + linesep + linesep);
			else
				buffer.append("#!/bin/ksh" + linesep + linesep);
			buffer.append("# Copyright(r) IBM Corporation" + linesep);
			buffer.append("#" + linesep);
			buffer.append("# Vikram Khatri (vikram.khatri@us.ibm.com)"
					+ linesep);
			buffer.append("#" + linesep + linesep);
			buffer
					.append("# You can run this script to generate an input file for MEET."
							+ linesep);
			buffer.append("#" + linesep + linesep);

			buffer
					.append("echo -------------------------------------------------------------------"
							+ linesep);
			buffer
					.append("echo Generate MEET output for analysis to be done by IBM Representative"
							+ linesep);
			buffer
					.append("echo -------------------------------------------------------------------"
							+ linesep);

			buffer.append("JAVA_HOME=" + this.javaHome + linesep);
			buffer.append("CLASSPATH="
					+ IBMExtractUtilities.putQuote(this.appJAR, sep) + ":"
					+ IBMExtractUtilities.putQuote(this.srcJDBC, sep) + linesep
					+ linesep);

			buffer.append("SRCSCHEMA=" + FormatENVString(this.srcSchName)
					+ linesep);
			buffer
					.append("SERVER=" + FormatENVString(this.srcServer)
							+ linesep);
			buffer.append("DATABASE=" + FormatENVString(this.srcDBName)
					+ linesep);
			buffer.append("PORT=" + this.srcPort + linesep);
			buffer.append("DBUID=" + FormatENVString(this.srcUid) + linesep);
			buffer
					.append("### Password was encrypted. You can replace this with clear text password, if required."
							+ linesep);
			buffer.append("DBPWD=" + IBMExtractUtilities.Encrypt(this.srcPwd)
					+ linesep + linesep);

			buffer
					.append("$JAVA_HOME/bin/java -DINPUT_DIR=. -cp $CLASSPATH ibm.GenerateMeet $SERVER $DATABASE $PORT $DBUID $DBPWD $SRCSCHEMA"
							+ linesep);
			Runtime rt = Runtime.getRuntime();
			this.genMeetWriter = new BufferedWriter(new FileWriter(tmpFileName,
					false));
			rt.exec("chmod 755 " + tmpFileName);
		}
		this.genMeetWriter.write(buffer.toString());
		this.genMeetWriter.close();
		log("Command File " + tmpFileName + " created.");
	}

	public void writeGeninput() throws IOException {
		new File(this.outputDirectory).mkdirs();
		String tmpFileName = this.outputDirectory + filesep + this.geninput;
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);
		if (osType.equals("WIN")) {
			buffer.append(":: Copyright(r) IBM Corporation" + linesep);
			buffer.append("::" + linesep);
			buffer.append(":: Vikram Khatri (vikram.khatri@us.ibm.com)"
					+ linesep);
			buffer.append("::" + linesep + linesep);
			buffer
					.append(":: You can run this script to overwrite tables script, which is an input to the unload."
							+ linesep);
			buffer
					.append(":: Normally, you will not require to run this script since tables file is already created."
							+ linesep);
			buffer.append("::" + linesep + linesep);

			buffer.append("@echo off" + linesep);
			buffer.append("cls" + linesep + linesep);

			buffer
					.append("ECHO Executed by: %USERNAME% Machine: %COMPUTERNAME% On %OS% %DATE% %TIME%"
							+ linesep);
			buffer.append("ECHO." + linesep);
			buffer
					.append("ECHO -------------------------------------------------------------------"
							+ linesep);
			buffer.append("ECHO Migration from " + this.srcVendor + " to "
					+ this.dstVendor + linesep);
			buffer
					.append("ECHO -------------------------------------------------------------------"
							+ linesep);
			buffer.append("ECHO." + linesep + linesep);

			buffer.append("SET JAVA_HOME=" + this.javaHome + linesep);
			buffer.append("SET CLASSPATH="
					+ IBMExtractUtilities.putQuote(this.appJAR, sep) + ";"
					+ IBMExtractUtilities.putQuote(this.srcJDBC, sep) + linesep
					+ linesep);

			buffer.append("SET DBVENDOR=" + FormatENVString(this.srcVendor)
					+ linesep);
			buffer.append("SET SRCSCHEMA=" + FormatENVString(this.dstSchName)
					+ linesep);
			buffer.append("SET DSTSCHEMA=" + FormatENVString(this.dstSchName)
					+ linesep);
			buffer.append("SET SERVER=" + FormatENVString(this.srcServer)
					+ linesep);
			buffer.append("SET DATABASE=" + FormatENVString(this.srcDBName)
					+ linesep);
			buffer.append("SET PORT=" + this.srcPort + linesep);
			buffer
					.append("SET DBUID=" + FormatENVString(this.srcUid)
							+ linesep);
			buffer
					.append("REM Password was encrypted. You can replace this with clear text password, if required."
							+ linesep);
			buffer.append("SET DBPWD="
					+ IBMExtractUtilities.Encrypt(this.srcPwd) + linesep
					+ linesep);

			buffer
					.append("\"%JAVA_HOME%\\bin\\java\" -DINPUT_DIR=. "
							+ getpFile()
							+ " -cp %CLASSPATH% ibm.GenInput %DBVENDOR% %DSTSCHEMA% %SRCSCHEMA% %SERVER% %DATABASE% %PORT% %DBUID% %DBPWD%"
							+ linesep);
			this.genInputWriter = new BufferedWriter(new FileWriter(
					tmpFileName, false));
		} else {
			if (this.dstVendor.equals("zdb2"))
				buffer.append("#!/bin/sh" + linesep + linesep);
			else
				buffer.append("#!/bin/ksh" + linesep + linesep);
			buffer.append("# Copyright(r) IBM Corporation" + linesep);
			buffer.append("#" + linesep);
			buffer.append("# Vikram Khatri (vikram.khatri@us.ibm.com)"
					+ linesep);
			buffer.append("#" + linesep + linesep);
			buffer
					.append("# You can run this script to overwrite tables script, which is an input to the unload."
							+ linesep);
			buffer
					.append("# Normally, you will not require to run this script since tables file is already created."
							+ linesep);
			buffer.append("#" + linesep + linesep);

			buffer
					.append("echo -------------------------------------------------------------------"
							+ linesep);
			buffer.append("echo Migration from " + this.srcVendor + " to "
					+ this.dstVendor + linesep);
			buffer
					.append("echo -------------------------------------------------------------------"
							+ linesep);

			buffer.append("JAVA_HOME=" + this.javaHome + linesep);
			buffer.append("CLASSPATH="
					+ IBMExtractUtilities.putQuote(this.appJAR, sep) + ":"
					+ IBMExtractUtilities.putQuote(this.srcJDBC, sep) + linesep
					+ linesep);

			buffer.append("DBVENDOR=" + FormatENVString(this.srcVendor)
					+ linesep);
			buffer.append("SRCSCHEMA=" + FormatENVString(this.srcSchName)
					+ linesep);
			buffer.append("DSTSCHEMA=" + FormatENVString(this.srcSchName)
					+ linesep);
			buffer
					.append("SERVER=" + FormatENVString(this.srcServer)
							+ linesep);
			buffer.append("DATABASE=" + FormatENVString(this.srcDBName)
					+ linesep);
			buffer.append("PORT=" + this.srcPort + linesep);
			buffer.append("DBUID=" + FormatENVString(this.srcUid) + linesep);
			buffer
					.append("### Password was encrypted. You can replace this with clear text password, if required."
							+ linesep);
			buffer.append("DBPWD=" + IBMExtractUtilities.Encrypt(this.srcPwd)
					+ linesep + linesep);

			buffer
					.append("$JAVA_HOME/bin/java -DINPUT_DIR=. "
							+ getpFile()
							+ " -cp $CLASSPATH ibm.GenInput $DBVENDOR $DSTSCHEMA $SRCSCHEMA $SERVER $DATABASE $PORT $DBUID $DBPWD"
							+ linesep);
			Runtime rt = Runtime.getRuntime();
			this.genInputWriter = new BufferedWriter(new FileWriter(
					tmpFileName, false));
			rt.exec("chmod 755 " + tmpFileName);
		}
		this.genInputWriter.write(buffer.toString());
		this.genInputWriter.close();
		log("Command File " + tmpFileName + " created.");
	}

	public void writeUnload(String fileName) throws IOException {
		new File(this.outputDirectory).mkdirs();
		String tmpFileName = this.outputDirectory + filesep + fileName;
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);
		if (this.srcVendor.equals("mysql"))
			this.fetchSize = "0";
		if (osType.equals("WIN")) {
			buffer.append(":: Copyright(r) IBM Corporation" + linesep);
			buffer.append("::" + linesep);
			buffer.append(":: Vikram Khatri (vikram.khatri@us.ibm.com)"
					+ linesep);
			buffer.append("::" + linesep + linesep);
			buffer
					.append(":: This script is the heart of the IBM Data Movement Tool."
							+ linesep);
			buffer.append(":: This script can be run from GUI or command line."
					+ linesep);
			buffer.append("::" + linesep + linesep);

			buffer.append("@echo off" + linesep);
			buffer.append("cls" + linesep + linesep);

			buffer
					.append("ECHO Executed by: %USERNAME% Machine: %COMPUTERNAME% On %OS% %DATE% %TIME%"
							+ linesep);
			buffer.append("ECHO." + linesep);
			buffer
					.append("ECHO -------------------------------------------------------------------"
							+ linesep);
			buffer.append("ECHO Migration from " + this.srcVendor + " to "
					+ this.dstVendor + linesep);
			buffer
					.append("ECHO -------------------------------------------------------------------"
							+ linesep);
			buffer.append("ECHO." + linesep + linesep);

			buffer.append("SET JAVA_HOME=" + this.javaHome + linesep);
			buffer.append("SET CLASSPATH="
					+ IBMExtractUtilities.putQuote(this.appJAR, sep) + ";"
					+ IBMExtractUtilities.putQuote(this.srcJDBC, sep) + linesep
					+ linesep);

			buffer.append("SET TABLES=" + FormatENVString(this.srcDBName)
					+ ".tables" + linesep);
			buffer.append("SET COLSEP=~" + linesep);
			buffer.append("SET DBVENDOR=" + FormatENVString(this.srcVendor)
					+ linesep);
			if (this.extractData.equalsIgnoreCase("true"))
				buffer.append("SET NUM_THREADS=" + this.numThreads + linesep);
			else
				buffer.append("SET NUM_THREADS=1" + linesep);
			buffer.append("SET SERVER=" + FormatENVString(this.srcServer)
					+ linesep);
			buffer.append("SET DATABASE=" + FormatENVString(this.srcDBName)
					+ linesep);
			buffer.append("SET PORT=" + this.srcPort + linesep);
			buffer
					.append("SET DBUID=" + FormatENVString(this.srcUid)
							+ linesep);
			buffer
					.append("REM Password was encrypted. You can replace this with clear text password, if required."
							+ linesep);
			buffer.append("SET DBPWD="
					+ IBMExtractUtilities.Encrypt(this.srcPwd) + linesep);
			buffer.append("SET GENDDL=" + this.extractDDL + linesep);
			buffer.append("SET UNLOAD=" + this.extractData + linesep);
			buffer.append("SET FETCHSIZE=" + this.fetchSize + linesep);
			buffer.append("SET LOADREPLACE=true" + linesep);

			buffer
					.append("\"%JAVA_HOME%\\bin\\java\" -Xmx990m -DOUTPUT_DIR=. "
							+ getpFile()
							+ " -cp %CLASSPATH% ibm.GenerateExtract %TABLES% %COLSEP% %DBVENDOR% %NUM_THREADS% %SERVER% %DATABASE% %PORT% %DBUID% %DBPWD% %GENDDL% %UNLOAD% %FETCHSIZE% %LOADREPLACE%"
							+ linesep);
			this.unloadWriter = new BufferedWriter(new FileWriter(tmpFileName,
					false));
		} else {
			if (this.dstVendor.equals("zdb2"))
				buffer.append("#!/bin/sh" + linesep);
			else
				buffer.append("#!/bin/ksh" + linesep);
			buffer.append("# Copyright(r) IBM Corporation" + linesep);
			buffer.append("#" + linesep);
			buffer.append("# Vikram Khatri (vikram.khatri@us.ibm.com)"
					+ linesep);
			buffer.append("#" + linesep);
			buffer
					.append("# This script is the heart of the IBM Data Movement Tool."
							+ linesep);
			buffer.append("# This script can be run from GUI or command line."
					+ linesep);
			buffer.append("#" + linesep + linesep);

			buffer
					.append("echo -------------------------------------------------------------------"
							+ linesep);
			buffer.append("echo Migration from " + this.srcVendor + " to "
					+ this.dstVendor + linesep);
			buffer
					.append("echo -------------------------------------------------------------------"
							+ linesep);

			buffer.append("JAVA_HOME=" + this.javaHome + linesep);
			if (this.dstVendor.equals("db2"))
				buffer.append("CLASSPATH="
						+ IBMExtractUtilities.putQuote(this.appJAR, sep) + ":"
						+ IBMExtractUtilities.putQuote(this.srcJDBC, sep)
						+ linesep + linesep);
			else {
				buffer.append("CLASSPATH="
						+ IBMExtractUtilities.putQuote(this.appJAR, sep)
						+ ":$JZOS_HOME/ibmjzos.jar:"
						+ IBMExtractUtilities.putQuote(this.srcJDBC, sep)
						+ linesep + linesep);
			}
			buffer.append("TABLES=" + this.srcDBName + ".tables" + linesep);
			buffer.append("COLSEP=\\~" + linesep);
			buffer.append("DBVENDOR=" + FormatENVString(this.srcVendor)
					+ linesep);
			if (this.extractData.equalsIgnoreCase("true"))
				buffer.append("NUM_THREADS=" + this.numThreads + linesep);
			else
				buffer.append("NUM_THREADS=1" + linesep);
			buffer
					.append("SERVER=" + FormatENVString(this.srcServer)
							+ linesep);
			buffer.append("DATABASE=" + FormatENVString(this.srcDBName)
					+ linesep);
			buffer.append("PORT=" + this.srcPort + linesep);
			buffer.append("DBUID=" + FormatENVString(this.srcUid) + linesep);
			buffer
					.append("### Password was encrypted. You can replace this with clear text password, if required."
							+ linesep);
			buffer.append("DBPWD=" + IBMExtractUtilities.Encrypt(this.srcPwd)
					+ linesep);
			buffer.append("GENDDL=" + this.extractDDL + linesep);
			buffer.append("UNLOAD=" + this.extractData + linesep);
			buffer.append("FETCHSIZE=" + this.fetchSize + linesep);
			buffer.append("LOADREPLACE=true" + linesep);

			buffer
					.append("$JAVA_HOME/bin/java -Xmx990m -DOUTPUT_DIR=. "
							+ getpFile()
							+ " -cp $CLASSPATH ibm.GenerateExtract $TABLES $COLSEP $DBVENDOR $NUM_THREADS $SERVER $DATABASE $PORT $DBUID $DBPWD $GENDDL $UNLOAD $FETCHSIZE $LOADREPLACE"
							+ linesep);
			Runtime rt = Runtime.getRuntime();
			this.unloadWriter = new BufferedWriter(new FileWriter(tmpFileName,
					false));
			rt.exec("chmod 755 " + tmpFileName);
		}
		this.unloadWriter.write(buffer.toString());
		this.unloadWriter.close();
		log("Command file " + tmpFileName + " created.");
	}

	public void writeRowCount() throws IOException {
		new File(this.outputDirectory).mkdirs();
		String tmpFileName = this.outputDirectory + filesep + this.rowcount;
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);
		if (osType.equals("WIN")) {
			buffer.append(":: Copyright(r) IBM Corporation" + linesep);
			buffer.append("::" + linesep);
			buffer.append(":: Vikram Khatri (vikram.khatri@us.ibm.com)"
					+ linesep);
			buffer.append("::" + linesep + linesep);
			buffer
					.append(":: Run this script to count rows from source and target DB server to compare."
							+ linesep);
			buffer.append("::" + linesep + linesep);

			buffer.append("@echo off" + linesep);
			buffer.append("cls" + linesep + linesep);

			buffer
					.append("ECHO Executed by: %USERNAME% Machine: %COMPUTERNAME% On %OS% %DATE% %TIME%"
							+ linesep);
			buffer.append("ECHO." + linesep);
			buffer
					.append("ECHO -------------------------------------------------------------------"
							+ linesep);
			buffer.append("ECHO Migration from " + this.srcVendor + " to "
					+ this.dstVendor + linesep);
			buffer
					.append("ECHO -------------------------------------------------------------------"
							+ linesep);
			buffer.append("ECHO." + linesep + linesep);

			buffer.append("SET JAVA_HOME=" + this.javaHome + linesep);
			if (this.srcJDBC.equals(this.dstJDBC))
				buffer.append("SET CLASSPATH="
						+ IBMExtractUtilities.putQuote(this.appJAR, sep) + ";"
						+ IBMExtractUtilities.putQuote(this.srcJDBC, sep)
						+ linesep + linesep);
			else {
				buffer.append("SET CLASSPATH="
						+ IBMExtractUtilities.putQuote(this.appJAR, sep) + ";"
						+ IBMExtractUtilities.putQuote(this.srcJDBC, sep) + ";"
						+ IBMExtractUtilities.putQuote(this.dstJDBC, sep)
						+ linesep + linesep);
			}
			buffer.append("SET TABLES=" + this.srcDBName + ".tables" + linesep);
			buffer.append("SET SRC_VENDOR=" + this.srcVendor + linesep);
			buffer.append("SET SRC_SERVER=" + FormatENVString(this.srcServer)
					+ linesep);
			buffer.append("SET SRC_DATABASE=" + FormatENVString(this.srcDBName)
					+ linesep);
			buffer.append("SET SRC_PORT=" + this.srcPort + linesep);
			buffer.append("SET SRC_UID=" + FormatENVString(this.srcUid)
					+ linesep);
			buffer
					.append("REM Password was encrypted. You can replace this with clear text password, if required."
							+ linesep);
			buffer.append("SET SRC_PWD="
					+ IBMExtractUtilities.Encrypt(this.srcPwd) + linesep
					+ linesep);
			buffer.append("SET DST_VENDOR=" + this.dstVendor + linesep);
			buffer.append("SET DST_SERVER=" + FormatENVString(this.dstServer)
					+ linesep);
			buffer.append("SET DST_DATABASE=" + FormatENVString(this.dstDBName)
					+ linesep);
			buffer.append("SET DST_PORT=" + this.dstPort + linesep);
			buffer.append("SET DST_UID=" + FormatENVString(this.dstUid)
					+ linesep);
			buffer
					.append("REM Password was encrypted. You can replace this with clear text password, if required."
							+ linesep);
			buffer.append("SET DST_PWD="
					+ IBMExtractUtilities.Encrypt(this.dstPwd) + linesep
					+ linesep);

			buffer
					.append("\"%JAVA_HOME%\\bin\\java\" -DINPUT_DIR=. -cp %CLASSPATH% ibm.Count %TABLES% %SRC_VENDOR% %SRC_SERVER% %SRC_DATABASE% %SRC_PORT% %SRC_UID% %SRC_PWD% %DST_VENDOR% %DST_SERVER% %DST_DATABASE% %DST_PORT% %DST_UID% %DST_PWD%"
							+ linesep);
			this.rowCountWriter = new BufferedWriter(new FileWriter(
					tmpFileName, false));
		} else {
			if (this.dstVendor.equals("zdb2"))
				buffer.append("#!/bin/sh" + linesep + linesep);
			else
				buffer.append("#!/bin/ksh" + linesep + linesep);
			buffer.append("# Copyright(r) IBM Corporation" + linesep);
			buffer.append("#" + linesep);
			buffer.append("# Vikram Khatri (vikram.khatri@us.ibm.com)"
					+ linesep);
			buffer.append("#" + linesep + linesep);
			buffer
					.append("# Run this script to count rows from source and target DB server to compare."
							+ linesep);
			buffer.append("#" + linesep + linesep);

			buffer
					.append("echo -------------------------------------------------------------------"
							+ linesep);
			buffer.append("echo Migration from " + this.srcVendor + " to "
					+ this.dstVendor + linesep);
			buffer
					.append("echo -------------------------------------------------------------------"
							+ linesep);

			buffer.append("JAVA_HOME=" + this.javaHome + linesep);
			if (this.srcJDBC.equals(this.dstJDBC))
				buffer.append("CLASSPATH="
						+ IBMExtractUtilities.putQuote(this.appJAR, sep) + ":"
						+ IBMExtractUtilities.putQuote(this.srcJDBC, sep)
						+ linesep + linesep);
			else {
				buffer.append("CLASSPATH="
						+ IBMExtractUtilities.putQuote(this.appJAR, sep) + ":"
						+ IBMExtractUtilities.putQuote(this.srcJDBC, sep) + ":"
						+ IBMExtractUtilities.putQuote(this.dstJDBC, sep)
						+ linesep + linesep);
			}
			buffer.append("TABLES=" + this.srcDBName + ".tables" + linesep);
			buffer.append("SRC_VENDOR=" + this.srcVendor + linesep);
			buffer.append("SRC_SERVER=" + FormatENVString(this.srcServer)
					+ linesep);
			buffer.append("SRC_DATABASE=" + FormatENVString(this.srcDBName)
					+ linesep);
			buffer.append("SRC_PORT=" + this.srcPort + linesep);
			buffer.append("SRC_UID=" + FormatENVString(this.srcUid) + linesep);
			buffer
					.append("### Password was encrypted. You can replace this with clear text password, if required."
							+ linesep);
			buffer.append("SRC_PWD=" + IBMExtractUtilities.Encrypt(this.srcPwd)
					+ linesep + linesep);
			buffer.append("DST_VENDOR=" + FormatENVString(this.dstVendor)
					+ linesep);
			buffer.append("DST_SERVER=" + FormatENVString(this.dstServer)
					+ linesep);
			buffer.append("DST_DATABASE=" + FormatENVString(this.dstDBName)
					+ linesep);
			buffer.append("DST_PORT=" + this.dstPort + linesep);
			buffer.append("DST_UID=" + FormatENVString(this.dstUid) + linesep);
			buffer
					.append("### Password was encrypted. You can replace this with clear text password, if required."
							+ linesep);
			buffer.append("DST_PWD=" + IBMExtractUtilities.Encrypt(this.dstPwd)
					+ linesep + linesep);

			buffer
					.append("$JAVA_HOME/bin/java -DINPUT_DIR=. -cp $CLASSPATH ibm.Count $TABLES $SRC_VENDOR $SRC_SERVER $SRC_DATABASE $SRC_PORT $SRC_UID $SRC_PWD $DST_VENDOR $DST_SERVER $DST_DATABASE $DST_PORT $DST_UID $DST_PWD"
							+ linesep);
			Runtime rt = Runtime.getRuntime();
			this.rowCountWriter = new BufferedWriter(new FileWriter(
					tmpFileName, false));
			rt.exec("chmod 755 " + tmpFileName);
		}
		this.rowCountWriter.write(buffer.toString());
		this.rowCountWriter.close();
		log("Command file " + tmpFileName + " created.");
	}

	public void getParamValues() {
		this.appJAR = (this.currHome + File.separator + "IBMDataMovementTool.jar");
		log("appJar                 : '" + this.appJAR + "'");
		if (this.paramPropFound) {
			this.javaHome = ((String) this.propParams.get("javaHome"));
			this.srcJDBC = ((String) this.propParams.get("srcJDBC"));
			this.dstJDBC = ((String) this.propParams.get("dstJDBC"));
			this.srcVendor = ((String) this.propParams.get("srcVendor"));
			this.srcServer = ((String) this.propParams.get("srcServer"));
			this.srcPort = Integer.parseInt((String) this.propParams
					.get("srcPort"));
			this.srcDBName = ((String) this.propParams.get("srcDBName"));
			this.srcSchName = ((String) this.propParams.get("srcSchName"));
			this.srcUid = ((String) this.propParams.get("srcUid"));
			this.srcPwd = ((String) this.propParams.get("srcPwd"));
			if (IBMExtractUtilities.isHexString(this.srcPwd))
				this.srcPwd = IBMExtractUtilities.Decrypt(this.srcPwd);
			this.dstVendor = ((String) this.propParams.get("dstVendor"));
			this.dstServer = ((String) this.propParams.get("dstServer"));
			this.dstPort = Integer.parseInt((String) this.propParams
					.get("dstPort"));
			this.dstDBName = ((String) this.propParams.get("dstDBName"));
			this.dstSchName = ((String) this.propParams.get("dstSchName"));
			this.dstUid = ((String) this.propParams.get("dstUid"));
			this.dstPwd = ((String) this.propParams.get("dstPwd"));
			if (IBMExtractUtilities.isHexString(this.dstPwd))
				this.dstPwd = IBMExtractUtilities.Decrypt(this.dstPwd);
			this.extractDDL = ((String) this.propParams.get("extractDDL"));
			this.extractData = ((String) this.propParams.get("extractData"));
			this.srcJDBCHome = ((String) this.propParams.get("srcJDBCHome"));
			if ((this.srcJDBCHome == null) || (this.srcJDBCHome.length() == 0))
				this.srcJDBCHome = this.currHome;
			this.dstJDBCHome = ((String) this.propParams.get("dstJDBCHome"));
			this.outputDirectory = ((String) this.propParams
					.get("outputDirectory"));
			this.srcDB2Home = ((String) this.propParams.get("srcDB2Home"));
			this.dstDB2Home = ((String) this.propParams.get("dstDB2Home"));
			this.srcDB2Instance = ((String) this.propParams
					.get("srcDB2Instance"));
			this.dstDB2Instance = ((String) this.propParams
					.get("dstDB2Instance"));
			this.srcDB2Release = ((String) this.propParams.get("srcDB2Release"));
			this.dstDB2Release = ((String) this.propParams.get("dstDB2Release"));
			this.regenerateTriggers = ((String) this.propParams
					.get("regenerateTriggers"));
			this.trimTrailingSpaces = ((String) this.propParams
					.get("trimTrailingSpaces"));
			this.db2Compatibility = ((String) this.propParams
					.get("db2_compatibility"));
			this.dbclob = ((String) this.propParams.get("dbclob"));
			this.graphic = ((String) this.propParams.get("graphic"));
			this.remoteLoad = ((String) this.propParams.get("remoteLoad"));
			this.compressTable = ((String) this.propParams.get("compressTable"));
			this.compressIndex = ((String) this.propParams.get("compressIndex"));
			this.debug = ((String) this.propParams.get("debug"));
			this.extractPartitions = ((String) this.propParams
					.get("extractPartitions"));
			this.extractHashPartitions = this.propParams
					.getProperty("extractHashPartitions");
			this.encoding = this.propParams.getProperty("encoding");
			if (this.encoding == null)
				this.encoding = "UTF-8";
			this.customMapping = this.propParams.getProperty("customMapping");
			if (this.customMapping == null)
				this.customMapping = "false";
			if (this.extractHashPartitions == null)
				this.extractHashPartitions = "false";
			this.retainConstraintsName = this.propParams
					.getProperty("retainConstraintsName");
			if (this.retainConstraintsName == null)
				this.retainConstraintsName = "false";
			this.useBestPracticeTSNames = this.propParams
					.getProperty("useBestPracticeTSNames");
			if (this.useBestPracticeTSNames == null)
				this.useBestPracticeTSNames = "true";
			this.limitExtractRows = ((String) this.propParams
					.get("limitExtractRows"));
			if (this.limitExtractRows == null)
				this.limitExtractRows = "ALL";
			this.limitLoadRows = ((String) this.propParams.get("limitLoadRows"));
			if (this.limitLoadRows == null)
				this.limitLoadRows = "ALL";
			this.oracleNumberMapping = ((String) this.propParams
					.get("oracleNumberMapping"));
			if (this.oracleNumberMapping == null)
				this.oracleNumberMapping = "false";
			this.oracleNumb31Mapping = ((String) this.propParams
					.get("oracleNumb31Mapping"));
			if (this.oracleNumb31Mapping == null)
				this.oracleNumb31Mapping = "false";
			this.zdb2tableseries = ((String) this.propParams
					.get("zdb2tableseries"));
			if (this.zdb2tableseries == null)
				this.zdb2tableseries = "Q";
			this.znocopypend = ((String) this.propParams.get("znocopypend"));
			if (this.znocopypend == null)
				this.znocopypend = "true";
			this.zoveralloc = ((String) this.propParams.get("zoveralloc"));
			if (this.zoveralloc == null)
				this.zoveralloc = "1.3636";
			this.zsecondary = ((String) this.propParams.get("zsecondary"));
			if (this.zsecondary == null)
				this.zsecondary = "0";
			this.storclas = ((String) this.propParams.get("storclas"));
			if (this.storclas == null)
				this.storclas = "none";
		}
	}

	public boolean pingJDBCDrivers(String jarNames) {
		String[] tmp2 = jarNames.split("\\|\\|");
		boolean jdbcFound = true;
		this.jarFiles2 = "";
		for (int j = 0; j < tmp2.length; j++) {
			String[] tmp = tmp2[j].split(sep);
			for (int i = 0; i < tmp.length; i++) {
				String name = tmp[i];
				if (IBMExtractUtilities.FileExists(name)) {
					log("JAR file '" + name + "' found");
					this.jarFiles2 = (this.jarFiles2 + name + sep);
				} else {
					log("JAR file '" + name + "' not found.");
					jdbcFound = false;
				}
			}
			if (jdbcFound) {
				return true;
			}
		}
		this.Message = "JDBC Driver JAR files not found.";
		return false;
	}

	public String getJDBCList(String vendor) {
		String jdbcList = "";
		String listJDBC = (String) this.propJDBC.get(vendor.toLowerCase());
		String[] tmp2 = listJDBC.split("\\|\\|");
		for (int j = 0; j < tmp2.length; j++) {
			String[] tmp = tmp2[j].split(":");
			if (j > 0) {
				jdbcList = jdbcList + " OR \n";
			}
			for (int i = 0; i < tmp.length; i++) {
				if (i < tmp.length - 1) {
					jdbcList = jdbcList + tmp[i] + "\n";
				} else {
					jdbcList = jdbcList + tmp[i];
				}
			}
		}
		return jdbcList;
	}

	public String getJDBCJARName(String vendor, String jdbcHome) {
		String jdbcName = "";
		String returnJDBCStr = "";
		jdbcName = (String) this.propJDBC.get(vendor.toLowerCase());
		this.dstJDBC = ((String) this.propJDBC
				.get(this.dstVendor.toLowerCase()));
		String[] tmp2 = jdbcName.split("\\|\\|");
		if ((jdbcHome == null) || (jdbcHome.equals(""))) {
			if (vendor.equalsIgnoreCase("db2")) {
				jdbcHome = IBMExtractUtilities.db2JDBCHome();
			} else
				jdbcHome = this.currHome;
		}
		for (int j = 0; j < tmp2.length; j++) {
			String[] tmp = tmp2[j].split(":");
			for (int i = 0; i < tmp.length; i++) {
				String name;
				if ((jdbcHome == null) || (jdbcHome.length() == 0))
					name = tmp[i];
				else
					name = jdbcHome + File.separator + tmp[i];
				if (i < tmp.length - 1) {
					returnJDBCStr = returnJDBCStr + name + sep;
				} else {
					returnJDBCStr = returnJDBCStr + name;
				}
			}
			returnJDBCStr = returnJDBCStr + "||";
		}
		return returnJDBCStr;
	}

	public void loadConfigFile() {
		this.propParams = new Properties();
		this.propJDBC = new Properties();
		InputStream istream;
		try {
			istream = ClassLoader
					.getSystemResourceAsStream(this.JDBC_PROP_FILE);
			if (istream == null) {
				try {
					this.propJDBC
							.load(new FileInputStream(this.JDBC_PROP_FILE));
					log("Configuration file loaded: '" + this.JDBC_PROP_FILE
							+ "'");
				} catch (Exception e) {
					log("exception loading properties: " + e);
					System.exit(-1);
				}
			} else {
				this.propJDBC.load(istream);
				log("Configuration file loaded: '" + this.JDBC_PROP_FILE + "'");
			}
		} catch (IOException ex) {
			log("exception loading properties: " + ex);
			System.exit(-1);
		}

		this.paramPropFound = true;
		try {
			istream = ClassLoader
					.getSystemResourceAsStream(this.PARAM_PROP_FILE);
			if (istream == null) {
				try {
					this.propParams.load(new FileInputStream(
							this.PARAM_PROP_FILE));
					if (this.propParams.get("javaHome") == null) {
						this.paramPropFound = false;
					}
					if (this.paramPropFound)
						log("Configuration file loaded: '"
								+ this.PARAM_PROP_FILE + "'");
				} catch (Exception e) {
					this.paramPropFound = false;
				}
			} else {
				this.propParams.load(istream);
				log("Configuration file loaded: '" + this.PARAM_PROP_FILE + "'");
			}
		} catch (IOException ex) {
			log("exception loading properties: " + ex);
			System.exit(-1);
		}
	}

	private String FormatCMDString(String inputStr) {
		if ((inputStr == null) || (inputStr.equals(""))) {
			return inputStr;
		}
		return inputStr.replaceAll("\\\\", "\\\\\\\\");
	}

	public void writeConfigFile() {
		try {
			BufferedWriter propWriter = new BufferedWriter(new FileWriter(
					this.PARAM_PROP_FILE, false));
			if (osType.equalsIgnoreCase("Win")) {
				propWriter.write("appJAR=" + FormatCMDString(this.appJAR)
						+ linesep);
				propWriter.write("javaHome=" + FormatCMDString(this.javaHome)
						+ linesep);
				propWriter.write("srcDB2Home="
						+ FormatCMDString(this.srcDB2Home) + linesep);
				propWriter.write("srcDB2Instance="
						+ FormatCMDString(this.srcDB2Instance) + linesep);
				propWriter.write("srcJDBC=" + FormatCMDString(this.srcJDBC)
						+ linesep);
				propWriter.write("srcJDBCHome="
						+ FormatCMDString(this.srcJDBCHome) + linesep);
				propWriter.write("srcServer=" + FormatCMDString(this.srcServer)
						+ linesep);
				propWriter.write("srcDBName=" + FormatCMDString(this.srcDBName)
						+ linesep);
				propWriter.write("srcSchName="
						+ FormatCMDString(this.srcSchName) + linesep);
				propWriter.write("srcUid=" + FormatCMDString(this.srcUid)
						+ linesep);
				propWriter.write("srcPwd="
						+ IBMExtractUtilities.Encrypt(this.srcPwd) + linesep);
			} else {
				propWriter.write("appJAR=" + this.appJAR + linesep);
				propWriter.write("javaHome=" + this.javaHome + linesep);
				propWriter.write("srcDB2Home=" + this.srcDB2Home + linesep);
				propWriter.write("srcDB2Instance=" + this.srcDB2Instance
						+ linesep);
				propWriter.write("srcJDBC=" + this.srcJDBC + linesep);
				propWriter.write("srcJDBCHome=" + this.srcJDBCHome + linesep);
				propWriter.write("srcServer=" + this.srcServer + linesep);
				propWriter.write("srcDBName=" + this.srcDBName + linesep);
				propWriter.write("srcSchName=" + this.srcSchName + linesep);
				propWriter.write("srcUid=" + this.srcUid + linesep);
				propWriter.write("srcPwd="
						+ IBMExtractUtilities.Encrypt(this.srcPwd) + linesep);
			}
			propWriter.write("srcVendor=" + this.srcVendor + linesep);
			propWriter.write("srcPort=" + this.srcPort + linesep);
			if (osType.equalsIgnoreCase("Win")) {
				propWriter.write("dstDB2Home="
						+ FormatCMDString(this.dstDB2Home) + linesep);
				propWriter.write("dstDB2Instance="
						+ FormatCMDString(this.dstDB2Instance) + linesep);
				propWriter.write("dstDB2Release="
						+ FormatCMDString(this.dstDB2Release) + linesep);
				propWriter.write("dstJDBC=" + FormatCMDString(this.dstJDBC)
						+ linesep);
				propWriter.write("dstJDBCHome="
						+ FormatCMDString(this.dstJDBCHome) + linesep);
				propWriter.write("outputDirectory="
						+ FormatCMDString(this.outputDirectory) + linesep);
				propWriter.write("dstServer=" + FormatCMDString(this.dstServer)
						+ linesep);
				propWriter.write("dstDBName=" + FormatCMDString(this.dstDBName)
						+ linesep);
				propWriter.write("dstSchName="
						+ FormatCMDString(this.dstSchName) + linesep);
				propWriter.write("dstUid=" + FormatCMDString(this.dstUid)
						+ linesep);
				propWriter.write("dstPwd="
						+ IBMExtractUtilities.Encrypt(this.dstPwd) + linesep);
			} else {
				propWriter.write("dstDB2Home=" + this.dstDB2Home + linesep);
				propWriter.write("dstDB2Instance=" + this.dstDB2Instance
						+ linesep);
				propWriter.write("dstDB2Release=" + this.dstDB2Release
						+ linesep);
				propWriter.write("dstJDBC=" + this.dstJDBC + linesep);
				propWriter.write("dstJDBCHome=" + this.dstJDBCHome + linesep);
				propWriter.write("outputDirectory=" + this.outputDirectory
						+ linesep);
				propWriter.write("dstServer=" + this.dstServer + linesep);
				propWriter.write("dstDBName=" + this.dstDBName + linesep);
				propWriter.write("dstSchName=" + this.dstSchName + linesep);
				propWriter.write("dstUid=" + this.dstUid + linesep);
				propWriter.write("dstPwd="
						+ IBMExtractUtilities.Encrypt(this.dstPwd) + linesep);
			}
			propWriter.write("dstVendor=" + this.dstVendor + linesep);
			propWriter.write("dstPort=" + this.dstPort + linesep);
			propWriter.write("extractDDL=" + this.extractDDL + linesep);
			propWriter.write("extractData=" + this.extractData + linesep);

			propWriter.write("debug=" + this.debug + linesep);
			propWriter.write("RetainColName=true" + linesep);
			propWriter.write("encoding=" + this.encoding + linesep);
			propWriter.write("graphic=" + this.graphic + linesep);
			propWriter.write("mssqltexttoclob=false" + linesep);
			propWriter.write("dumpfile=false" + linesep);
			propWriter.write("loadstats=false" + linesep);
			propWriter.write("customMapping=" + this.customMapping + linesep);
			propWriter.write("putconnectstatement=true" + linesep);
			propWriter.write("db2_compatibility=" + this.db2Compatibility
					+ linesep);
			propWriter.write("roundDown_31=true" + linesep);
			propWriter.write("dbclob=" + this.dbclob + linesep);
			propWriter.write("trimTrailingSpaces=" + this.trimTrailingSpaces
					+ linesep);
			propWriter.write("regenerateTriggers=" + this.regenerateTriggers
					+ linesep);
			propWriter.write("remoteLoad=" + this.remoteLoad + linesep);
			propWriter.write("compressTable=" + this.compressTable + linesep);
			propWriter.write("compressIndex=" + this.compressIndex + linesep);
			propWriter.write("extractPartitions=" + this.extractPartitions
					+ linesep);
			propWriter.write("extractHashPartitions="
					+ this.extractHashPartitions + linesep);
			propWriter.write("retainConstraintsName="
					+ this.retainConstraintsName + linesep);
			propWriter.write("useBestPracticeTSNames="
					+ this.useBestPracticeTSNames + linesep);
			propWriter.write("limitExtractRows=" + this.limitExtractRows
					+ linesep);
			propWriter.write("limitLoadRows=" + this.limitLoadRows + linesep);
			propWriter.write("oracleNumberMapping=" + this.oracleNumberMapping
					+ linesep);
			propWriter.write("oracleNumb31Mapping=" + this.oracleNumb31Mapping
					+ linesep);

			if (this.dstVendor.equalsIgnoreCase("zdb2")) {
				propWriter.write("zdb2tableseries=" + this.zdb2tableseries
						+ linesep);
				propWriter.write("znocopypend=" + this.znocopypend + linesep);
				propWriter.write("zoveralloc=" + this.zoveralloc + linesep);
				propWriter.write("zsecondary=" + this.zsecondary + linesep);
				propWriter.write("storclas=" + this.storclas + linesep);
			}

			propWriter.write(linesep + linesep + "#### Comments ##### "
					+ linesep);
			propWriter
					.write("#debug=[false|true]. If true, debug messages will be printed. "
							+ linesep);
			propWriter
					.write("#RetainColName=[true|false]. If true, tool will not truncate column name to 30 chars. "
							+ linesep);
			propWriter
					.write("#encoding=[US-ASCII|ISO-8859-1|UTF-8|UTF-16BE|UTF-16LE|UTF-16]"
							+ linesep);
			propWriter
					.write("#graphic=false. Treat NVARCHAR, NCHAR and NTEXT column as VARCHAR, CHAR and CLOB if false."
							+ linesep);
			propWriter
					.write("#mssqltexttoclob=false. Treat SQL Server TEXT as String and not CLOB while retrieving (Performance)."
							+ linesep);
			propWriter
					.write("#dumpfile=false. Create dumpfile option in LOAD Script if true."
							+ linesep);
			propWriter
					.write("#loadstats=false. Create STATISTICS option in LOAD Script if true."
							+ linesep);
			propWriter
					.write("#customMapping=false. Do not use this unless you are doing FileNet migration. The default is false and use true for FileNet"
							+ linesep);
			propWriter
					.write("#putconnectstatement=true. If true, put CONNECT TO statement in SQL scripts and if false, do not put"
							+ linesep);
			propWriter
					.write("#db2_compatibility=true. If true, use Oracle compatibility features in DB2. Use it when migrating from Oracle to DB2 V9.7 onwards"
							+ linesep);
			propWriter
					.write("#roundDown_31=true. If true, round down number > 31 to 31 otherwise map them to DOUBLE data type."
							+ linesep);
			propWriter
					.write("#dbclob=false. If false, CLOBS are retained as it is. If true, CLOB is converted to DBCLOB in DB2."
							+ linesep);
			propWriter
					.write("#trimTrailingSpaces=true. If true, it will trim trailing whitespaces from CHAR and VARCHAR columns."
							+ linesep);
			propWriter
					.write("#remoteLoad=true. If true, LOAD can be used from a client. But, CLOBS, BLOBS and XML still need to be on DB2 server as that is the LOAD requirement"
							+ linesep);
			propWriter
					.write("#compressTable=true. If true, table will be created with COMPRESS YES in CREATE TABLE script"
							+ linesep);
			propWriter
					.write("#compressIndex=true. If true, index will be created with COMPRESS YES in CREATE INDEX script for DB2 9.7 onwards"
							+ linesep);
			propWriter
					.write("#extractPartitions=true. If true, Oracle table partitions information will be mapped in DB2"
							+ linesep);
			propWriter
					.write("#extractHashPartitions=false. If true, Oracle Hash Partitions will be mapped in DB2"
							+ linesep);
			propWriter
					.write("#retainConstraintsName=false. If true, constraints name are used from source database as it is"
							+ linesep);
			propWriter
					.write("#useBestPracticeTSNames=true. If false, extract Oracle tablespace names and use it in DB2."
							+ linesep);
			propWriter
					.write("#limitExtractRows=ALL. If ALL, extract all rows otherwise specify a value > 0."
							+ linesep);
			propWriter
					.write("#limitLoadRows=ALL. If ALL, load all rows in DB2 otherwise specify a value > 0."
							+ linesep);
			propWriter
					.write("#oracleNumberMapping=false. If set to true in mode db2_compatibility=true, map NUMBER(0) -> DOUBLE, NUMBER(1-5) -> SMALLINT, NUMBER(5-9) -> INTEGER, NUMBER(10-18) -> BIGINT, NUMBER(19-21) -> FLOAT, NUMBER(>31) -> DOUBLE"
							+ linesep);
			propWriter
					.write("#oracleNumb31Mapping=false. (Applicable for DB2 >= 9.5) If set to true, map NUMBER(>31) -> DECFLOAT(34). roundDown_31=false or true has no effect on this param"
							+ linesep);
			if (this.dstVendor.equals("zdb2")) {
				propWriter
						.write("#zdb2tableseries=Q. The first letter of the dataset series name for not to overwrite dataset for other databases migration. This is only for migration to zDB2"
								+ linesep);

				propWriter
						.write("#znocopypend=[true|false]. If true, use NOCOPYPEND option in LOAD for zDB2"
								+ linesep);
				propWriter
						.write("#zoveralloc=value. The overAlloc variable specifies by how much we want to oversize our file allocation requests.  A value of 1 would mean don't oversize at all.  In an environment with tons of free storage, this might actually work.  In a realistic environment, 15/11 (1.3636) seems to be a good guess.  But, I guess it could be good for tuning to give others the ability to customize this.  I would recommend starting at 1.3636 (15/11) and lowering the value until you get file write errors, and then bumping it back up a little. "
								+ linesep);

				propWriter
						.write("#zsecondary=value. Allocate fixed secondary extent. Starting with secondary set to 0 and increase it slowly until file errors occur and then bring it back down"
								+ linesep);

				propWriter
						.write("#storclas=none. Specify none if do not want to use storclas. Length can be from 1 to 8 only"
								+ linesep);
			}

			propWriter.close();
			log("All input parameters are saved in 'IBMExtract.properties' file.");
			this.propParams.load(new FileInputStream(this.PARAM_PROP_FILE));
		} catch (IOException ex) {
			ex.printStackTrace();
			System.exit(-1);
		}
	}

	private int getVendorNum() {
		if ((this.srcVendor.equals("")) || (this.srcVendor.equals("oracle")))
			return 1;
		if (this.srcVendor.equals("mssql"))
			return 2;
		if (this.srcVendor.equals("sybase"))
			return 3;
		if (this.srcVendor.equals("access"))
			return 4;
		if (this.srcVendor.equals("mysql"))
			return 5;
		if (this.srcVendor.equals("postgres"))
			return 6;
		if (this.srcVendor.equals("zdb2"))
			return 7;
		if (this.srcVendor.equals("db2")) {
			return 8;
		}
		return 0;
	}

	public int getDefaultVendorPort(String vendor) {
		int defPort = 0;
		if (vendor.equals("oracle"))
			defPort = 1521;
		else if (vendor.equals("mssql"))
			defPort = 1433;
		else if (vendor.equals("sybase"))
			defPort = 1433;
		else if (vendor.equals("mysql"))
			defPort = 3306;
		else if (vendor.equals("access"))
			defPort = 0;
		else if (vendor.equals("postgres"))
			defPort = 5432;
		else if (vendor.equals("zdb2"))
			defPort = 50000;
		else if (vendor.equals("db2"))
			defPort = 50000;
		return defPort;
	}

	public String getVendor(int num) {
		String vendor = "";
		switch (num) {
		case 1:
			vendor = "oracle";
			break;
		case 2:
			vendor = "mssql";
			break;
		case 3:
			vendor = "sybase";
			break;
		case 4:
			vendor = "access";
			break;
		case 5:
			vendor = "mysql";
			break;
		case 6:
			vendor = "postgres";
			break;
		case 7:
			vendor = "zdb2";
			break;
		case 8:
			vendor = "db2";
			break;
		default:
			vendor = "";
		}
		return vendor;
	}

	private void getSrcVendorConsoleInput() {
		int num = 1;
		do {
			int defNum = getVendorNum();
			num = 1;
			System.out.println("Oracle                  : 1 ");
			System.out.println("MS SQL Server           : 2 ");
			System.out.println("Sybase                  : 3 ");
			System.out.println("MS Access Database      : 4 ");
			System.out.println("MySQL                   : 5 ");
			System.out.println("PostgreSQL              : 6 ");
			System.out.println("DB2 z/OS                : 7 ");
			System.out.println("DB2 LUW                 : 8 ");
			System.out.print("Enter a number (Default " + defNum + ") : ");
			try {
				String inputa = this.stdin.readLine();
				num = inputa.equals("") ? defNum : Integer.parseInt(inputa);
			} catch (Exception e) {
				num = -1;
			}
		} while ((num < 1) || (num > 8));

		this.srcVendor = getVendor(num);
		if (this.srcPort == 0) {
			this.srcPort = getDefaultVendorPort(this.srcVendor);
		}
		if (this.srcVendor.equals("oracle")) {
			num = getDB2Compatibility().equals("true") ? 1 : 2;
			num = getYesNoQuitConsoleInput(num,
					"DB2 Compatibility Feature (DB2 V9.7 or later)  : 1 ",
					"No Compatibility feature                       : 2 ",
					"Enter compatibility feature");

			switch (num) {
			case 1:
				this.db2Compatibility = "true";
				break;
			case 2:
				this.db2Compatibility = "false";
			}

			if (this.db2Compatibility.equalsIgnoreCase("true")) {
				num = this.regenerateTriggers.equals("true") ? 1 : 2;
				num = getYesNoQuitConsoleInput(num,
						"Split Oracle multiple action triggers (Yes) : 1 ",
						"Do not split                                : 2 ",
						"Enter regenerateTriggers");

				switch (num) {
				case 1:
					this.regenerateTriggers = "true";
					break;
				case 2:
					this.regenerateTriggers = "false";
				}
			}
		}
	}

	private String getStringConsoleInput(String token, String prompt,
			String validValues) {
		int num = 1;
		String returnValue = "";
		do {
			if (!IBMExtractUtilities.isValidValue(token, validValues))
				System.out.println("Invalid value=" + token
						+ ". Valid values are: " + validValues);
			if ((token == null) || (token.length() == 0)) {
				System.out.print(prompt);
			} else if (IBMExtractUtilities.isValidValue(token, validValues)) {
				System.out.print(prompt + " (Default=" + token + ") : ");
			} else
				System.out.print(prompt);

			try {
				String inputa = this.stdin.readLine();
				if (inputa.equals("")) {
					num = -1;
				} else if (inputa.equals("1")) {
					num = 1;
				} else {
					token = inputa.toUpperCase();
					if (IBMExtractUtilities.isValidValue(token, validValues)) {
						System.out
								.print("You entered '"
										+ token
										+ "' Press 1 to re-enter or hit enter to continue : ");
					}
				}
				returnValue = token;
			} catch (Exception e) {
				num = -1;
			}
		} while (num == 1);
		return returnValue;
	}

	public String getStringConsoleInput(String token, String prompt) {
		int num = 1;
		String returnValue = "";
		do {
			if (token.equals(""))
				System.out.print(prompt + " : ");
			else
				System.out.print(prompt + " (Default=" + token + ") : ");
			try {
				String inputa = this.stdin.readLine();
				if (inputa.equals("")) {
					num = -1;
					returnValue = token;
				} else {
					System.out
							.print("You entered '"
									+ inputa
									+ "' Press 1 to re-enter or hit enter to continue : ");
					returnValue = inputa;
					inputa = this.stdin.readLine();
					if (inputa.equals("")) {
						num = -1;
					}
				}
			} catch (Exception e) {
				num = -1;
			}
		} while (num == 1);
		return returnValue;
	}

	private String getIntOrALLConsoleInput(String token, String prompt) {
		int num = 1;
		String returnValue = "";
		do {
			if ((token == null) || (token.length() == 0)) {
				token = "ALL";
			}
			System.out.print(prompt + " (Default=" + token + ") : ");
			try {
				String inputa = this.stdin.readLine();
				if (inputa.equals("")) {
					num = -1;
					returnValue = token;
				} else if (inputa.equalsIgnoreCase("ALL")) {
					num = -1;
					returnValue = "ALL";
				} else {
					int x = Integer.parseInt(inputa);
					num = x < 0 ? 1 : -1;
					returnValue = inputa;
				}
			} catch (Exception e) {
				num = 1;
			}
		} while (num == 1);
		return returnValue;
	}

	public int getIntConsoleInput(int token, String prompt) {
		int num = 1;

		int returnValue = 0;
		do {
			num = -1;
			System.out.print(prompt + " (Default=" + token + ") : ");
			try {
				String inputa = this.stdin.readLine();
				if (inputa.equals("")) {
					num = returnValue = token;
				} else {
					num = returnValue = inputa.equals("") ? token : Integer
							.parseInt(inputa);
				}
			} catch (Exception e) {
				num = -1;
			}
		} while (num == -1);
		return returnValue;
	}

	public int getYesNoQuitConsoleInput(int defToken, String line1,
			String line2, String prompt) {
		int num = 1;
		do {
			num = 1;
			System.out.println(line1);
			System.out.println(line2);
			System.out.print(prompt + " (Default=" + defToken + ") : ");
			try {
				String inputa = this.stdin.readLine();
				num = inputa.equals("") ? defToken : Integer.parseInt(inputa);
			} catch (Exception e) {
				num = -1;
			}
		} while ((num < 1) || (num > 2));
		return num;
	}

	private void extractData() {
		if (getYesNoQuitConsoleInput(1,
				"Do you want to change output directory (Yes) : 1 ",
				"No                                           : 2 ",
				"Enter a number") == 1) {
			this.outputDirectory = getStringConsoleInput(this.outputDirectory,
					"Enter output directory");
		}
		System.setProperty("OUTPUT_DIR", this.outputDirectory);
		IBMExtractUtilities.CreateTableScript(this.srcVendor, "",
				this.srcSchName, this.srcServer, this.srcPort, this.srcDBName,
				this.srcUid, this.srcPwd);

		if (IBMExtractUtilities.Message.equals("")) {
			if (getYesNoQuitConsoleInput(
					1,
					"You can remove tables that you do not want to migrate by editing "
							+ this.srcDBName
							+ ".tables file"
							+ "\nDo you want to go ahead and extract data (Yes) : 1 ",
					"Quit Program                                   : 2 ",
					"Enter a number") == 2) {
				return;
			}
			if (this.dstVendor.equals("db2"))
				this.dstDBName = getStringConsoleInput(this.dstDBName,
						"Enter target db2 database name");
			else
				this.dstDBName = getStringConsoleInput(this.dstDBName,
						"Enter z/OS db2 location name");
			try {
				writeConfigFile();
				writeGeninput();
				writeUnload(this.unload);
				writeRowCount();
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (this.dstVendor.equals("db2")) {
				System.setProperty("IBMExtractPropFile", getIBMPropFile());
				String tabFile = this.outputDirectory + "/" + getSrcDBName()
						+ ".tables";
				String[] myArgs = { tabFile, "~", this.srcVendor,
						String.valueOf(this.numThreads), this.srcServer,
						this.srcDBName, "" + this.srcPort, this.srcUid,
						this.srcPwd, this.extractDDL, this.extractData,
						this.fetchSize, "false", "false" };

				GenerateExtract.main(myArgs);
			} else {
				System.out
						.println("Scripts generated for migration. Please check them and run unload to extract data.");
			}
		}
	}

	public void collectInput() {
		int num = 1;

		num = this.debug.equals("true") ? 1 : 2;
		num = getYesNoQuitConsoleInput(num, "Debug (Yes)        : 1 ",
				"Debug (No)         : 2 ", "Enter a number");

		switch (num) {
		case 1:
			this.debug = "true";
			break;
		case 2:
			this.debug = "false";
		}

		if (this.dstVendor.equals("db2")) {
			num = this.remoteLoad.equals("false") ? 1 : 2;
			num = getYesNoQuitConsoleInput(num,
					"IS TARGET DB2 LOCAL (YES)   : 1 ",
					"IS TARGET DB2 REMOTE (NO)   : 2 ", "Enter a number");

			switch (num) {
			case 1:
				this.remoteLoad = "false";
				break;
			case 2:
				this.remoteLoad = "true";
			}
		} else {
			this.remoteLoad = "false";
		}
		num = this.extractDDL.equals("true") ? 1 : 2;
		num = getYesNoQuitConsoleInput(num, "Extract DDL (Yes)        : 1 ",
				"Extract DDL (No)         : 2 ", "Enter a number");

		switch (num) {
		case 1:
			this.extractDDL = "true";
			break;
		case 2:
			this.extractDDL = "false";
		}
		num = this.extractData.equals("true") ? 1 : 2;

		num = getYesNoQuitConsoleInput(num, "Extract Data (Yes)        : 1 ",
				"Extract Data (No)         : 2 ", "Enter a number");

		switch (num) {
		case 1:
			this.extractData = "true";
			break;
		case 2:
			this.extractData = "false";
		}

		if (this.extractData.equals("true")) {
			getIntOrALLConsoleInput(this.limitExtractRows,
					"Enter # of rows limit to extract. ");
			getIntOrALLConsoleInput(this.limitLoadRows,
					"Enter # of rows limit to load data in DB2. ");
		}

		num = this.compressTable.equals("false") ? 1 : 2;
		num = getYesNoQuitConsoleInput(num,
				"Compress Table in DB2 (No)     : 1 ",
				"Compress Table in DB2 (YES)    : 2 ", "Enter a number");

		switch (num) {
		case 1:
			this.compressTable = "false";
			break;
		case 2:
			this.compressTable = "true";
		}

		num = this.compressIndex.equals("false") ? 1 : 2;
		num = getYesNoQuitConsoleInput(num,
				"Compress Index in DB2 (No)     : 1 ",
				"Compress Index in DB2 (YES)    : 2 ", "Enter a number");

		switch (num) {
		case 1:
			this.compressIndex = "false";
			break;
		case 2:
			this.compressIndex = "true";
		}

		System.out.println(" ******* Source database information: ***** ");

		getSrcVendorConsoleInput();

		String validValues = "US-ASCII,ISO-8859-1,UTF-8,UTF-16BE,UTF-16LE,UTF-16,ISO-8859-8";
		this.encoding = getStringConsoleInput(this.encoding,
				"Enter encoding. ", validValues);

		this.numThreads = getIntConsoleInput(this.numThreads,
				"Number of threads to extract data");
		if (this.numThreads < 1) {
			this.numThreads = 5;
			System.out.println("Invalid input. Setting threads="
					+ this.numThreads);
		}

		if (getYesNoQuitConsoleInput(1, "Do you want to continue (Yes) : 1 ",
				"Quit program                  : 2 ", "Enter a number") == 2) {
			return;
		}
		if (this.srcVendor.equals("oracle")) {
			num = this.extractPartitions.equals("true") ? 1 : 2;
			num = getYesNoQuitConsoleInput(num,
					"Map Oracle Range Partition to DB2 (Yes) : 1 ",
					"Map Oracle Range Partition to DB2 (No)  : 2 ",
					"Enter a number");

			switch (num) {
			case 1:
				this.extractPartitions = "true";
				break;
			case 2:
				this.extractPartitions = "false";
			}
			num = this.extractHashPartitions.equals("true") ? 1 : 2;
			num = getYesNoQuitConsoleInput(num,
					"Map Oracle Hash Partition to DB2 (Yes) : 1 ",
					"Map Oracle Hash Partition to DB2 (No)  : 2 ",
					"Enter a number");

			switch (num) {
			case 1:
				this.extractHashPartitions = "true";
				break;
			case 2:
				this.extractHashPartitions = "false";
			}
		}

		if (this.srcVendor.equals("access")) {
			System.out
					.println("We will only extract data from access database. Turning DDL off: ");
			System.out
					.println("Contact vikram.khatri@us.ibm.com for extra steps on how to extract DDL from access.");
			this.extractDDL = "false";
			this.srcServer = getStringConsoleInput(this.srcServer,
					"Enter full path for MS Acccess database file name");
			this.srcDBName = "access";
			this.srcPort = 0;
			this.srcUid = "null";
			this.srcPwd = "null";
			this.srcSchName = "ADMIN";
			this.fetchSize = "0";
		} else if (this.srcVendor.equals("mysql")) {
			this.fetchSize = "0";
		} else {
			this.srcServer = getStringConsoleInput(this.srcServer,
					"Enter source database Host Name or IP Address");
		}

		if (!this.srcVendor.equals("access")) {
			this.srcPort = getIntConsoleInput(this.srcPort, "Enter "
					+ this.srcServer + "'s port number");
			if (this.srcVendor.equals("oracle"))
				this.srcDBName = getStringConsoleInput(this.srcDBName,
						"Enter Oracle Service Name or Instance Name");
			else
				this.srcDBName = getStringConsoleInput(this.srcDBName,
						"Enter source Database name");
			this.srcUid = getStringConsoleInput(this.srcUid,
					"Enter User ID of source database");
			this.srcPwd = getStringConsoleInput(this.srcPwd,
					"Enter source database Passsword");
			if (!getJDBCValidation(1, this.srcJDBCHome, this.srcVendor))
				return;
			System.out.println("Now we will try to connect to "
					+ this.srcVendor + " to extract schema names.");
			if (getYesNoQuitConsoleInput(1,
					"Do you want to continue (Yes) : 1 ",
					"Quit program                  : 2 ", "Enter a number") == 2) {
				return;
			}
			if (IBMExtractUtilities.TestConnection(true, true, this.srcVendor,
					this.srcServer, this.srcPort, this.srcDBName, this.srcUid,
					this.srcPwd)) {
				this.srcDB2Home = IBMExtractUtilities.DB2Path;
				this.srcDB2Instance = IBMExtractUtilities.InstanceName;
				String srcSchName2 = IBMExtractUtilities.GetSchemaList(
						this.srcVendor, this.srcServer, this.srcPort,
						this.srcDBName, this.srcUid, this.srcPwd);
				if (IBMExtractUtilities.Message.equals("")) {
					System.out.println(this.srcVendor
							+ "'s schema List extracted=" + srcSchName2);
					if (getYesNoQuitConsoleInput(1, "Do you want to use '"
							+ this.srcUid.toUpperCase() + "' as schema : 1 ",
							"Or the extracted List                   : 2 ",
							"Enter a number") == 1) {
						this.srcSchName = this.srcUid;
					} else
						this.srcSchName = srcSchName2;

					extractData();
				} else {
					System.out.println(IBMExtractUtilities.Message);
				}
			} else {
				System.out.println(IBMExtractUtilities.Message);
			}
		} else {
			extractData();
		}

		System.out.println(" ******* Target database information: ***** ");

		this.dstVendor = (osType.equalsIgnoreCase("z/OS") ? "zdb2" : "db2");
		System.out.println("Your Target database is '" + this.dstVendor + "'");

		if (this.dstVendor.equals("db2")) {
			if (!IBMExtractUtilities.isDB2Installed(Boolean.valueOf(
					this.remoteLoad).booleanValue())) {
				System.out.println("Sorry. I did not detect DB2.");
				if (osType.equalsIgnoreCase("win")) {
					System.out
							.println("This may be due to the fact that you are running this program from a regular Windows command prompt.");
					System.out
							.println("If you have DB2 installed, launch DB2 command prompt and then run this application again.");
				} else {
					System.out
							.println("This may be due to the fact that you are not sourcing db2profile file in your profile.");
					System.out
							.println("You should have DBADM authority to do this migration");
				}
				if (getYesNoQuitConsoleInput(1,
						"Do you want to continue (Yes) : 1 ",
						"Quit program                  : 2 ", "Enter a number") == 2) {
					return;
				}
			}
		}
		if (this.dstVendor.equals("db2")) {
			this.dstServer = getStringConsoleInput(this.dstServer,
					"Enter db2 database Host Name or IP Address");
			this.dstPort = getIntConsoleInput(this.dstPort,
					"Enter db2 Port Number");
			this.dstDBName = getStringConsoleInput(this.dstDBName,
					"Enter db2 Database name");
			this.dstUid = getStringConsoleInput(this.dstUid,
					"Enter db2 database User ID");
			this.dstPwd = getStringConsoleInput(this.dstPwd,
					"Enter db2 database Passsword");
		} else {
			System.out
					.println(" *** Collecting z/OS specific information now. If not sure, use defaults *** ");
			this.zdb2tableseries = getStringConsoleInput(this.zdb2tableseries,
					"Enter qualifier for where to put unload datasets");
			this.znocopypend = getStringConsoleInput(this.znocopypend,
					"Enter the value for NOCOPYPEND for DB2 LOAD");
			this.zoveralloc = getStringConsoleInput(this.zoveralloc,
					"Enter the value of over allocation param");
			this.zsecondary = getStringConsoleInput(this.zsecondary,
					"Enter number of secondary cylinders");
			this.storclas = getStringConsoleInput(this.storclas,
					"Enter the store class");
			System.out
					.println(" *** Collecting z/OS DB2 connection information. *** ");
			this.dstServer = getStringConsoleInput(this.dstServer,
					"Enter z/OS db2 database Host Name or IP Address");
			this.dstPort = getIntConsoleInput(this.dstPort,
					"Enter z/OS db2 Port Number");
			this.dstDBName = getStringConsoleInput(this.dstDBName,
					"Enter z/OS db2 LOCATION name");
			this.dstUid = getStringConsoleInput(this.dstUid,
					"Enter z/OS db2 database User ID");
			this.dstPwd = getStringConsoleInput(this.dstPwd,
					"Enter z/OS db2 database Passsword");
		}

		this.dstJDBCHome = IBMExtractUtilities.db2JDBCHome();
		if (!getJDBCValidation(2, this.dstJDBCHome, this.dstVendor))
			return;
		boolean compatibilityMode = this.db2Compatibility
				.equalsIgnoreCase("true");
		boolean remote = this.remoteLoad.equalsIgnoreCase("true");
		if (IBMExtractUtilities.TestConnection(remote, compatibilityMode,
				this.dstVendor, this.dstServer, this.dstPort, this.dstDBName,
				this.dstUid, this.dstPwd)) {
			this.dstDB2Home = IBMExtractUtilities.DB2Path;
			this.dstDB2Instance = IBMExtractUtilities.InstanceName;
			this.dstDB2Release = IBMExtractUtilities.ReleaseLevel;
			writeConfigFile();
			if (this.dstVendor.equals("db2")) {
				if (getYesNoQuitConsoleInput(1,
						"Do you want to go ahead and deploy data (Yes) : 1 ",
						"Quit Program                                  : 2 ",
						"Enter a number") == 1) {
					String executingScriptName = IBMExtractUtilities
							.db2ScriptName(this.outputDirectory, Boolean
									.parseBoolean(this.extractDDL), Boolean
									.parseBoolean(this.extractData));

					System.out.println("Script to execute :"
							+ executingScriptName);
					RunScript task = new RunScript(null, null,
							this.dstDB2Instance, this.dstDB2Home,
							executingScriptName);
					task.run();
				} else {
					return;
				}
			}
		} else {
			System.out.println(IBMExtractUtilities.Message);
		}
	}

	private void AddJarsToClasspath(String jarList) {
		String[] tmp = jarList.split(sep);
		for (int i = 0; i < tmp.length; i++) {
			File f = new File(tmp[i]);
			try {
				IBMExtractUtilities.AddFile(f);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}

	public boolean getJDBCValidation(int src, String home, String vendor) {
		System.out.println("Now let me check if you have " + vendor
				+ "'s JDBC drivers in " + home + " directory.");
		this.jarFiles = getJDBCJARName(vendor, home);
		this.found = pingJDBCDrivers(this.jarFiles);

		if (!this.found) {
			int num;
			do {
				num = 1;
				System.out.println("Do you know location of  " + vendor
						+ "'s JDBC drivers (Yes) : 1 ");
				System.out
						.println("Quit program                                         : 2 ");
				System.out.print("Enter a number (Default 1) : ");
				try {
					String inputa = this.stdin.readLine();
					num = inputa.equals("") ? 1 : Integer.parseInt(inputa);
				} catch (Exception e) {
					num = -1;
				}
			} while ((num < 1) || (num > 2));

			if (num == 2) {
				return false;
			}

			do {
				num = 1;
				System.out.print("Enter " + vendor
						+ "'s JDBC directory name  : ");
				try {
					String jdbcHome = this.stdin.readLine();
					if (src == 2)
						this.dstJDBCHome = jdbcHome;
					else
						this.srcJDBCHome = jdbcHome;
					if ((jdbcHome != null) && (!jdbcHome.equals(""))) {
						this.jarFiles = getJDBCJARName(vendor, jdbcHome);
						this.found = pingJDBCDrivers(this.jarFiles);
						num = this.found ? -1 : 1;
					}
				} catch (Exception e) {
					num = -1;
				}
			} while (num == 1);
		}
		if (src == 2) {
			this.dstJDBC = this.jarFiles2;
		} else {
			this.srcJDBC = this.jarFiles2;
		}
		AddJarsToClasspath(this.jarFiles2);
		return true;
	}

	private static void log(String msg) {
		System.out.println("[" + timestampFormat.format(new Date()) + "] "
				+ msg);
	}

	public static void main(String[] args) {
		IBMExtractConfig cfg = new IBMExtractConfig();
		cfg.loadConfigFile();
		cfg.getParamValues();
		cfg.collectInput();
	}

	public String getCustomMapping() {
		return this.customMapping;
	}

	public void setCustomMapping(String customMapping) {
		this.customMapping = customMapping;
	}
}