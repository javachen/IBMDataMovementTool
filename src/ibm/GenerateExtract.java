package ibm;

import ibm.lexer.OraToDb2Converter;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.jzos.CatalogSearch;
import com.ibm.jzos.CatalogSearchField;
import com.ibm.jzos.ZFile;
import com.ibm.jzos.ZUtil;

public class GenerateExtract {
	private static final SimpleDateFormat timestampFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH.mm.ss.SSS");
	private static String filesep = System.getProperty("file.separator");
	private static String linesep = System.getProperty("line.separator");
	private static String osType = IBMExtractUtilities.osType;
	private static String sqlTerminator;
	private static String colsep;
	private static String dbSourceName;
	private static String dbTargetName = "db2luw";
	private static String encoding = "UTF-8";
	private static String storclas = "NONE";
	private static String TABLES_PROP_FILE = "";
	private static String OUTPUT_DIR = null;
	private static String PARAM_PROP_FILE = "IBMExtract.properties";
	private static String DATAMAP_PROP_FILE = "datamap.properties";
	private static Properties propDatamap;
	private static Properties propParams;
	private static Properties login = new Properties();
	private static String server = "";
	private static String dbName = "";
	private static String db2dbname = "SAMPLE";
	private static String zdb2tableseries = "Q";
	private static String dstUid;
	private static String dstPwd;
	private static String db2Instance = "";
	private static String appJAR = "";
	private static boolean autoCommit = false;
	private static boolean dataUnload;
	private static boolean ddlGen;
	private static boolean remoteLoad = false;
	private static boolean retainColName = true;
	private static boolean graphic = false;
	private static boolean dumpfileb = false;
	private static boolean oracleNumberMapping = false;
	private static boolean oracleNumb31Mapping = false;
	private static boolean loadstats = false;
	private static boolean mssqltexttoclob = false;
	private static boolean udfcreated = false;
	private static boolean lobsToFiles = false;
	private static boolean znocopypend = true;
	private static boolean debug = false;
	private static boolean customMapping = false;
	private static boolean putConnectStatement = true;
	private static boolean loadReplace = true;
	private static boolean db2_compatibility = true;
	private static boolean roundDown_31 = true;
	private static boolean dbclob = false;
	private static boolean trimTrailingSpaces = false;
	private static boolean regenerateTriggers = false;
	private static boolean compressTable = false;
	private static boolean compressIndex = false;
	private static boolean extractPartitions = true;
	private static boolean extractHashPartitions = false;
	private static boolean retainConstraintsName = false;
	private static boolean useBestPracticeTSNames = true;
	private static String limitExtractRows = "ALL";
	private static String limitLoadRows = "ALL";
	private static String sourceSchema = "";
	private static double overAlloc = 1.3636D;
	private static int secondary = 0;
	private static int threads;
	private static int port;
	private static int fetchSize;
	private static int totalTables;
	private static int numIndex = 1;
	private static int numFkey = 1;
	private static int numUniq = 1;
	private static int nameSeq = 0;
	private static int suffix = 1;
	private static int majorSourceDBVersion;
	private static int minorSourceDBVersion;
	private static float releaseLevel = -1.0F;
	private static long triggerCount = 1L;
	private static String[] query;
	private static String[] schemaName;
	private static String[] tableName;
	private static String[] srcTableName;
	private static String[] srcSchName;
	private static String[] countSQL;
	private static String[] zOSDataSets;
	private static int[] multiTables;
	private static DataOutputStream[] fp;
	private static BufferedWriter[] db2ObjectsWriter;
	private static Hashtable<String, Integer> plsqlHashTable = new Hashtable<String, Integer>();
	private static ZFile[] zfp;
	private static BufferedWriter db2LoadWriter;
	private static BufferedWriter db2TablesWriter;
	private static BufferedWriter db2FKWriter;
	private static BufferedWriter db2DropWriter;
	private static BufferedWriter db2ConsWriter;
	private static BufferedWriter db2ViewsWriter;
	private static BufferedWriter db2rolePrivsWriter;
	private static BufferedWriter db2FKDropWriter;
	private static BufferedWriter db2RunstatWriter;
	private static BufferedWriter db2TabStatusWriter;
	private static BufferedWriter db2CheckPendingWriter;
	private static BufferedWriter db2TabCountWriter;
	private static BufferedWriter db2objPrivsWriter;
	private static BufferedWriter db2udfWriter;
	private static BufferedWriter db2tsbpWriter;
	private static BufferedWriter db2SynonymWriter;
	private static BufferedWriter db2LoadTerminateWriter;
	private static BufferedWriter db2ScriptWriter;
	private static BufferedWriter db2CheckScriptWriter;
	private static BufferedWriter db2CheckWriter;
	private static BufferedWriter db2UniqWriter;
	private static BufferedWriter db2SeqWriter;
	private static BufferedWriter db2DefaultWriter;
	private static BufferedWriter db2TruncNameWriter;
	private static BufferedWriter db2mviewWriter;
	private static BufferedWriter db2DropSynWriter;
	private static BufferedWriter db2DropSeqWriter;
	private static BufferedWriter db2droptsbpWriter;
	private static BufferedWriter db2DropScriptWriter;
	private static BufferedWriter db2DropObjectsWriter;
	private static Properties mapiDB2TableNames = null;
	private static BladeRunner[] blades;
	private static final Object empty = new Object();
	private Connection[] connPool;
	private static Connection mainConn;

	public static String putQuote(String name) {
		return IBMExtractUtilities.putQuote(name);
	}

	public static String removeQuote(String name) {
		return IBMExtractUtilities.removeQuote(name);
	}

	public static String trim(String name) {
		if (name != null)
			name = name.trim();
		return name;
	}

	public static String byteToHex(byte b) {
		char[] hexDigit = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
				'a', 'b', 'c', 'd', 'e', 'f' };

		char[] array = { hexDigit[(b >> 4 & 0xF)], hexDigit[(b & 0xF)] };
		return new String(array);
	}

	public static String getSrctoDstSchema(String schema) {
		for (int i = 0; i < srcSchName.length; i++) {
			if (schema.equals(removeQuote(srcSchName[i])))
				return removeQuote(schemaName[i]);
		}
		return schema;
	}

	public static String charToHex(char c) {
		byte hi = (byte) (c >>> '\b');
		byte lo = (byte) (c & 0xFF);
		return byteToHex(hi) + byteToHex(lo);
	}

	public static String printBytes(byte[] array, int start, int len) {
		String tmp = "";
		for (int k = start; k < len; k++) {
			tmp = tmp + byteToHex(array[k]) + " ";
		}
		return tmp;
	}

	public static String printBytes(byte[] array) {
		return printBytes(array, 0, array.length);
	}

	private static String getNameSeq(String name) {
		Calendar now = Calendar.getInstance();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
		nameSeq = nameSeq == 9 ? 0 : nameSeq + 1;
		return name + nameSeq + formatter.format(now.getTime());
	}

	private static String getHexCode(String singleChar) throws Exception {
		byte[] bytes = singleChar.getBytes(encoding);
		return Integer.toHexString(bytes[0] & 0xFF).toUpperCase();
	}

	private static String getTruncName(int id, String type, String name, int len)
			throws IOException {
		if ((name != null) && (name != "")) {
			name = name.toUpperCase();
			if (name.length() > len) {
				if (type.equals("COL"))
					db2TruncNameWriter.write(schemaName[id] + "."
							+ tableName[id] + " Column=" + name
							+ " truncated to " + name.substring(0, len)
							+ linesep);
				name = name.substring(0, len);
			}
		}
		return name;
	}

	private static void delDataset(String dsnName) throws Exception {
		if (ZFile.dsExists("//'" + dsnName + "'")) {
			ZFile.remove("//'" + dsnName + "'");
		}
	}

	private static void deleteFile(String fileSuffix) throws Exception {
		String tql = ZFile.getFullyQualifiedDSN("TBLDATA");

		CatalogSearch catSearch = new CatalogSearch(tql + ".**." + fileSuffix);
		catSearch.addFieldName("ENTNAME");
		catSearch.search();
		while (catSearch.hasNext()) {
			CatalogSearch.Entry entry = (CatalogSearch.Entry) catSearch.next();
			if (entry.isDatasetEntry()) {
				CatalogSearchField field = entry.getField("ENTNAME");
				String dsn = field.getFString().trim();
				delDataset(dsn);
				log("Deleted=" + dsn);
			}
		}
	}

	public static void cleanupPSFiles() throws Exception {
		deleteFile("CERR");
		deleteFile("LERR");
		deleteFile("DISC");
		deleteFile("UT1");
		deleteFile("OUT");
	}

	private static void assignDataFileFP(int i) throws IOException {
		String schema = removeQuote(schemaName[i].toLowerCase());
		String table = removeQuote(tableName[i].toLowerCase());
		if (dbTargetName.equals("db2luw")) {
			String fileName = IBMExtractUtilities.FixSpecialChars(schema + "_"
					+ table);
			if (multiTables[i] > 0)
				fileName = OUTPUT_DIR + "data" + filesep + fileName
						+ multiTables[i] + ".txt";
			else
				fileName = OUTPUT_DIR + "data" + filesep + fileName + ".txt";
			fp[i] = new DataOutputStream(new BufferedOutputStream(
					new FileOutputStream(fileName)));
		} else {
			zfp[i] = null;
			zOSDataSets[i] = null;
		}
	}

	private static void initDBSources() {
		int multCount = 0;
		ArrayList al = new ArrayList();

		String tmpDBName = dbName.toLowerCase();
		String driverName = "";
		String nocopypend = "true";
		String overallocStr = "1.3636";
		String secondaryStr = "0";

		String remoteLoadStr = "false";
		String compressTableStr = "false";
		String compressIndexStr = "false";
		String dstDB2ReleaseStr = "-1.0F";
		String extractPartitionsStr = "true";
		String useBestPracticeTSNamesStr = "true";
		String extractHashPartitionsStr = "false";
		String prevTable = "";
		String prevSchema = "";

		if ((!dbSourceName.equalsIgnoreCase("postgres"))
				&& (!dbSourceName.equalsIgnoreCase("oracle"))
				&& (!dbSourceName.equalsIgnoreCase("db2"))
				&& (!dbSourceName.equalsIgnoreCase("mysql"))
				&& (!dbSourceName.equalsIgnoreCase("sybase"))
				&& (!dbSourceName.equalsIgnoreCase("mssql"))
				&& (!dbSourceName.equalsIgnoreCase("zdb2"))
				&& (!dbSourceName.equalsIgnoreCase("access"))
				&& (!dbSourceName.equalsIgnoreCase("hxtt"))
				&& (!dbSourceName.equalsIgnoreCase("domino"))
				&& (!dbSourceName.equalsIgnoreCase("idb2"))) {
			log("Invalid dbSourceName supplied. Valid value can be one of these -> postgres oracle db2 mysql sybase mssql");
			System.exit(-1);
		}

		if (osType.equals("z/OS")) {
			dbTargetName = "zdb2";
		}
		log("Target DB       : " + dbTargetName);

		OUTPUT_DIR = System.getProperty("OUTPUT_DIR");
		if (OUTPUT_DIR == null)
			OUTPUT_DIR = "data";
		if (!OUTPUT_DIR.equalsIgnoreCase("data")) {
			if ((!OUTPUT_DIR.endsWith("\\")) && (!OUTPUT_DIR.endsWith("/"))) {
				OUTPUT_DIR += filesep;
			}
		}
		File tmpfile = new File(OUTPUT_DIR);
		tmpfile.mkdirs();
		try {
			log("OUTPUT_DIR is : " + tmpfile.getCanonicalPath());
		} catch (IOException e1) {
			log(e1.getMessage());
		}
		if (dataUnload) {
			new File(OUTPUT_DIR + "data").mkdirs();
			if (dbTargetName.equals("db2luw")) {
				new File(OUTPUT_DIR + "dump").mkdirs();
				new File(OUTPUT_DIR + "msg").mkdirs();
			}
		}

		propDatamap = new Properties();
		propParams = new Properties();
		try {
			String propFile = System.getProperty("IBMExtractPropFile");
			if ((propFile != null) && (!propFile.equals(""))) {
				PARAM_PROP_FILE = propFile;
			}
			InputStream istream = ClassLoader
					.getSystemResourceAsStream(PARAM_PROP_FILE);
			if (istream == null) {
				try {
					propParams.load(new FileInputStream(PARAM_PROP_FILE));
					log("Configuration file loaded: '" + PARAM_PROP_FILE + "'");
				} catch (Exception e) {
					log("Configuration file : '"
							+ PARAM_PROP_FILE
							+ "' not found. You need to run 'java -jar IBMDataMovementTool.jar' to create property file.");
					System.exit(-1);
				}
			} else {
				propParams.load(istream);
				log("Configuration file loaded: '" + PARAM_PROP_FILE + "'");
			}

			appJAR = propParams.getProperty("appJAR");
			String debugStr = propParams.getProperty("debug");
			String retainCol = propParams.getProperty("RetainColName");
			encoding = propParams.getProperty("encoding");
			String msgraphic = propParams.getProperty("graphic");
			String texttoclob = propParams.getProperty("mssqltexttoclob");
			String dumpfil = propParams.getProperty("dumpfile");
			String loadstat = propParams.getProperty("loadstats");
			db2dbname = propParams.getProperty("dstDBName");
			String customMappingStr = propParams.getProperty("customMapping");
			String putConnectStr = propParams
					.getProperty("putconnectstatement");
			String db2_compatibilityStr = propParams
					.getProperty("db2_compatibility");
			String roundDown_31Str = propParams.getProperty("roundDown_31");
			String dbclobStr = propParams.getProperty("dbclob");
			String trimTrailingSpacesStr = propParams
					.getProperty("trimTrailingSpaces");
			String regenerateTriggersStr = propParams
					.getProperty("regenerateTriggers");
			remoteLoadStr = propParams.getProperty("remoteLoad");
			compressTableStr = propParams.getProperty("compressTable");
			compressIndexStr = propParams.getProperty("compressIndex");
			extractPartitionsStr = propParams.getProperty("extractPartitions");
			extractHashPartitionsStr = propParams
					.getProperty("extractHashPartitions");
			String retainConstraintsNameStr = propParams
					.getProperty("retainConstraintsName");
			useBestPracticeTSNamesStr = propParams
					.getProperty("useBestPracticeTSNames");
			db2Instance = propParams.getProperty("dstDB2Instance");
			dstDB2ReleaseStr = propParams.getProperty("dstDB2Release");
			limitExtractRows = propParams.getProperty("limitExtractRows");
			limitLoadRows = propParams.getProperty("limitLoadRows");
			sourceSchema = propParams.getProperty("sourceSchema");

			if (extractHashPartitionsStr == null)
				extractHashPartitionsStr = "false";
			if (trimTrailingSpacesStr == null)
				trimTrailingSpacesStr = "false";
			if (retainConstraintsNameStr == null)
				retainConstraintsNameStr = "false";
			if (useBestPracticeTSNamesStr == null)
				useBestPracticeTSNamesStr = "true";
			if (limitExtractRows == null)
				limitExtractRows = "ALL";
			if (limitLoadRows == null) {
				limitLoadRows = "ALL";
			}
			String oracleNumberMappingStr = propParams
					.getProperty("oracleNumberMapping");
			if (oracleNumberMappingStr == null)
				oracleNumberMappingStr = "false";
			String oracleNumb31MappingStr = propParams
					.getProperty("oracleNumb31Mapping");
			if (oracleNumb31MappingStr == null) {
				oracleNumb31MappingStr = "false";
			}
			remoteLoad = Boolean.valueOf(remoteLoadStr).booleanValue();

			if (dbTargetName.equals("zdb2")) {
				zdb2tableseries = propParams.getProperty("zdb2tableseries");
				nocopypend = propParams.getProperty("znocopypend");
				overallocStr = propParams.getProperty("zoveralloc");
				secondaryStr = propParams.getProperty("zsecondary");
				storclas = propParams.getProperty("storclas");
			}
			if (remoteLoad) {
				dstUid = propParams.getProperty("dstUid");
				dstPwd = propParams.getProperty("dstPwd");
				if ((dstUid == null) || (dstPwd == null)) {
					log("Valid property dstUid or dstPwd not found inIBM.Extract.properties file.");
					System.exit(-1);
				}
			}

			if (db2_compatibilityStr == null) {
				db2_compatibilityStr = "false";
			}
			if (roundDown_31Str == null) {
				roundDown_31Str = "false";
			}
			if (dbclobStr == null) {
				dbclobStr = "false";
			}
			if (trimTrailingSpacesStr == null) {
				trimTrailingSpaces = true;
			}
			if ((debugStr == null) || (retainCol == null) || (encoding == null)
					|| (msgraphic == null) || (dumpfil == null)
					|| (loadstat == null) || (texttoclob == null)
					|| (db2dbname == null)) {
				log("Valid Property not found in IBMExtract.properties file. Valid values are "
						+ linesep
						+ "debug=false. If true, debug messages will be printed."
						+ linesep
						+ "RetainColName=true # [true|false] If true, tool will not truncate column name to 30 chars. "
						+ linesep
						+ "encoding=UTF-8 # [US-ASCII|ISO-8859-1|UTF-8|UTF-16BE|UTF-16LE|UTF-16] If true, tool will create utf-8 data files for extracted data. "
						+ linesep
						+ "graphic=false #[True|False] Treat NVARCHAR, NCHAR and NTEXT column as VARCHAR, CHAR and CLOB if false. "
						+ linesep
						+ "mssqltexttoclob=false #[False|True]Treat SQL Server TEXT as String and not CLOB while retrieving (Performance)."
						+ linesep
						+ "dumpfile=false. #[False|True] Create dumpfile option in LOAD Script if true."
						+ linesep
						+ "loadstats=false. #[False|True] Create STATISTICS option in LOAD Script if true."
						+ linesep
						+ "db2dbname=SAMPLE # Use this DB2DBNAME in scripts that are generated.");

				System.exit(-1);
			}
			if (dbTargetName.equals("zdb2")) {
				if ((zdb2tableseries == null) || (nocopypend == null)
						|| (overallocStr == null) || (secondaryStr == null)
						|| (storclas == null)) {
					log("zdb2tableseries=Q # Use this as a series name for zDB2 datasets to store LOAD data."
							+ linesep
							+ "znocopypend=[true|false] #If true, use NOCOPYPEND option in LOAD for zDB2"
							+ linesep
							+ "zoveralloc=1.3636 #The overAlloc variable specifies by how much we want to oversize our file allocation"
							+ linesep
							+ "zsecondary=0 #The secondary extents. Start with a default value of 0"
							+ linesep
							+ "storclas=none #Specify none if do not want to use storclas. Length can be from 1 to 8 only"
							+ linesep);

					System.exit(-1);
				}
			}

			debug = Boolean.valueOf(debugStr).booleanValue();
			retainColName = Boolean.valueOf(retainCol).booleanValue();
			encoding = encoding.toUpperCase();
			graphic = Boolean.valueOf(msgraphic).booleanValue();
			mssqltexttoclob = Boolean.valueOf(texttoclob).booleanValue();
			dumpfileb = Boolean.valueOf(dumpfil).booleanValue();
			loadstats = Boolean.valueOf(loadstat).booleanValue();
			db2dbname = db2dbname.toUpperCase();
			zdb2tableseries = zdb2tableseries.toUpperCase();
			znocopypend = Boolean.valueOf(nocopypend).booleanValue();
			customMapping = customMappingStr == null ? false : Boolean.valueOf(
					customMappingStr).booleanValue();
			putConnectStatement = putConnectStr == null ? true : Boolean
					.valueOf(putConnectStr).booleanValue();
			overAlloc = Double.parseDouble(overallocStr);
			secondary = Integer.parseInt(secondaryStr);
			db2_compatibility = Boolean.valueOf(db2_compatibilityStr)
					.booleanValue();
			sqlTerminator = db2_compatibility ? "/" : "@";
			roundDown_31 = Boolean.valueOf(roundDown_31Str).booleanValue();
			dbclob = Boolean.valueOf(dbclobStr).booleanValue();
			trimTrailingSpaces = Boolean.valueOf(trimTrailingSpacesStr)
					.booleanValue();
			regenerateTriggers = Boolean.valueOf(regenerateTriggersStr)
					.booleanValue();
			compressTable = Boolean.valueOf(compressTableStr).booleanValue();
			compressIndex = Boolean.valueOf(compressIndexStr).booleanValue();
			extractPartitions = Boolean.valueOf(extractPartitionsStr)
					.booleanValue();
			extractHashPartitions = Boolean.valueOf(extractHashPartitionsStr)
					.booleanValue();
			retainConstraintsName = Boolean.valueOf(retainConstraintsNameStr)
					.booleanValue();
			useBestPracticeTSNames = Boolean.valueOf(useBestPracticeTSNamesStr)
					.booleanValue();
			oracleNumberMapping = Boolean.valueOf(oracleNumberMappingStr)
					.booleanValue();
			oracleNumb31Mapping = Boolean.valueOf(oracleNumb31MappingStr)
					.booleanValue();

			if (!dbSourceName.equalsIgnoreCase("oracle")) {
				extractHashPartitions = false;
				extractPartitions = false;
				useBestPracticeTSNames = true;
			}

			try {
				releaseLevel = Float.parseFloat(dstDB2ReleaseStr);
			} catch (Exception e1) {
				releaseLevel = -1.0F;
			}

			if ((!encoding.equals("US-ASCII"))
					&& (!encoding.equals("ISO-8859-1"))
					&& (!encoding.equals("ISO-8859-8"))
					&& (!encoding.equals("UTF-16BE"))
					&& (!encoding.equals("UTF-16LE"))
					&& (!encoding.equals("UTF-16"))
					&& (!encoding.equals("UTF-8"))) {
				log("Invalid encoding specified: Valid values are: US-ASCII or ISO-8859-1 or UTF-8 or UTF-16BE or UTF-16LE or UTF-16 or ISO-8859-8");
				System.exit(-1);
			}

			log("debug                 : " + (debug ? "True" : "False"));
			log("Encoding              : " + encoding);
			log("RetainColName         : " + (retainColName ? "True" : "False"));
			log("graphic               : " + (graphic ? "True" : "False"));
			log("mssqltexttoclob       : "
					+ (mssqltexttoclob ? "True" : "False"));
			log("dumpfile              : " + (dumpfileb ? "True" : "False"));
			log("loadstats             : " + (loadstats ? "True" : "False"));
			log("db2dbname             : " + db2dbname);
			log("customMapping         : " + (customMapping ? "True" : "False"));
			log("putconnectstatement   : "
					+ (putConnectStatement ? "True" : "False"));
			log("db2_compatibility     : "
					+ (db2_compatibility ? "True" : "False"));
			log("roundDown_31          : " + (roundDown_31 ? "True" : "False"));
			log("dbclob                : " + (dbclob ? "True" : "False"));
			log("trimTrailingSpaces    : "
					+ (trimTrailingSpaces ? "True" : "False"));
			log("remoteLoad            : " + (remoteLoad ? "True" : "False"));
			log("compressTable         : " + (compressTable ? "True" : "False"));
			log("compressIndex         : " + (compressIndex ? "True" : "False"));
			log("extractPartitions     : "
					+ (extractPartitions ? "True" : "False"));
			log("extractHashPartitions : "
					+ (extractHashPartitions ? "True" : "False"));
			log("retainConstraintsName : "
					+ (retainConstraintsName ? "True" : "False"));
			log("useBestPracticeTSNames: "
					+ (useBestPracticeTSNames ? "True" : "False"));
			log("limitExtractRows      : " + limitExtractRows);
			log("limitLoadRows         : " + limitLoadRows);
			if ((db2Instance != null) && (!db2Instance.equals(""))
					&& (!db2Instance.equals("null")))
				log("DB2 Instance name     : " + db2Instance);
			if (releaseLevel != -1.0F)
				log("DB2 Release         : " + releaseLevel);
			if (dbTargetName.equals("zdb2")) {
				log("zdb2tableseries       : " + zdb2tableseries);
				log("znocopypend           : "
						+ (znocopypend ? "true" : "false"));
				log("zoveralloc            : " + overallocStr);
				log("zsecondary            : " + secondaryStr);
				log("storclas              : " + storclas);
				if (storclas.length() > 8) {
					log("Length of storclas can  not be greater than 8.");
					System.exit(-1);
				}
			}
			String line;
			try {
				LineNumberReader in = new LineNumberReader(new FileReader(
						TABLES_PROP_FILE));
				while ((line = in.readLine()) != null) {
					if ((line.trim().equals("")) || (line.startsWith("#")))
						continue;
					al.add(line);
				}
				Collections.sort(al);
			} catch (FileNotFoundException e) {
				log("Configuration file '" + TABLES_PROP_FILE
						+ "' not found. Existing ...");
				System.exit(-1);
			} catch (Exception ex) {
				log("Error reading configuration file '" + TABLES_PROP_FILE
						+ "'");
				System.exit(-1);
			}

			if (al.size() == 0) {
				log("It looks that the " + TABLES_PROP_FILE + " is empty.");
				log("This may happen due to 2 possible reasons");
				log("1. There are no tables in the source schema that you selected. Source Schema Name = "
						+ sourceSchema);
				log("2. If you copied this file from Windows to Unix, there might be ^M chars in the file.");
				log("   If that is the case, you might need to Run dos2unix command on "
						+ TABLES_PROP_FILE + ". Exiting ...");
				System.exit(-1);
			}
			totalTables = al.size();

			log("Configuration file loaded: '" + TABLES_PROP_FILE + "'");
			if (totalTables < threads) {
				threads = totalTables;
			}
			query = new String[totalTables];
			schemaName = new String[totalTables];
			tableName = new String[totalTables];
			srcTableName = new String[totalTables];
			srcSchName = new String[totalTables];
			countSQL = new String[totalTables];
			zOSDataSets = new String[totalTables];
			multiTables = new int[totalTables];

			if (dbTargetName.equals("db2luw")) {
				fp = new DataOutputStream[totalTables];

				lobsToFiles = false;
			} else {
				log("ZUtil.getDefaultPlatformEncoding(): "
						+ ZUtil.getDefaultPlatformEncoding());
				log("Default file.encoding:"
						+ System.getProperty("file.encoding"));

				lobsToFiles = true;
				zfp = new ZFile[totalTables];
				try {
					cleanupPSFiles();
				} catch (Exception e) {
					log("Problem with deleting the PS datasets ");
					e.printStackTrace();
				}

			}

			log("query size " + query.length + " schemaName size = "
					+ schemaName.length);

			for (int i = 0; i < totalTables; i++) {
				query[i] = new String();
				schemaName[i] = new String();
				tableName[i] = new String();
				srcTableName[i] = new String();
				srcSchName[i] = new String();
				countSQL[i] = new String();
				multiTables[i] = 0;
			}

			int i = 0;
			Iterator itr = al.iterator();
			while (itr.hasNext()) {
				line = (String) itr.next();
				String values = line.substring(0, line.indexOf(":"));
				schemaName[i] = values.substring(0, values.indexOf("."));
				query[i] = line.substring(line.indexOf(":") + 1);
				String tmpStr = query[i].toUpperCase();
				tableName[i] = values.substring(values.indexOf(".") + 1);
				srcSchName[i] = query[i].substring(tmpStr.indexOf("FROM ") + 5);
				srcTableName[i] = srcSchName[i].substring(srcSchName[i]
						.indexOf(".") + 1);
				if (i > 0) {
					if ((prevSchema.equalsIgnoreCase(schemaName[i]))
							&& (prevTable.equalsIgnoreCase(tableName[i]))) {
						multCount++;
						multiTables[i] = multCount;
					} else {
						multCount = 0;
					}
				}
				int fromPos = srcSchName[i].indexOf(".");
				if (fromPos > 0) {
					srcSchName[i] = srcSchName[i].substring(0, fromPos);
					if ((dbSourceName.equalsIgnoreCase("mssql"))
							|| (dbSourceName.equalsIgnoreCase("zdb2"))
							|| (dbSourceName.equalsIgnoreCase("db2"))
							|| (dbSourceName.equalsIgnoreCase("sybase"))) {
						countSQL[i] = ("SELECT COUNT_BIG(*) FROM "
								+ srcSchName[i] + "." + tableName[i]);
					} else
						countSQL[i] = ("SELECT COUNT(*) FROM " + srcSchName[i]
								+ "." + tableName[i]);
				} else {
					srcSchName[i] = schemaName[i];
					if ((dbSourceName.equalsIgnoreCase("mssql"))
							|| (dbSourceName.equalsIgnoreCase("zdb2"))
							|| (dbSourceName.equalsIgnoreCase("db2"))
							|| (dbSourceName.equalsIgnoreCase("sybase"))) {
						countSQL[i] = ("SELECT COUNT_BIG(*) FROM " + tableName[i]);
					} else
						countSQL[i] = ("SELECT COUNT(*) FROM " + tableName[i]);
				}

				if (dataUnload) {
					assignDataFileFP(i);
				}
				prevSchema = schemaName[i];
				prevTable = tableName[i];
				i++;
			}

			if (dataUnload) {
				db2LoadWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR
						+ "db2load.sql", false));
				IBMExtractUtilities.putHelpInformation(db2LoadWriter,
						"db2load.sql");
				db2RunstatWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR
						+ "db2runstats.sql", false));
				IBMExtractUtilities.putHelpInformation(db2RunstatWriter,
						"db2runstats.sql");
				db2TabStatusWriter = new BufferedWriter(new FileWriter(
						OUTPUT_DIR + "db2tabstatus.sql", false));
				IBMExtractUtilities.putHelpInformation(db2TabStatusWriter,
						"db2tabstatus.sql");
				db2TabCountWriter = new BufferedWriter(new FileWriter(
						OUTPUT_DIR + "db2tabcount.sql", false));
				IBMExtractUtilities.putHelpInformation(db2TabCountWriter,
						"db2tabcount.sql");
				db2LoadTerminateWriter = new BufferedWriter(new FileWriter(
						OUTPUT_DIR + "db2loadterminate.db2", false));
				IBMExtractUtilities.putHelpInformation(db2LoadTerminateWriter,
						"db2loadterminate.db2");
				db2CheckPendingWriter = new BufferedWriter(new FileWriter(
						OUTPUT_DIR + "db2checkpending.sql", false));
				IBMExtractUtilities.putHelpInformation(db2CheckPendingWriter,
						"db2checkpending.sql");
				String addstr = "";
				if ((dbTargetName.equals("zdb2"))
						|| ((dbTargetName.equals("db2luw")) && (!remoteLoad))) {
					if (db2dbname.equals("SAMPLE")) {
						if (putConnectStatement) {
							addstr = "-- TO CHANGE DATABASE NAME, modify IBMExtract.properties file."
									+ linesep;
							db2LoadWriter.write(addstr);
							db2RunstatWriter.write(addstr);
							db2TabStatusWriter.write(addstr);
							db2TabCountWriter.write(addstr);
							db2LoadTerminateWriter.write(addstr);
							db2CheckPendingWriter.write(addstr);
						}
					}
					if (putConnectStatement) {
						db2LoadWriter.write("CONNECT TO " + db2dbname + ";"
								+ linesep);
						db2RunstatWriter.write("CONNECT TO " + db2dbname + ";"
								+ linesep);
						db2TabStatusWriter.write("CONNECT TO " + db2dbname
								+ ";" + linesep);
						db2TabCountWriter.write("CONNECT TO " + db2dbname + ";"
								+ linesep);
						db2LoadTerminateWriter.write("CONNECT TO " + db2dbname
								+ ";" + linesep);
						db2CheckPendingWriter.write("CONNECT TO " + db2dbname
								+ ";" + linesep);
					}
				}
			}
			if (ddlGen) {
				db2TablesWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR
						+ "db2tables.sql", false));
				IBMExtractUtilities.putHelpInformation(db2TablesWriter,
						"db2tables.sql");
				db2FKWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR
						+ "db2fkeys.sql", false));
				IBMExtractUtilities.putHelpInformation(db2FKWriter,
						"db2fkeys.sql");
				db2DropWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR
						+ "db2droptables.sql", false));
				IBMExtractUtilities.putHelpInformation(db2DropWriter,
						"db2droptables.sql");
				db2ConsWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR
						+ "db2cons.sql", false));
				IBMExtractUtilities.putHelpInformation(db2ConsWriter,
						"db2cons.sql");
				db2FKDropWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR
						+ "db2dropfkeys.sql", false));
				IBMExtractUtilities.putHelpInformation(db2FKDropWriter,
						"db2dropfkeys.sql");
				db2CheckWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR
						+ "db2check.sql", false));
				IBMExtractUtilities.putHelpInformation(db2CheckWriter,
						"db2check.sql");
				db2UniqWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR
						+ "db2uniq.sql", false));
				IBMExtractUtilities.putHelpInformation(db2UniqWriter,
						"db2uniq.sql");
				db2SeqWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR
						+ "db2sequences.sql", false));
				IBMExtractUtilities.putHelpInformation(db2SeqWriter,
						"db2sequences.sql");
				db2DropSeqWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR
						+ "db2dropsequences.sql", false));
				IBMExtractUtilities.putHelpInformation(db2DropSeqWriter,
						"db2dropsequences.sql");
				db2ViewsWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR
						+ "db2views.db2", false));
				IBMExtractUtilities.putHelpInformation(db2ViewsWriter,
						"db2views.sql");
				db2DropObjectsWriter = new BufferedWriter(new FileWriter(
						OUTPUT_DIR + "db2dropobjects.sql", false));
				IBMExtractUtilities.putHelpInformation(db2DropObjectsWriter,
						"db2dropobjects.sql");
				if ((dbSourceName.equals("oracle"))
						|| (dbSourceName.equals("db2"))) {
					db2SynonymWriter = new BufferedWriter(new FileWriter(
							OUTPUT_DIR + "db2synonyms.db2", false));
					IBMExtractUtilities.putHelpInformation(db2SynonymWriter,
							"db2synonyms.db2");
					db2DropSynWriter = new BufferedWriter(new FileWriter(
							OUTPUT_DIR + "db2dropsynonyms.sql", false));
					IBMExtractUtilities.putHelpInformation(db2DropSynWriter,
							"db2dropsynonyms.sql");
					db2mviewWriter = new BufferedWriter(new FileWriter(
							OUTPUT_DIR + "db2mviews.db2", false));
					IBMExtractUtilities.putHelpInformation(db2mviewWriter,
							"db2mviews.db2");
				}
				if (dbSourceName.equals("oracle")) {
					db2rolePrivsWriter = new BufferedWriter(new FileWriter(
							OUTPUT_DIR + "db2roleprivs.db2", false));
					IBMExtractUtilities.putHelpInformation(db2rolePrivsWriter,
							"db2roleprivs.db2");
					db2objPrivsWriter = new BufferedWriter(new FileWriter(
							OUTPUT_DIR + "db2objprivs.db2", false));
					IBMExtractUtilities.putHelpInformation(db2objPrivsWriter,
							"db2objprivs.db2");
				}
				db2udfWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR
						+ "db2udf.sql", false));
				IBMExtractUtilities.putHelpInformation(db2udfWriter,
						"db2udf.sql");
				db2tsbpWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR
						+ "db2tsbp.sql", false));
				IBMExtractUtilities.putHelpInformation(db2tsbpWriter,
						"db2tsbp.sql");
				db2droptsbpWriter = new BufferedWriter(new FileWriter(
						OUTPUT_DIR + "db2droptsbp.sql", false));
				IBMExtractUtilities.putHelpInformation(db2droptsbpWriter,
						"db2droptsbp.sql");
				db2DefaultWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR
						+ "db2default.sql", false));
				IBMExtractUtilities.putHelpInformation(db2DefaultWriter,
						"db2default.sql");
				db2TruncNameWriter = new BufferedWriter(new FileWriter(
						OUTPUT_DIR + "db2TruncName.txt", false));
				IBMExtractUtilities.putHelpInformation(db2TruncNameWriter,
						"db2TruncName.txt");
				String addstr = "";
				if ((dbTargetName.equals("zdb2"))
						|| ((dbTargetName.equals("db2luw")) && (!remoteLoad))) {
					if (db2dbname.equals("SAMPLE")) {
						addstr = "-- TO CHANGE DATABASE NAME, modify IBMExtract.properties file."
								+ linesep;
						if (putConnectStatement) {
							db2TablesWriter.write(addstr);
							db2FKWriter.write(addstr);
							db2DropWriter.write(addstr);
							db2ConsWriter.write(addstr);
							db2FKDropWriter.write(addstr);
							db2CheckWriter.write(addstr);
							db2UniqWriter.write(addstr);
							db2SeqWriter.write(addstr);
							db2DropSeqWriter.write(addstr);
							db2ViewsWriter.write(addstr);
							db2DropObjectsWriter.write(addstr);
							if ((dbSourceName.equals("oracle"))
									|| (dbSourceName.equals("db2"))) {
								db2SynonymWriter.write(addstr);
								db2DropSynWriter.write(addstr);
								db2mviewWriter.write(addstr);
							}
							if (dbSourceName.equals("oracle")) {
								db2rolePrivsWriter.write(addstr);
								db2objPrivsWriter.write(addstr);
							}
							db2udfWriter.write(addstr);
							db2tsbpWriter.write(addstr);
							db2droptsbpWriter.write(addstr);
							db2DefaultWriter.write(addstr);
						}
					}
					if (putConnectStatement) {
						db2TablesWriter.write("CONNECT TO " + db2dbname + ";"
								+ linesep);
						db2FKWriter.write("CONNECT TO " + db2dbname + ";"
								+ linesep);
						db2DropWriter.write("CONNECT TO " + db2dbname + ";"
								+ linesep);
						db2ConsWriter.write("CONNECT TO " + db2dbname + ";"
								+ linesep);
						db2FKDropWriter.write("CONNECT TO " + db2dbname + ";"
								+ linesep);
						db2CheckWriter.write("CONNECT TO " + db2dbname + ";"
								+ linesep);
						db2UniqWriter.write("CONNECT TO " + db2dbname + ";"
								+ linesep);
						db2SeqWriter.write("CONNECT TO " + db2dbname + ";"
								+ linesep);
						db2DropSeqWriter.write("CONNECT TO " + db2dbname + ";"
								+ linesep);
						db2ViewsWriter.write("--#SET TERMINATOR "
								+ sqlTerminator + linesep);
						db2ViewsWriter.write("CONNECT TO " + db2dbname
								+ linesep);
						db2ViewsWriter.write(sqlTerminator + linesep);
						db2DropObjectsWriter.write("CONNECT TO " + db2dbname
								+ ";" + linesep);
						if ((dbSourceName.equals("oracle"))
								|| (dbSourceName.equals("db2"))) {
							db2SynonymWriter.write("CONNECT TO " + db2dbname
									+ ";" + linesep);
							db2DropSynWriter.write("CONNECT TO " + db2dbname
									+ ";" + linesep);
							db2mviewWriter.write("--#SET TERMINATOR "
									+ sqlTerminator + linesep);
							db2mviewWriter.write("CONNECT TO " + db2dbname
									+ linesep);
							db2mviewWriter.write(sqlTerminator + linesep);
						}
						if (dbSourceName.equals("oracle")) {
							db2rolePrivsWriter.write("CONNECT TO " + db2dbname
									+ ";" + linesep);
							db2objPrivsWriter.write("CONNECT TO " + db2dbname
									+ ";" + linesep);
						}
						db2udfWriter.write("CONNECT TO " + db2dbname + ";"
								+ linesep);
						db2tsbpWriter.write("CONNECT TO " + db2dbname + ";"
								+ linesep);
						db2droptsbpWriter.write("CONNECT TO " + db2dbname + ";"
								+ linesep);
						db2DefaultWriter.write("CONNECT TO " + db2dbname + ";"
								+ linesep);
					}
				}
			}
			String tmpFileName = IBMExtractUtilities.db2ScriptName(OUTPUT_DIR,
					ddlGen, dataUnload);
			if (osType.equals("WIN")) {
				db2ScriptWriter = new BufferedWriter(new FileWriter(
						tmpFileName, false));
				db2CheckScriptWriter = new BufferedWriter(new FileWriter(
						OUTPUT_DIR + "db2checkRemoval.cmd", false));
				db2DropScriptWriter = new BufferedWriter(new FileWriter(
						OUTPUT_DIR + "db2dropobjects.cmd", false));
			} else {
				Runtime rt = Runtime.getRuntime();
				db2ScriptWriter = new BufferedWriter(new FileWriter(
						tmpFileName, false));
				db2CheckScriptWriter = new BufferedWriter(new FileWriter(
						OUTPUT_DIR + "db2checkRemoval.sh", false));
				db2DropScriptWriter = new BufferedWriter(new FileWriter(
						OUTPUT_DIR + "db2dropobjects.sh", false));
				rt.exec("chmod 755 " + tmpFileName);
				rt.exec("chmod 755 " + OUTPUT_DIR + "db2checkRemoval.sh");
				rt.exec("chmod 755 " + OUTPUT_DIR + "db2dropobjects.sh");
			}

			istream = ClassLoader.getSystemResourceAsStream(DATAMAP_PROP_FILE);
			if (istream == null) {
				propDatamap.load(new FileInputStream(DATAMAP_PROP_FILE));
			} else {
				propDatamap.load(istream);
			}
			log("Configuration file loaded: '" + DATAMAP_PROP_FILE + "'");
		} catch (IOException ex) {
			log("exception loading properties: " + ex);
			System.exit(-1);
		}

		try {
			driverName = IBMExtractUtilities.getDriverName(dbSourceName);
			Class.forName(driverName).newInstance();
			log("Driver " + driverName + " loaded");
			String url = IBMExtractUtilities.getURL(dbSourceName, server, port,
					dbName);
			if (dbSourceName.equalsIgnoreCase("domino")) {
				mainConn = DriverManager.getConnection(url);
			} else {
				mainConn = DriverManager.getConnection(url, login);
				mainConn.setAutoCommit(autoCommit);
			}
			DatabaseMetaData md = mainConn.getMetaData();
			try {
				log("Database Product Name :" + md.getDatabaseProductName());
			} catch (Exception e) {
			}
			try {
				log("Database Product Version :"
						+ md.getDatabaseProductVersion());
			} catch (Exception e) {
			}
			try {
				log("JDBC driver " + md.getDriverName() + " Version = "
						+ md.getDriverVersion());
			} catch (Exception e) {
			}
			try {
				majorSourceDBVersion = md.getDatabaseMajorVersion();
				log("Database Major Version :" + majorSourceDBVersion);
			} catch (Exception e) {
			}
			try {
				minorSourceDBVersion = md.getDatabaseMinorVersion();
				log("Database Minor Version :" + minorSourceDBVersion);
			} catch (Exception e) {
			}
			if (!autoCommit)
				mainConn.commit();
		} catch (Exception e) {
			log("Error for : " + driverName + " for " + dbSourceName
					+ " Error Message :" + e.getMessage());
			System.exit(-1);
		}
	}

	public GenerateExtract() {
		initDBSources();

		blades = new BladeRunner[threads];

		synchronized (blades) {
			this.connPool = new Connection[threads];

			if (blades[0] == null) {
				for (int i = 0; i < threads; i++) {
					Connection conn = this.connPool[(i % 1)];
					blades[i] = new BladeRunner(i, conn);
				}
			}
		}
	}

	private void genDB2TSBP() {
		try {
			db2tsbpWriter.write("-- **** Best practice recommendations *** "
					+ linesep + linesep);
			db2tsbpWriter
					.write("-- Create your DB2 database with a PAGESIZE of 32K and enable AUTOMATIC STORAGE by using storage paths."
							+ linesep);
			db2tsbpWriter
					.write("-- (Windows) db2 create db testdb automatic storage yes on C:,D: DBPATH ON E: PAGESIZE 32 K"
							+ linesep);
			db2tsbpWriter
					.write("-- (Unix)    db2 create db testdb automatic storage yes on /db2data1,/db2data2,/db2data3 DBPATH ON /db2system PAGESIZE 32 K"
							+ linesep + linesep);

			db2tsbpWriter.write("--#SET :BPTS:BUFFERPOOL:BP8" + linesep);
			db2tsbpWriter
					.write("create bufferpool bp8 size automatic pagesize 8K"
							+ linesep + ";" + linesep);
			db2tsbpWriter.write("--#SET :BPTS:BUFFERPOOL:BP32" + linesep);
			db2tsbpWriter
					.write("create bufferpool bp32 size automatic pagesize 32K"
							+ linesep + ";" + linesep);
			db2tsbpWriter.write("--#SET :BPTS:BUFFERPOOL:BPU8" + linesep);
			db2tsbpWriter
					.write("create bufferpool bpu8 size automatic pagesize 8K"
							+ linesep + ";" + linesep);
			db2tsbpWriter.write("--#SET :BPTS:BUFFERPOOL:BPU32" + linesep);
			db2tsbpWriter
					.write("create bufferpool bpu32 size automatic pagesize 32K"
							+ linesep + ";" + linesep);

			db2tsbpWriter.write("--#SET :BPTS:TABLESPACE:TSU8" + linesep);
			db2tsbpWriter
					.write("create user temporary tablespace tsu8 pagesize 8K managed by automatic storage bufferpool bpu8"
							+ linesep + ";" + linesep + linesep);
			db2tsbpWriter.write("--#SET :BPTS:TABLESPACE:TSUTMP32" + linesep);
			db2tsbpWriter
					.write("create user temporary tablespace tsutmp32 pagesize 32K managed by automatic storage bufferpool bpu32"
							+ linesep + ";" + linesep);
			db2tsbpWriter.write("--#SET :BPTS:TABLESPACE:TSSTMP32" + linesep);
			db2tsbpWriter
					.write("create system temporary tablespace tsstmp32 pagesize 32K managed by automatic storage bufferpool bpu32"
							+ linesep + ";" + linesep + linesep);

			if (useBestPracticeTSNames) {
				db2tsbpWriter
						.write("-- **** Best practice recommendations *** "
								+ linesep + linesep);

				db2tsbpWriter.write("--#SET :BPTS:TABLESPACE:TS8" + linesep);
				db2tsbpWriter
						.write("create tablespace ts8 pagesize 8k bufferpool bp8"
								+ linesep + ";" + linesep + linesep);
				db2tsbpWriter.write("--#SET :BPTS:TABLESPACE:TS32" + linesep);
				db2tsbpWriter
						.write("create tablespace ts32 pagesize 32k bufferpool bp32"
								+ linesep + ";" + linesep + linesep);

				db2droptsbpWriter.write("DROP TABLESPACE TS32;" + linesep);
				db2droptsbpWriter.write("DROP TABLESPACE TS8;" + linesep);
			} else {
				String sql = "";
				PreparedStatement statement = null;
				ResultSet rs1 = null;

				sql = "SELECT DECODE(TABLESPACE_NAME,'SYSTEM','DB2SYSTEM',TABLESPACE_NAME) FROM DBA_TABLES WHERE TABLESPACE_NAME = 'SYSTEM' OR TABLESPACE_NAME NOT LIKE 'SYS%' UNION SELECT DECODE(TABLESPACE_NAME,'SYSTEM','DB2SYSTEM',TABLESPACE_NAME) FROM DBA_INDEXES WHERE TABLESPACE_NAME = 'SYSTEM' OR TABLESPACE_NAME NOT LIKE 'SYS%'";
				try {
					statement = mainConn.prepareStatement(sql);
					rs1 = statement.executeQuery();
					while (rs1.next()) {
						String tsName = rs1.getString(1);
						db2tsbpWriter.write("--#SET :BPTS:TABLESPACE:" + tsName
								+ linesep);
						db2tsbpWriter.write("create tablespace " + tsName
								+ " pagesize 32k bufferpool bp32" + linesep
								+ ";" + linesep + linesep);
						db2droptsbpWriter.write("DROP TABLESPACE " + tsName
								+ ";" + linesep);
					}
					if (rs1 != null)
						rs1.close();
					if (statement != null)
						statement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

			db2droptsbpWriter.write("DROP TABLESPACE TSSTMP32;" + linesep);
			db2droptsbpWriter.write("DROP TABLESPACE TSUTMP32;" + linesep);
			db2droptsbpWriter.write("DROP TABLESPACE TSU8;" + linesep);

			db2droptsbpWriter.write("DROP BUFFERPOOL BPU32;" + linesep);
			db2droptsbpWriter.write("DROP BUFFERPOOL BPU8;" + linesep);
			db2droptsbpWriter.write("DROP BUFFERPOOL BP32;" + linesep);
			db2droptsbpWriter.write("DROP BUFFERPOOL BP8;" + linesep);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private String executeSQL(String sql) {
		return executeSQL(sql, true);
	}

	private String executeSQL(String sql, boolean terminator) {
		int count = 0;
		StringBuffer sb = new StringBuffer();

		ResultSet Reader = null;
		String tok = terminator ? linesep + sqlTerminator + linesep : "~";
		try {
			PreparedStatement queryStatement = mainConn.prepareStatement(sql);
			Reader = queryStatement.executeQuery();
			while (Reader.next()) {
				if (count > 0)
					sb.append(tok);
				sb.append(Reader.getString(1));
				count++;
			}
			if ((count == 1) && (terminator))
				sb.append(linesep + sqlTerminator + linesep);
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return sb.toString();
	}

	private void genSynonyms(String schema) throws SQLException, IOException {
		String sql = "";
		String sql2 = "";

		ResultSet Reader = null;
		ResultSet Reader2 = null;

		if (dbSourceName.equalsIgnoreCase("oracle")) {
			sql = "SELECT O.OBJECT_NAME, O.OBJECT_TYPE FROM DBA_OBJECTS O WHERE O.OWNER = '"
					+ schema
					+ "' "
					+ "AND O.OBJECT_TYPE IN ('TABLE','VIEW','SEQUENCE','PACKAGE BODY') ";
		}

		if (sql.equals(""))
			return;

		PreparedStatement queryStatement = mainConn.prepareStatement(sql);
		queryStatement.setFetchSize(fetchSize);
		Reader = queryStatement.executeQuery();
		int objCount = 0;
		while (Reader.next()) {
			String table_name = Reader.getString(1);
			String object_type = Reader.getString(2);
			String qualifier;
			if (object_type.equalsIgnoreCase("PACKAGE BODY")) {
				qualifier = " MODULE ";
			} else {
				if (object_type.equalsIgnoreCase("SEQUENCE"))
					qualifier = " SEQUENCE ";
				else
					qualifier = "";
			}
			sql2 = "SELECT S.OWNER, S.SYNONYM_NAME, S.TABLE_OWNER FROM DBA_SYNONYMS S WHERE S.TABLE_OWNER = '"
					+ schema
					+ "' "
					+ "AND S.OWNER = 'PUBLIC' "
					+ "AND S.DB_LINK IS NULL "
					+ "AND S.TABLE_NAME = '"
					+ table_name + "' ";

			PreparedStatement queryStatement2 = mainConn.prepareStatement(sql2);
			queryStatement2.setFetchSize(fetchSize);
			Reader2 = queryStatement2.executeQuery();
			while (Reader2.next()) {
				String owner = Reader2.getString(1);
				String synonym_name = Reader2.getString(2);
				String table_owner = Reader2.getString(3);
				String synSchema;
				if (owner.equals("PUBLIC"))
					synSchema = "\"";
				else
					synSchema = "\"" + getSrctoDstSchema(schema) + "\".\"";
				String dstSchema = getSrctoDstSchema(table_owner);
				String queryOutput = "CREATE "
						+ (owner.equals("PUBLIC") ? "PUBLIC " : "")
						+ "SYNONYM " + synSchema + synonym_name + "\" FOR "
						+ qualifier + putQuote(dstSchema) + "."
						+ putQuote(table_name) + ";";

				if ((objCount > 0) && (objCount % 20 == 0))
					log(objCount + " numbers of synonyms extracted for schema "
							+ schema);
				objCount++;
				db2SynonymWriter.write(queryOutput + linesep);
				db2DropSynWriter.write("DROP PUBLIC SYNONYM " + synSchema
						+ synonym_name + "\";" + linesep);
			}
			if (Reader2 != null)
				Reader2.close();
			if (queryStatement2 != null)
				queryStatement2.close();
		}
		if (objCount > 0)
			log(objCount
					+ " Total numbers of public synonyms extracted for schema "
					+ schema);
		if (Reader != null)
			Reader.close();
		if (queryStatement != null)
			queryStatement.close();
	}

	private void genPrivs(String schema) throws SQLException, IOException {
		String sql = "";
		String dstSchema = getSrctoDstSchema(schema);

		ResultSet Reader = null;

		if (dbSourceName.equalsIgnoreCase("oracle")) {
			sql = "SELECT PRIVILEGE, TABLE_NAME, GRANTABLE FROM DBA_TAB_PRIVS WHERE GRANTEE = '&schemaname&' AND PRIVILEGE IN ('ALTER','INSERT','UPDATE','SELECT','DELETE','INDEX','REFERENCES')";
		}

		if (sql.equals(""))
			return;

		db2objPrivsWriter.write("-- Grants listed below are for GRANTEE "
				+ schema + linesep);
		sql = sql.replace("&schemaname&", schema);

		PreparedStatement queryStatement = mainConn.prepareStatement(sql);
		queryStatement.setFetchSize(fetchSize);
		Reader = queryStatement.executeQuery();
		int objCount = 0;
		while (Reader.next()) {
			String privilege = Reader.getString(1);
			String table_name = Reader.getString(2);
			String grantable = Reader.getString(3);
			String queryOutput = "GRANT " + privilege + " ON \"" + dstSchema
					+ "\".\"" + table_name + "\" TO USER \"" + dstSchema + "\""
					+ (grantable.equals("YES") ? " WITH GRANT OPTION" : "")
					+ ";";
			db2objPrivsWriter.write(queryOutput + linesep);
			objCount++;
		}
		if (Reader != null)
			Reader.close();
		if (queryStatement != null) {
			queryStatement.close();
		}
		sql = "SELECT PRIVILEGE, TABLE_NAME, COLUMN_NAME, GRANTABLE  FROM DBA_COL_PRIVS WHERE GRANTEE = '&schemaname&' AND PRIVILEGE IN ('ALTER','INSERT','UPDATE','SELECT','DELETE','INDEX','REFERENCES')";

		db2objPrivsWriter.write(linesep);

		sql = sql.replace("&schemaname&", schema);

		queryStatement = mainConn.prepareStatement(sql);
		queryStatement.setFetchSize(fetchSize);
		Reader = queryStatement.executeQuery();
		while (Reader.next()) {
			String privilege = Reader.getString(1);
			String table_name = Reader.getString(2);
			String column_name = Reader.getString(3);
			String grantable = Reader.getString(4);
			String queryOutput = "GRANT " + privilege + "(\"" + column_name
					+ "\") ON \"" + dstSchema + "\".\"" + table_name
					+ "\" TO USER \"" + dstSchema + "\""
					+ (grantable.equals("YES") ? " WITH GRANT OPTION" : "")
					+ ";";
			objCount++;
			db2objPrivsWriter.write(queryOutput + linesep);
		}
		if (objCount > 0)
			log(objCount + " numbers of privileges extracted for schema "
					+ schema);
		if (Reader != null)
			Reader.close();
		if (queryStatement != null)
			queryStatement.close();
	}

	private String getTriggerUpdateColumnsList(String schemaTrigger,
			String triggerName) {
		String sql = "";
		String columnList = "";

		ResultSet Reader = null;

		if (dbSourceName.equalsIgnoreCase("idb2")) {
			sql = "SELECT COLUMN_NAME FROM " + putQuote(schemaTrigger)
					+ ".SYSTRIGCOL " + "WHERE TRIGGER_SCHEMA = '"
					+ schemaTrigger + "' " + "AND TRIGGER_NAME = '"
					+ triggerName + "'";
		}

		if (sql.equals(""))
			return "";

		try {
			PreparedStatement queryStatement = mainConn.prepareStatement(sql);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			int objCount = 0;
			while (Reader.next()) {
				if (objCount == 0) {
					columnList = putQuote(Reader.getString(1)) + " ";
				} else {
					columnList = columnList + ","
							+ putQuote(Reader.getString(1)) + " ";
				}
				objCount++;
			}
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (Exception e) {
			log("Error in getting trigger columns for " + triggerName);
			e.printStackTrace();
		}
		return " OF " + columnList;
	}

	private String getTriggerSource(String schemaTrigger, String triggerName) {
		String sql = "";
		String dstSchema = getSrctoDstSchema(schemaTrigger);

		ResultSet Reader = null;
		StringBuffer buffer = new StringBuffer();

		if (dbSourceName.equalsIgnoreCase("idb2")) {
			sql = "select action_timing, trigger_mode, action_orientation, action_reference_old_row, action_reference_new_row, action_reference_old_table, action_reference_new_table, action_condition, event_object_schema, event_object_table, event_manipulation, action_statement from "
					+ putQuote(schemaTrigger)
					+ ".systriggers "
					+ "where trigger_schema = '"
					+ schemaTrigger
					+ "' "
					+ "and trigger_name = '" + triggerName + "'";
		} else if (dbSourceName.equalsIgnoreCase("oracle")) {
			sql = "select description, trigger_body from dba_triggers where owner = '"
					+ schemaTrigger
					+ "' "
					+ "and trigger_name = '"
					+ triggerName + "'";
		}

		if (sql.equals(""))
			return "";

		try {
			PreparedStatement queryStatement = mainConn.prepareStatement(sql);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			while (Reader.next()) {
				buffer.append("SET CURRENT SCHEMA = '" + dstSchema + "'"
						+ linesep + sqlTerminator + linesep);
				buffer.append("SET PATH = SYSTEM PATH, " + putQuote(dstSchema)
						+ linesep + sqlTerminator + linesep);
				buffer.append("--#SET :TRIGGER:" + dstSchema + ":"
						+ triggerName + linesep);
				if (dbSourceName.equalsIgnoreCase("idb2")) {
					String actionTiming = "";
					String triggerMode = "";
					String actionOrientation = "";
					String actionReferenceOldRow = "";
					String actionReferenceNewRow = "";
					String actionReferenceOldTable = "";
					String actionReferenceNewTable = "";
					String actionCondition = "";
					String eventObjectSchema = "";
					String eventObjectTable = "";
					String actionStatement = "";
					boolean ref = false;

					buffer.append("CREATE TRIGGER " + putQuote(dstSchema) + "."
							+ putQuote(triggerName) + linesep);
					actionTiming = Reader.getString(1);
					triggerMode = Reader.getString(2);
					actionOrientation = Reader.getString(3);
					actionReferenceOldRow = Reader.getString(4);
					actionReferenceNewRow = Reader.getString(5);
					actionReferenceOldTable = Reader.getString(6);
					actionReferenceNewTable = Reader.getString(7);
					actionCondition = Reader.getString(8);
					eventObjectSchema = Reader.getString(9);
					eventObjectTable = Reader.getString(10);
					String eventManipulation = Reader.getString(11);
					actionStatement = Reader.getString(12);
					buffer.append(actionTiming + " ");
					buffer.append(eventManipulation + " ");
					if (eventManipulation.equalsIgnoreCase("UPDATE")) {
						buffer.append(getTriggerUpdateColumnsList(
								schemaTrigger, triggerName)
								+ linesep);
					} else
						buffer.append(linesep);
					buffer.append("ON " + putQuote(eventObjectSchema) + "."
							+ putQuote(eventObjectTable) + linesep);
					if (((actionReferenceOldRow != null) && (actionReferenceOldRow
							.length() > 0))
							|| ((actionReferenceNewRow != null) && (actionReferenceNewRow
									.length() > 0))
							|| ((actionReferenceOldTable != null) && (actionReferenceOldTable
									.length() > 0))
							|| ((actionReferenceNewTable != null) && (actionReferenceNewTable
									.length() > 0))) {
						ref = true;
					}
					if (ref)
						buffer.append("REFERENCING ");
					if ((actionReferenceOldRow != null)
							&& (actionReferenceOldRow.length() > 0))
						buffer.append("OLD ROW AS " + actionReferenceOldRow
								+ linesep);
					if ((actionReferenceNewRow != null)
							&& (actionReferenceNewRow.length() > 0))
						buffer.append("NEW ROW AS " + actionReferenceNewRow
								+ linesep);
					if ((actionReferenceOldTable != null)
							&& (actionReferenceOldTable.length() > 0))
						buffer.append("OLD TABLE AS " + actionReferenceNewRow
								+ linesep);
					if ((actionReferenceNewTable != null)
							&& (actionReferenceNewTable.length() > 0))
						buffer.append("NEW TABLE AS " + actionReferenceNewTable
								+ linesep);
					if ((actionOrientation != null)
							&& (actionOrientation.length() > 0)) {
						buffer
								.append("FOR EACH " + actionOrientation
										+ linesep);
					}
					if ((triggerMode != null) || (triggerMode.length() > 0)) {
						buffer.append("MODE " + triggerMode + linesep);
					}
					if ((actionCondition != null)
							&& (actionCondition.length() > 0)) {
						buffer.append("WHEN " + actionCondition + linesep);
					}
					buffer.append(actionStatement + linesep + sqlTerminator
							+ linesep + linesep);
					continue;
				}

				String description = Reader.getString(1);
				StringBuffer chunks = new StringBuffer();
				IBMExtractUtilities.getStringChunks(Reader, 2, chunks);
				buffer.append("CREATE TRIGGER " + description + linesep);
				buffer.append(chunks + linesep + sqlTerminator + linesep
						+ linesep);
			}

			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (Exception e) {
			log("Error in getting Trigger for " + triggerName);
			e.printStackTrace();
		}
		return buffer.toString();
	}

	private void genTriggers(String schema) {
		String sql = "";

		ResultSet Reader = null;
		StringBuffer buffer = new StringBuffer();
		StringBuffer chunks = new StringBuffer();
		String dstSchema = getSrctoDstSchema(schema);

		if (dbSourceName.equalsIgnoreCase("idb2")) {
			sql = "select trigger_schema, trigger_name from "
					+ putQuote(schema) + ".systriggers " + "where  "
					+ " event_object_schema = '" + schema + "'";
		} else if (dbSourceName.equalsIgnoreCase("db2")) {
			sql = "SELECT trigschema, trigname, text, func_path FROM SYSCAT.TRIGGERS WHERE trigschema = '"
					+ schema + "' " + "ORDER BY CREATE_TIME";
		} else if (dbSourceName.equalsIgnoreCase("oracle")) {
			sql = "SELECT owner, TRIGGER_NAME, TRIGGER_BODY, '' AS FUNC_PATH FROM DBA_TRIGGERS WHERE owner = '"
					+ schema + "' ";
		}

		if (sql.equals(""))
			return;

		try {
			PreparedStatement queryStatement = mainConn.prepareStatement(sql);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			int objCount = 0;
			while (Reader.next()) {
				try {
					buffer.setLength(0);
					chunks.setLength(0);
					String triggerSchema = trim(Reader.getString(1));
					String triggerName = trim(Reader.getString(2));
					db2DropObjectsWriter.write("DROP TRIGGER "
							+ putQuote(dstSchema) + "." + putQuote(triggerName)
							+ ";" + linesep);
					if (dbSourceName.equalsIgnoreCase("db2")) {
						IBMExtractUtilities.getStringChunks(Reader, 3, chunks);
						buffer.append("SET CURRENT SCHEMA = '" + dstSchema
								+ "'" + linesep + sqlTerminator + linesep);
						buffer.append("SET PATH = SYSTEM PATH, "
								+ putQuote(dstSchema) + linesep + sqlTerminator
								+ linesep);
						buffer.append("--#SET :TRIGGER:" + dstSchema + ":"
								+ triggerName + linesep);
						buffer.append(chunks);
						buffer.append(linesep + sqlTerminator + linesep);
						db2ObjectsWriter[((Integer) plsqlHashTable
								.get("TRIGGER".toLowerCase())).intValue()]
								.write(buffer.toString());
					} else if (dbSourceName.equalsIgnoreCase("idb2")) {
						db2ObjectsWriter[((Integer) plsqlHashTable
								.get("TRIGGER".toLowerCase())).intValue()]
								.write(getTriggerSource(triggerSchema,
										triggerName));
					} else {
						db2ObjectsWriter[((Integer) plsqlHashTable
								.get("TRIGGER".toLowerCase())).intValue()]
								.write(getTriggerSource(triggerSchema,
										triggerName));
					}
					if ((objCount > 0) && (objCount % 20 == 0))
						log(objCount
								+ " numbers of Triggers extracted for schema "
								+ schema);
					objCount++;
				} catch (IOException e) {
					log("Error writing Triggers in file " + e.getMessage());
				} catch (SQLException ex) {
					log("Error getting Triggers " + ex.getMessage());
					ex.printStackTrace();
				}
			}
			if (objCount > 0)
				log(objCount
						+ " Total numbers of Triggers extracted for schema "
						+ schema);
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private String getRoutineBody(String srcType, String schema,
			String specificName) {
		String sql = "";
		String routineSource = "";

		ResultSet Reader = null;

		if (dbSourceName.equalsIgnoreCase("idb2")) {
			if (srcType.equals("1")) {
				sql = "SELECT ROUTINE_DEFINITION FROM SYSIBM.ROUTINES WHERE SPECIFIC_SCHEMA = '"
						+ schema
						+ "' "
						+ "AND   SPECIFIC_NAME = '"
						+ specificName + "'";
			} else {
				sql = "SELECT ROUTINE_DEFINITION FROM QSYS2.SYSFUNCS WHERE SPECIFIC_SCHEMA = '"
						+ schema
						+ "' "
						+ "AND   SPECIFIC_NAME = '"
						+ specificName + "'";
			}

		}

		if (sql.equals(""))
			return "";

		try {
			PreparedStatement queryStatement = mainConn.prepareStatement(sql);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			while (Reader.next()) {
				routineSource = Reader.getString(1);
				routineSource = OraToDb2Converter
						.fixiDB2CursorForReturn(routineSource);
			}
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (Exception e) {
			log("Error in getting procedure body for " + specificName);
			e.printStackTrace();
		}
		return routineSource;
	}

	private String getSourceColumnList(String schema, String sourcespecific,
			String sourceschema) {
		String sql = "";
		String columnList = "";

		if (dbSourceName.equalsIgnoreCase("db2")) {
			sql = "SELECT typeschema, typename FROM SYSCAT.ROUTINEPARMS WHERE specificname = '"
					+ sourcespecific
					+ "' "
					+ "AND routineschema = '"
					+ sourceschema
					+ "' "
					+ "AND rowtype IN ('B', 'O', 'P') "
					+ "ORDER BY ordinal";
		}

		if (sql.equals(""))
			return "";

		ResultSet Reader = null;
		try {
			PreparedStatement queryStatement = mainConn.prepareStatement(sql);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			int objCount = 0;
			while (Reader.next()) {
				String typeSchema = Reader.getString(1);
				String typeName = Reader.getString(2);
				String tmp = composeType(typeSchema, typeName, -1, 0, -1,
						schema);
				if (objCount == 0) {
					columnList = tmp;
				} else {
					columnList = columnList + "," + tmp;
				}
				objCount++;
			}
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (Exception e) {
			log("Error in getting procedure columns for " + sourcespecific);
			e.printStackTrace();
		}
		return columnList;
	}

	private String getProcColumnList(int srcType, String schema,
			String specificName, String columnType) {
		String sql = "";
		String columnList = "";
		String typeName = "";

		ResultSet Reader = null;

		if (dbSourceName.equalsIgnoreCase("idb2")) {
			if (srcType == 1) {
				sql = "SELECT CASE WHEN COLUMN_TYPE = 1 THEN 'IN' WHEN COLUMN_TYPE  = 2 THEN 'INOUT' WHEN COLUMN_TYPE = 4 THEN 'OUT' ELSE 'UNKNOWN' END, COLUMN_NAME, TYPE_NAME, COLUMN_SIZE, DECIMAL_DIGITS FROM SYSIBM.SQLPROCEDURECOLS WHERE PROCEDURE_SCHEM = '"
						+ schema
						+ "' "
						+ "AND SPECIFIC_NAME = '"
						+ specificName + "'";
			} else {
				sql = "SELECT '' X, COLUMN_NAME, TYPE_NAME, COLUMN_SIZE, DECIMAL_DIGITS FROM SYSIBM.SQLPROCEDURECOLS WHERE PROCEDURE_SCHEM = '"
						+ schema
						+ "' "
						+ "AND SPECIFIC_NAME = '"
						+ specificName
						+ "' "
						+ "AND COLUMN_TYPE = "
						+ columnType + "";
			}

		} else if (dbSourceName.equalsIgnoreCase("db2")) {
			sql = "SELECT CASE WHEN rowtype = 'P' THEN 'IN' WHEN ROWTYPE = 'O' THEN 'OUT' ELSE 'INOUT'END, parmname, typename, length, scale, codepage, typeschema, CASE WHEN locator = 'Y' THEN 'AS LOCATOR' ELSE ' ' END FROM SYSCAT.ROUTINEPARMS WHERE '"
					+ specificName
					+ "' = specificname "
					+ "AND '"
					+ schema
					+ "' = routineschema "
					+ "AND rowtype IN ('B', 'O', 'P') "
					+ "ORDER BY ordinal";
		}

		if (sql.equals(""))
			return "";

		try {
			PreparedStatement queryStatement = mainConn.prepareStatement(sql);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			int objCount = 0;
			while (Reader.next()) {
				String tmp = "";
				if (dbSourceName.equalsIgnoreCase("idb2")) {
					typeName = Reader.getString(3);
					if ((typeName.equalsIgnoreCase("VARCHAR"))
							|| (typeName.equalsIgnoreCase("CHAR"))
							|| (typeName.equalsIgnoreCase("CHARACTER"))
							|| (typeName.equalsIgnoreCase("GRAPHIC"))
							|| (typeName.equalsIgnoreCase("VARGRAPHIC"))
							|| (typeName.equalsIgnoreCase("CLOB"))
							|| (typeName.equalsIgnoreCase("DBCLOB"))
							|| (typeName.equalsIgnoreCase("BLOB"))) {
						String dec = Reader.getString(4);
						if (dec != null)
							tmp = typeName + "(" + dec + ")";
						else
							tmp = typeName;
					} else if ((typeName.equalsIgnoreCase("NUMERIC"))
							|| (typeName.equalsIgnoreCase("DECIMAL"))) {
						String dec = Reader.getString(5);
						if (dec != null)
							tmp = typeName + "(" + Reader.getString(4) + ","
									+ Reader.getString(5) + ")";
						else
							tmp = typeName + "(" + Reader.getString(4) + ")";
					} else if (typeName
							.equalsIgnoreCase("CHARACTER FOR BIT DATA")) {
						tmp = "CHARACTER (" + Reader.getString(4)
								+ ") FOR BIT DATA";
					} else if (typeName.equalsIgnoreCase("CHAR FOR BIT DATA")) {
						tmp = "CHAR (" + Reader.getString(4) + ") FOR BIT DATA";
					} else if (typeName
							.equalsIgnoreCase("VARCHAR FOR BIT DATA")) {
						tmp = "VARCHAR (" + Reader.getString(4)
								+ ") FOR BIT DATA";
					} else {
						tmp = typeName;
					}
				} else if (dbSourceName.equalsIgnoreCase("db2")) {
					typeName = trim(Reader.getString(3));
					String typeSchema = trim(Reader.getString(7));
					int length = Reader.getInt(4);
					int scale = Reader.getInt(5);
					int codePage = Reader.getInt(6);
					tmp = composeType(typeSchema, typeName, length, scale,
							codePage, schema)
							+ Reader.getString(8);
				}
				if (objCount == 0) {
					columnList = Reader.getString(1) + " "
							+ putQuote(Reader.getString(2)) + " " + tmp;
				} else {
					columnList = columnList + "," + linesep
							+ Reader.getString(1) + " "
							+ putQuote(Reader.getString(2)) + " " + tmp;
				}
				objCount++;
			}
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (Exception e) {
			log("Error in getting procedure columns for " + specificName);
			e.printStackTrace();
		}
		return columnList;
	}

	private String authColumnList(String schema, String colName) {
		String sql = "";
		String columnList = "";

		ResultSet Reader = null;

		if (dbSourceName.equalsIgnoreCase("db2")) {
			sql = "SELECT typeschema, typename FROM SYSCAT.ROUTINEPARMS WHERE specificname = '"
					+ colName
					+ "' AND routineschema = '"
					+ schema
					+ "' AND rowtype IN ('B', 'O', 'P') ORDER BY ordinal ASC";
		}

		if (sql.equals(""))
			return "";

		try {
			PreparedStatement queryStatement = mainConn.prepareStatement(sql);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			int objCount = 0;
			while (Reader.next()) {
				String typeSchema = trim(Reader.getString(1));
				String typeName = trim(Reader.getString(2));
				if (objCount == 0) {
					columnList = composeType(typeSchema, typeName, -1, 0, -1,
							schema);
				} else {
					columnList = columnList
							+ ","
							+ composeType(typeSchema, typeName, -1, 0, -1,
									schema);
				}
				objCount++;
			}
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (Exception e) {
			log("Error in getting authColumnList " + sql);
			e.printStackTrace();
		}
		return columnList;
	}

	private void genDB2Grants(String schema) {
		String sql = "";
		String dstSchema = getSrctoDstSchema(schema);

		StringBuffer buffer = new StringBuffer();
		int objCount = 0;

		if (dbSourceName.equalsIgnoreCase("db2")) {
			sql = "WITH AUTH AS ( SELECT granteetype, grantee, tabname as name, CAST(NULL AS VARCHAR(128)) AS colname, auth, 'TABLE' as type FROM SYSCAT.TABAUTH,  LATERAL(VALUES     (case when controlauth = 'Y' then 'CONTROL' else NULL end),     (case when alterauth = 'Y' then 'ALTER' when alterauth = 'N' then NULL else 'ALTER      GRANT' end),     (case when deleteauth = 'Y' then 'DELETE' when deleteauth = 'N' then NULL else 'DELETE     GRANT' end),     (case when indexauth = 'Y' then 'INDEX' when indexauth = 'N' then NULL else 'INDEX      GRANT' end),     (case when insertauth = 'Y' then 'INSERT' when insertauth = 'N' then NULL else 'INSERT     GRANT' end),     (case when selectauth = 'Y' then 'SELECT' when selectauth = 'N' then NULL else 'SELECT     GRANT' end),     (case when refauth = 'Y' then 'REFERENCE' when refauth = 'N' then NULL else 'REFERENCES GRANT' end),     (case when updateauth = 'Y' then 'UPDATE' when updateauth = 'N' then NULL else 'UPDATE     GRANT' end)  )  AS A(auth) WHERE tabschema = '"
					+ schema
					+ "' "
					+ "UNION ALL "
					+ "SELECT granteetype, "
					+ "       grantee, "
					+ "       tabname as name, "
					+ "       colname, "
					+ "       CASE privtype WHEN 'U' THEN 'UPDATE' WHEN 'R' THEN 'REFERENCES' END || ' GRANT' AS auth, "
					+ "       'COLUMN' AS type "
					+ "FROM SYSCAT.COLAUTH "
					+ "WHERE tabschema = '"
					+ schema
					+ "' "
					+ "UNION ALL "
					+ "SELECT granteetype, "
					+ "       grantee, "
					+ "       indname as name, "
					+ "       CAST(NULL AS VARCHAR(128)) AS colname, "
					+ "       (case when controlauth = 'Y' then 'CONTROL' else NULL end) AS auth, "
					+ "       'INDEX' as type "
					+ "FROM SYSCAT.INDEXAUTH "
					+ "WHERE indschema = '"
					+ schema
					+ "' "
					+ "UNION ALL "
					+ "SELECT granteetype, "
					+ "       grantee, "
					+ "       R.routinename as name,"
					+ "       R.specificname as colname, "
					+ "       (case when executeauth = 'Y' then 'EXECUTE' when executeauth = 'N' then NULL else 'EXECUTE    GRANT' end) AS auth, "
					+ "       CASE R.routinetype WHEN 'F' THEN 'FUNCTION' "
					+ "                          WHEN 'P' THEN 'PROCEDURE' "
					+ "                          END as type "
					+ "FROM SYSCAT.ROUTINEAUTH A, "
					+ "     SYSCAT.ROUTINES R "
					+ "WHERE A.schema = '"
					+ schema
					+ "' "
					+ "AND A.schema = R.routineschema "
					+ "AND A.specificname = R.specificname "
					+ "AND A.routinetype IN ('F', 'P') "
					+ "UNION ALL "
					+ "SELECT granteetype, "
					+ "       grantee, "
					+ "       '"
					+ schema
					+ "' as name, "
					+ "       CAST(NULL AS VARCHAR(128)) AS colname, "
					+ "       auth, "
					+ "       'SCHEMA' as type "
					+ "FROM SYSCAT.SCHEMAAUTH, "
					+ "  LATERAL(VALUES "
					+ "          (case when alterinauth = 'Y' then 'ALTERIN' when alterinauth = 'N' then NULL else 'ALTERIN    GRANT' end), "
					+ "          (case when createinauth = 'Y' then 'CREATEIN' when createinauth = 'N' then NULL else 'CREATEIN   GRANT' end), "
					+ "          (case when dropinauth = 'Y' then 'DROPIN' when dropinauth = 'N' then NULL else 'DROPIN     GRANT' end) "
					+ "       ) "
					+ "       AS A(auth) "
					+ "WHERE schemaname = '"
					+ schema
					+ "' "
					+ "UNION ALL "
					+ "SELECT granteetype, "
					+ "       grantee, "
					+ "       seqname as name, "
					+ "       CAST(NULL AS VARCHAR(128)) AS colname, "
					+ "       auth, "
					+ "       'SEQUENCE' as type "
					+ "FROM SYSCAT.SEQUENCEAUTH, "
					+ "  LATERAL(VALUES "
					+ "            (case when usageauth = 'Y' then 'USAGE' when usageauth = 'N' then NULL else 'USAGE      GRANT' end), "
					+ "            (case when alterauth = 'Y' then 'ALTER' when alterauth = 'N' then NULL else 'ALTER      GRANT' end) "
					+ "          ) "
					+ "   AS A(auth) "
					+ "WHERE seqschema = '"
					+ schema
					+ "' "
					+ ((majorSourceDBVersion >= 9)
							&& (minorSourceDBVersion >= 7) ? "UNION ALL SELECT granteetype, grantee, varname as name, CAST(NULL AS VARCHAR(128)) AS colname, auth, 'VARIABLE' as type FROM SYSCAT.VARIABLEAUTH,    LATERAL(VALUES       (case when readauth = 'Y' then 'READ' when readauth = 'N' then NULL else 'READ      GRANT' end),       (case when writeauth = 'Y' then 'WRITE' when writeauth = 'N' then NULL else 'WRITE     GRANT' end))    AS A(auth) WHERE varschema = '"
							+ schema + "' "
							: " ")
					+ ((majorSourceDBVersion >= 9)
							&& (minorSourceDBVersion >= 5) ? "UNION ALL SELECT granteetype, grantee, MODULENAME as name, CAST(NULL AS VARCHAR(128)) AS colname,        (case when EXECUTEAUTH = 'Y' then 'MODULE' when EXECUTEAUTH = 'N' then NULL else 'EXECUTE    GRANT' end) auth, 'EXECUTE' as type FROM SYSCAT.MODULEAUTH WHERE MODULESCHEMA = '"
							+ schema
							+ "' "
							+ "UNION ALL "
							+ "SELECT granteetype, grantee, rolename as name, CAST(NULL AS VARCHAR(128)) AS colname, "
							+ "       (case when admin = 'Y' then 'ROLE       GRANT' else 'ROLE' end) auth, 'ROLE' as type "
							+ "FROM SYSCAT.ROLEAUTH "
							+ "WHERE grantor = '"
							+ schema
							+ "' "
							+ "and rolename NOT LIKE 'SYSROLE%' "
							: " ")
					+ ") SELECT * FROM AUTH WHERE AUTH IS NOT NULL "
					+ "ORDER BY CASE type WHEN 'SCHEMA' THEN 1 "
					+ "WHEN 'TABLE' THEN 2 "
					+ "WHEN 'COLUMN' THEN 3 "
					+ "ELSE 4 END";

			if ((majorSourceDBVersion >= 9) && (minorSourceDBVersion >= 5)) {
				String role = executeSQL(
						"select rolename from syscat.roles where rolename not like 'SYS%'",
						false);
				if ((role != null) && (role.length() > 0)) {
					String[] roles = role.split("~");
					for (int i = 0; i < roles.length; i++) {
						try {
							db2ObjectsWriter[((Integer) plsqlHashTable
									.get("GRANTS".toLowerCase())).intValue()]
									.write("CREATE ROLE " + roles[i] + linesep
											+ sqlTerminator + linesep);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}

		if (sql.equals(""))
			return;

		ResultSet Reader = null;
		try {
			PreparedStatement queryStatement = mainConn.prepareStatement(sql);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			while (Reader.next()) {
				buffer.setLength(0);
				String granteeType = trim(Reader.getString(1));
				String grantee = trim(Reader.getString(2));
				String name = trim(Reader.getString(3));
				String colName = trim(Reader.getString(4));
				String auth = trim(Reader.getString(5));
				String type = trim(Reader.getString(6));
				if (auth.length() >= 10)
					buffer.append("GRANT " + auth.substring(0, 9));
				else
					buffer.append("GRANT " + auth);
				if (type.equals("COLUMN"))
					buffer.append(" (\"" + colName + "\")");
				if (!type.equals("ROLE"))
					buffer.append(" ON ");
				if ((type.equals("COLUMN")) || (type.equals("TABLE"))
						|| (type.equals("ROLE")))
					buffer.append(" \"" + name + "\" ");
				else
					buffer.append(type + " \"" + name + "\" ");
				if ((type.equals("FUNCTION")) || (type.equals("PROCEDURE"))) {
					buffer.append("(" + authColumnList(schema, colName) + ")");
				}
				buffer.append(" TO ");
				if (grantee.equals("PUBLIC")) {
					buffer.append(grantee);
				} else if (granteeType.equals("U"))
					buffer.append("USER \"" + grantee + "\"");
				else if (granteeType.equals("R"))
					buffer.append("ROLE \"" + grantee + "\"");
				else {
					buffer.append("GROUP \"" + grantee + "\"");
				}
				if (auth.endsWith("GRANT"))
					buffer.append(" WITH GRANT OPTION");
				buffer.append(linesep + sqlTerminator + linesep);
				db2ObjectsWriter[((Integer) plsqlHashTable.get("GRANTS"
						.toLowerCase())).intValue()].write(buffer.toString());
				if ((objCount > 0) && (objCount % 20 == 0))
					log(objCount + " numbers of grants extracted for schema "
							+ schema);
				objCount++;
			}
			if (objCount > 0)
				log(objCount + " Total numbers of Grants extracted for schema "
						+ schema);
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (Exception e) {
			log("Error in genInstallJavaJars for " + sql);
			e.printStackTrace();
		}
	}

	private String genInstallJavaJars() {
		boolean once = true;
		String sql = "";
		String jarPath = "";
		StringBuffer buffer = new StringBuffer();

		if (dbSourceName.equalsIgnoreCase("db2")) {
			sql = "select distinct jarschema, jar_id from syscat.routines where jar_id is not null ";
		}

		if (sql.equals(""))
			return "";

		ResultSet Reader = null;
		try {
			PreparedStatement queryStatement = mainConn.prepareStatement(sql);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			while (Reader.next()) {
				buffer.setLength(0);
				String jarSchema = trim(Reader.getString(1));
				String jarID = trim(Reader.getString(2));
				jarPath = getJavaJarPath(jarSchema, jarID);
				buffer.append("--#SET :JavaJars:" + jarSchema + ":" + jarID
						+ linesep);
				buffer.append("CALL SQLJ.REMOVE_JAR('" + jarSchema + "."
						+ jarID + "')" + linesep);
				buffer.append(sqlTerminator + linesep);
				buffer.append("CALL SQLJ.INSTALL_JAR('file:" + jarPath + "','"
						+ jarSchema + "." + jarID + "')" + linesep);
				buffer.append(sqlTerminator + linesep);
				buffer.append("CALL SQLJ.REFRESH_CLASSES()" + linesep);
				buffer.append(sqlTerminator + linesep);
				db2ObjectsWriter[((Integer) plsqlHashTable.get("JAVAJARS"
						.toLowerCase())).intValue()].write(buffer.toString());
			}
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (Exception e) {
			log("Error in genInstallJavaJars for " + sql);
			e.printStackTrace();
		}
		return buffer.toString();
	}

	private String getJavaJarPath(String jarSchema, String jarID) {
		String jarPath = "";
		String sql = "";
		if (osType.equalsIgnoreCase("win")) {
			if (majorSourceDBVersion > 8) {
				sql = "select REG_VAR_VALUE from sysibmadm.REG_VARIABLES where reg_var_name = 'DB2INSTPROF' and dbpartitionnum = 0";

				jarPath = executeSQL(sql, false) + "\\function\\jar\\"
						+ jarSchema + "\\" + jarID + ".jar";
			} else {
				Process p = null;
				BufferedReader stdInput = null;

				String[] tok = null;
				try {
					p = Runtime.getRuntime().exec("db2cmd /c /i /w set ");
					stdInput = new BufferedReader(new InputStreamReader(p
							.getInputStream()));
					String line;
					while ((line = stdInput.readLine()) != null) {
						if (!line.startsWith("DB2PATH"))
							continue;
						tok = line.split("=");
						jarPath = tok[1] + "\\java";
					}

					if (stdInput != null)
						stdInput.close();
				} catch (Exception e) {
					jarPath = "not detected";
				}
			}
		} else {
			String userHome = System.getProperty("user.home");
			if (IBMExtractUtilities.FileExists(userHome + "/sqllib")) {
				jarPath = userHome + "/sqllib/function/jar/" + jarSchema + "/"
						+ jarID + ".jar";
			} else
				jarPath = "not detected";
		}
		return jarPath;
	}

	private void getNonSQLProcedureSource(String schema) {
		boolean once = true;
		String sql = "";
		String routineName = "";
		String dstSchema = getSrctoDstSchema(schema);
		String specificName = "";

		ResultSet Reader = null;
		StringBuffer buffer = new StringBuffer();

		if (dbSourceName.equalsIgnoreCase("db2")) {
			sql = "SELECT routineschema, routinename, specificname, routinetype, origin, functiontype, language, sourceschema, sourcespecific, deterministic, external_action, nullcall, scratchpad, scratchpad_length, parallel, parameter_style, fenced, sql_data_access, dbinfo, result_sets, threadsafe, class, implementation, finalcall, cardinality, jar_id, jarschema FROM SYSCAT.ROUTINES WHERE routinetype IN ('F', 'P') AND routineschema = '"
					+ schema
					+ "' "
					+ "AND language <> 'SQL' "
					+ "AND origin IN ('U', 'E', 'M') "
					+ "ORDER BY specificname";
		}

		if (sql.equals(""))
			return;

		try {
			PreparedStatement queryStatement = mainConn.prepareStatement(sql);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			while (Reader.next()) {
				buffer.setLength(0);
				if (once) {
					buffer.append("SET CURRENT SCHEMA = '" + dstSchema + "'"
							+ linesep);
					buffer.append(sqlTerminator + linesep);
					once = false;
				}
				routineName = trim(Reader.getString(2));
				specificName = trim(Reader.getString(3));
				String routineType = trim(Reader.getString(4));
				String origin = trim(Reader.getString(5));
				String functionType = trim(Reader.getString(6));
				String language = trim(Reader.getString(7));
				String sourceSchema = trim(Reader.getString(8));
				String sourceSpecific = trim(Reader.getString(9));
				String procType = routineType.equals("F") ? "FUNCTION"
						: "PROCEDURE";
				db2DropObjectsWriter.write("DROP SPECIFIC " + procType + " "
						+ putQuote(dstSchema) + "." + putQuote(specificName)
						+ ";" + linesep);
				buffer.append("--#SET :" + procType + ":" + dstSchema + ":"
						+ routineName + linesep);
				buffer.append("CREATE " + procType + " " + putQuote(dstSchema)
						+ "." + putQuote(routineName));
				String tmp = getProcColumnList(1, schema, specificName, null);
				if ((tmp != null) && (tmp.length() == 0))
					buffer.append("()" + linesep);
				else
					buffer.append(linesep + "(" + linesep + tmp + linesep + ")"
							+ linesep);
				if (routineType.equals("F")) {
					buffer.append("RETURNS ");
					if (functionType.equals("T"))
						buffer.append("TABLE (" + linesep);
					buffer.append(getFunctionReturnMarker("2", null, null,
							schema, specificName));
					if (functionType.equals("T"))
						buffer.append(")" + linesep);
				}
				buffer.append("SPECIFIC " + putQuote(dstSchema) + "."
						+ putQuote(specificName) + linesep);
				if (origin.equals("M")) {
					buffer.append("AS TEMPLATE " + linesep);
				} else if (origin.equals("U")) {
					buffer.append("SOURCE ");
					if (sourceSchema.startsWith("SYSIBM")) {
						String implementation = Reader.getString(23);
						int pos = implementation.indexOf(".");
						tmp = implementation.substring(0, pos + 1);
						buffer.append(tmp);
						String name = implementation.substring(pos + 1,
								implementation.length());
						pos = name.indexOf(" (");
						buffer.append("\"");
						if (pos > 0)
							buffer.append(name.substring(0, pos) + "\""
									+ name.substring(pos, name.length()));
						else
							buffer.append(name + "\"");
						buffer.append(linesep);
					} else {
						if (!sourceSchema.equalsIgnoreCase(schema)) {
							buffer.append(putQuote(sourceSchema) + ".");
						}
						tmp = executeSQL(
								"SELECT routinename FROM SYSCAT.ROUTINES WHERE routineschema = '"
										+ sourceSchema + "' "
										+ "AND specificname = '"
										+ sourceSpecific + "'", false);

						buffer.append(putQuote(tmp)
								+ linesep
								+ "("
								+ linesep
								+ getSourceColumnList(schema, sourceSchema,
										sourceSpecific) + linesep + ")"
								+ linesep);
					}

				} else {
					buffer.append("EXTERNAL NAME ");
					String implementation = trim(Reader.getString(23));
					if (language.equalsIgnoreCase("JAVA")) {
						String jarID = trim(Reader.getString(26));
						String jarSchema = trim(Reader.getString(27));
						String classStr = trim(Reader.getString(22));
						buffer.append("'" + jarSchema + "." + jarID + ":"
								+ classStr);
						int pos = implementation.indexOf("(");
						if (pos > 0)
							buffer.append("!"
									+ implementation.substring(0, pos) + "'");
						else
							buffer.append("!" + implementation + "'");
					} else {
						buffer.append("'" + implementation + "'");
					}
					buffer.append(linesep);
					buffer.append("LANGUAGE " + language + linesep);
					String parameterStyle = trim(Reader.getString(16));
					if (parameterStyle != null) {
						if (parameterStyle.equalsIgnoreCase("DB2SQL"))
							tmp = "DB2SQL";
						else if (parameterStyle.equalsIgnoreCase("SQL"))
							tmp = "SQL";
						else if (parameterStyle.equalsIgnoreCase("DB2GENRL"))
							tmp = "DB2GENERAL";
						else if (parameterStyle.equalsIgnoreCase("GENERAL"))
							tmp = "GENERAL";
						else if (parameterStyle.equalsIgnoreCase("JAVA"))
							tmp = "JAVA";
						else if (parameterStyle.equalsIgnoreCase("DB2DARI"))
							tmp = "DB2DARI";
						else if (parameterStyle.equalsIgnoreCase("GNRLNULL"))
							tmp = "GENERAL WITH NULLS";
						else
							tmp = "";
						buffer.append("PARAMETER STYLE " + tmp + linesep);
					}
					String externalAction = Reader.getString(11);
					if ((externalAction != null)
							&& (externalAction.equals("Y")))
						buffer.append("EXTERNAL ACTION" + linesep);
					else
						buffer.append("NO EXTERNAL ACTION" + linesep);
					String scratchPad = Reader.getString(13);
					String scratchPadLength = Reader.getString(14);
					if ((scratchPad != null) && (scratchPad.equals("Y")))
						buffer.append("SCRATCHPAD " + scratchPadLength
								+ linesep);
					else if ((scratchPad != null) && (scratchPad.equals("N"))
							&& (!routineType.equals("P")))
						buffer.append("NO SCRATCHPAD " + linesep);
					String finalCall = Reader.getString(24);
					if ((finalCall != null) && (finalCall.equals("Y")))
						buffer.append("FINAL CALL" + linesep);
					else
						buffer.append("NO FINAL CALL" + linesep);
					String parallel = Reader.getString(15);
					if ((parallel != null) && (parallel.equals("Y")))
						buffer.append("ALLOW PARALLEL" + linesep);
					else if ((parallel != null) && (parallel.equals("Y"))
							&& (!routineType.equals("P")))
						buffer.append("DISALLOW PARALLEL" + linesep);
					String dbInfo = Reader.getString(19);
					if ((dbInfo != null) && (dbInfo.equals("Y")))
						buffer.append("DBINFO" + linesep);
					else
						buffer.append("NODBINFO" + linesep);
					int cardinality = Reader.getInt(25);
					if (cardinality > 0)
						buffer.append("CARDINALITY " + cardinality + linesep);
				}
				buffer.append(linesep + sqlTerminator + linesep + linesep);
				db2ObjectsWriter[((Integer) plsqlHashTable.get("ROUTINE"
						.toLowerCase())).intValue()].write(buffer.toString());
			}
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (Exception e) {
			log("Error in getting procedure for " + schema + "." + specificName);
			e.printStackTrace();
		}
	}

	private String getProcedureSource(String schema, String specificName) {
		String sql = "";
		String procName = "";
		String dstSchema = getSrctoDstSchema(schema);

		ResultSet Reader = null;
		StringBuffer buffer = new StringBuffer();

		if (dbSourceName.equalsIgnoreCase("idb2")) {
			sql = "SELECT PROCEDURE_SCHEM, PROCEDURE_NAME, NUM_RESULT_SETS, SPECIFIC_NAME FROM SYSIBM.SQLPROCEDURES WHERE PROCEDURE_SCHEM = '"
					+ schema
					+ "' "
					+ "AND   SPECIFIC_NAME = '"
					+ specificName
					+ "'";
		}

		if (sql.equals(""))
			return "";

		try {
			PreparedStatement queryStatement = mainConn.prepareStatement(sql);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			while (Reader.next()) {
				procName = Reader.getString(2);
				buffer.append("SET CURRENT SCHEMA = '" + dstSchema + "'"
						+ linesep);
				buffer.append(sqlTerminator + linesep);
				buffer.append("SET PATH = SYSTEM PATH, " + putQuote(dstSchema)
						+ linesep + sqlTerminator + linesep);
				buffer.append("ECHO PROCEDURE:" + dstSchema + ":" + procName
						+ linesep + sqlTerminator + linesep);
				buffer.append("--#SET :PROCEDURE:" + dstSchema + ":" + procName
						+ linesep);
				buffer.append("CREATE PROCEDURE " + putQuote(dstSchema) + "."
						+ putQuote(procName) + linesep);
				buffer.append("(" + linesep
						+ getProcColumnList(1, schema, specificName, null)
						+ linesep + ")" + linesep);
				String numRS = Reader.getString(3);
				if ((numRS != null) && (numRS.length() > 0))
					buffer.append("RESULT SETS " + numRS + linesep);
				buffer.append("LANGUAGE SQL" + linesep);
				buffer.append("SPECIFIC " + putQuote(dstSchema) + "."
						+ putQuote(Reader.getString(4)) + linesep);
				buffer.append(getRoutineBody("1", schema, specificName));
				buffer.append(linesep + sqlTerminator + linesep + linesep);
			}
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (Exception e) {
			log("Error in getting procedure for " + procName);
			e.printStackTrace();
		}
		return buffer.toString();
	}

	private String getFunctionReturnMarker(String functionType,
			String returnType, String charMaxLength, String schema,
			String specificName) {
		String returnStr = "";
		String parmName = "";
		if (functionType.equals("1")) {
			if (returnType.equalsIgnoreCase("CHARACTER VARYING")) {
				if ((charMaxLength != null) && (charMaxLength.length() > 0))
					returnStr = "VARCHAR(" + charMaxLength + ")";
				else
					returnStr = "VARCHAR";
			} else if ((returnType.equalsIgnoreCase("CHARACTER"))
					|| (returnType.equalsIgnoreCase("CHAR"))) {
				if ((charMaxLength != null) && (charMaxLength.length() > 0))
					returnStr = "CHARACTER(" + charMaxLength + ")";
				else
					returnStr = "CHARACTER";
			} else
				returnStr = returnType;
			return "RETURNS " + returnStr + linesep;
		}

		int colCount = 0;
		String sql = "";
		String columnList = "";
		String typeName = "";

		ResultSet Reader = null;

		if (dbSourceName.equalsIgnoreCase("idb2")) {
			sql = "SELECT COLUMN_NAME, TYPE_NAME, COLUMN_SIZE, BUFFER_LENGTH, DECIMAL_DIGITS, NUM_PREC_RADIX, IS_NULLABLE FROM SYSIBM.SQLFUNCTIONCOLS WHERE FUNCTION_SCHEM = '"
					+ schema
					+ "' "
					+ "AND   SPECIFIC_NAME = '"
					+ specificName
					+ "' "
					+ "AND COLUMN_TYPE <> 1 "
					+ "ORDER BY ORDINAL_POSITION ";
		} else if (dbSourceName.equalsIgnoreCase("db2")) {
			sql = "SELECT c.parmname parmname, C.typeschema as ctypeschema, C.typename ctypename, C.length clength, C.scale cscale, C.codepage ccodepage, R.typeschema as rtypeschema, R.typename rtypename, R.length rlength, R.scale rscale, R.codepage rcodepage, CASE WHEN c.locator = 'Y' THEN 'AS LOCATOR' ELSE ' ' END clocator FROM SYSCAT.ROUTINEPARMS C LEFT OUTER JOIN SYSCAT.ROUTINEPARMS R ON C.routineschema = R.routineschema AND C.specificname = R.specificname AND C.ordinal = R.ordinal AND R.rowtype = 'R' WHERE '"
					+ specificName
					+ "' = C.specificname "
					+ "AND '"
					+ schema
					+ "' = C.routineschema "
					+ "AND C.rowtype = 'C' "
					+ "ORDER BY C.ordinal";
		}

		if (sql.equals(""))
			return "";

		try {
			PreparedStatement queryStatement = mainConn.prepareStatement(sql);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			while (Reader.next()) {
				String tmp = "";
				if (dbSourceName.equalsIgnoreCase("idb2")) {
					parmName = Reader.getString(1);
					if (parmName == null)
						parmName = "";
					typeName = Reader.getString(2);
					if ((typeName.equalsIgnoreCase("VARCHAR"))
							|| (typeName.equalsIgnoreCase("CHAR"))
							|| (typeName.equalsIgnoreCase("CHARACTER"))
							|| (typeName.equalsIgnoreCase("GRAPHIC"))
							|| (typeName.equalsIgnoreCase("VARGRAPHIC"))
							|| (typeName.equalsIgnoreCase("CLOB"))
							|| (typeName.equalsIgnoreCase("DBCLOB"))
							|| (typeName.equalsIgnoreCase("BLOB"))) {
						String dec = Reader.getString(4);
						if (dec != null)
							tmp = typeName + "(" + dec + ")";
						else
							tmp = typeName;
					} else if ((typeName.equalsIgnoreCase("NUMERIC"))
							|| (typeName.equalsIgnoreCase("DECIMAL"))) {
						String dec = Reader.getString(5);
						if (dec != null) {
							if (dec.equals("0"))
								dec = Reader.getString(3);
							tmp = typeName + "(" + dec + ","
									+ Reader.getString(6) + ")";
						} else {
							tmp = typeName + "(" + Reader.getString(5) + ")";
						}
					} else if (typeName
							.equalsIgnoreCase("CHARACTER FOR BIT DATA")) {
						tmp = "CHARACTER (" + Reader.getString(4)
								+ ") FOR BIT DATA";
					} else if (typeName.equalsIgnoreCase("CHAR FOR BIT DATA")) {
						tmp = "CHAR (" + Reader.getString(4) + ") FOR BIT DATA";
					} else if (typeName
							.equalsIgnoreCase("VARCHAR FOR BIT DATA")) {
						tmp = "VARCHAR (" + Reader.getString(4)
								+ ") FOR BIT DATA";
					} else {
						tmp = typeName;
					}
				} else if (dbSourceName.equalsIgnoreCase("db2")) {
					parmName = Reader.getString(1);
					if (parmName == null)
						parmName = "";
					String ctypeSchema = trim(Reader.getString(2));
					String ctypeName = trim(Reader.getString(3));
					int clength = Reader.getInt(4);
					int cscale = Reader.getInt(5);
					int ccodePage = Reader.getInt(6);
					tmp = composeType(ctypeSchema, ctypeName, clength, cscale,
							ccodePage, schema);
					String rtypeSchema = Reader.getString(7);
					if ((rtypeSchema != null) && (rtypeSchema.length() > 0)) {
						String rtypeName = Reader.getString(8);
						int rlength = Reader.getInt(9);
						int rscale = Reader.getInt(10);
						int rcodePage = Reader.getInt(11);
						tmp = tmp
								+ composeType(rtypeSchema, rtypeName, rlength,
										rscale, rcodePage, schema);
					}
					tmp = tmp + Reader.getString(12);
				}

				if (colCount == 0) {
					columnList = putQuote(parmName) + " " + tmp + linesep;
				} else {
					columnList = columnList + "," + putQuote(parmName) + " "
							+ tmp + linesep;
				}
				colCount++;
			}
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (Exception e) {
			log("Error in getting sql table function columns " + specificName);
			e.printStackTrace();
		}
		if (dbSourceName.equalsIgnoreCase("idb2")) {
			return "RETURNS TABLE " + linesep + "(" + linesep + columnList
					+ linesep + ")" + linesep;
		}
		return columnList;
	}

	private String getFunctionSource(String schema, String specificName,
			String functionType) {
		String sql = "";
		String routineSchema = "";
		String returnType = "";
		String charMaxLength = "";
		String procName = "";
		String isDeterministic = "";
		String sqlDataAccess = "";
		String externalAction = "";

		ResultSet Reader = null;
		StringBuffer buffer = new StringBuffer();

		if (dbSourceName.equalsIgnoreCase("idb2")) {
			if (functionType.equals("1")) {
				sql = "SELECT ROUTINE_SCHEMA, ROUTINE_NAME, DATA_TYPE, SPECIFIC_SCHEMA, SPECIFIC_NAME, IS_DETERMINISTIC, SQL_DATA_ACCESS, CHARACTER_MAXIMUM_LENGTH FROM SYSIBM.ROUTINES WHERE SPECIFIC_SCHEMA = '"
						+ schema
						+ "' "
						+ "AND   SPECIFIC_NAME = '"
						+ specificName + "'";
			} else {
				sql = "SELECT ROUTINE_SCHEMA, ROUTINE_NAME, '' AS DATA_TYPE, SPECIFIC_SCHEMA, SPECIFIC_NAME, IS_DETERMINISTIC, SQL_DATA_ACCESS, 0 AS CHARACTER_MAXIMUM_LENGTH, EXTERNAL_ACTION FROM QSYS2.SYSFUNCS WHERE SPECIFIC_SCHEMA = '"
						+ schema
						+ "' "
						+ "AND   SPECIFIC_NAME = '"
						+ specificName + "'";
			}

		}

		if (sql.equals(""))
			return "";

		try {
			PreparedStatement queryStatement = mainConn.prepareStatement(sql);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			while (Reader.next()) {
				routineSchema = trim(Reader.getString(1));
				procName = trim(Reader.getString(2));
				returnType = trim(Reader.getString(3));
				isDeterministic = Reader.getString(6);
				sqlDataAccess = Reader.getString(7);
				charMaxLength = Reader.getString(8);
				String dstSchema = getSrctoDstSchema(routineSchema);
				buffer.append("SET CURRENT SCHEMA = '" + dstSchema + "'"
						+ linesep);
				buffer.append(sqlTerminator + linesep);
				buffer.append("SET PATH = SYSTEM PATH, " + putQuote(dstSchema)
						+ linesep);
				buffer.append(sqlTerminator + linesep);
				buffer.append("--#SET :FUNCTION:" + dstSchema + ":" + procName
						+ linesep);
				buffer.append("CREATE FUNCTION " + putQuote(dstSchema) + "."
						+ putQuote(procName) + linesep);
				buffer.append("(" + linesep
						+ getProcColumnList(2, schema, specificName, "1")
						+ linesep + ")" + linesep);
				buffer.append(getFunctionReturnMarker(functionType, returnType,
						charMaxLength, schema, specificName));
				buffer.append("LANGUAGE SQL ");
				buffer.append("SPECIFIC " + putQuote(dstSchema) + "."
						+ putQuote(Reader.getString(5)) + linesep);
				if (isDeterministic != null) {
					if (isDeterministic.equalsIgnoreCase("YES"))
						buffer.append("DETERMINISTIC  ");
					else
						buffer.append("NOT DETERMINISTIC ");
				}
				if (!functionType.equals("1")) {
					externalAction = Reader.getString(9);
					if ((externalAction != null)
							&& (externalAction.equals("N")))
						buffer.append("NO EXTERNAL ACTION" + linesep);
				}
				if (sqlDataAccess != null) {
					if (sqlDataAccess.startsWith("READS"))
						buffer.append("READS SQL DATA" + linesep);
					else if (sqlDataAccess.startsWith("MODIFIES"))
						buffer.append("MODIFIES SQL DATA" + linesep);
					else if (sqlDataAccess.startsWith("CONTAINS"))
						buffer.append("CONTAINS SQL" + linesep);
					else
						buffer.append(sqlDataAccess + linesep);
				}
				buffer.append(getRoutineBody(functionType, routineSchema,
						procName));
				buffer.append(linesep + sqlTerminator + linesep + linesep);
			}
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (Exception e) {
			log("Error in getting function for " + procName);
			e.printStackTrace();
		}
		return buffer.toString();
	}

	private String getColumnList(String schema, String mviewName) {
		String sql = "";
		String columnList = "";

		ResultSet Reader = null;

		if (dbSourceName.equalsIgnoreCase("oracle")) {
			sql = "SELECT COLUMN_NAME FROM DBA_TAB_COLUMNS WHERE OWNER = '"
					+ schema + "' AND TABLE_NAME = '" + mviewName
					+ "' ORDER BY COLUMN_ID ASC";
		} else if (dbSourceName.equalsIgnoreCase("idb2")) {
			sql = "SELECT COLUMN_NAME FROM SYSIBM.SQLCOLUMNS WHERE TABLE_SCHEM = '"
					+ schema
					+ "' AND TABLE_NAME = '"
					+ mviewName
					+ "' ORDER BY ORDINAL_POSITION ASC";
		}

		if (sql.equals(""))
			return "";

		try {
			PreparedStatement queryStatement = mainConn.prepareStatement(sql);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			int objCount = 0;
			while (Reader.next()) {
				if (objCount == 0) {
					columnList = putQuote(Reader.getString(1));
				} else {
					columnList = columnList + ","
							+ putQuote(Reader.getString(1));
				}
				objCount++;
			}
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (Exception e) {
			log("Error in getting materialized query columns for materialized view "
					+ mviewName);
			e.printStackTrace();
		}
		return columnList;
	}

	private void genDB2GVariables(String schema) {
		StringBuffer sb = new StringBuffer();
		String sql = "";
		String expression = "";
		String dstSchema = getSrctoDstSchema(schema);

		String varType = "";

		ResultSet Reader = null;

		if (dbSourceName.equalsIgnoreCase("db2")) {
			if ((majorSourceDBVersion >= 9) && (minorSourceDBVersion >= 7)) {
				sql = "SELECT VARNAME,TYPESCHEMA,TYPENAME,LENGTH, SCALE,CODEPAGE,READONLY,REMARKS,DEFAULT FROM SYSCAT.VARIABLES  WHERE VARSCHEMA = '"
						+ schema + "' " + "AND VARMODULENAME IS NULL ";
			}

		}

		if (sql.equals(""))
			return;

		try {
			PreparedStatement queryStatement = mainConn.prepareStatement(sql);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			int objCount = 0;
			while (Reader.next()) {
				sb.setLength(0);
				String varName = trim(Reader.getString(1));
				String typeSchema = trim(Reader.getString(2));
				String typeName = trim(Reader.getString(3));
				int length = Reader.getInt(4);
				int scale = Reader.getInt(5);
				int codePage = Reader.getInt(6);
				String readOnly = trim(Reader.getString(7));
				String remarks = trim(Reader.getString(8));
				String defaultValue = trim(Reader.getString(9));
				if (defaultValue == null)
					defaultValue = "";
				sb.append("--#SET :VARIABLE:" + dstSchema + ":" + varName
						+ linesep);
				db2DropObjectsWriter.write("DROP VARIABLE "
						+ putQuote(dstSchema) + "." + putQuote(varName) + ";"
						+ linesep);
				if (dbSourceName.equalsIgnoreCase("db2")) {
					varType = composeType(typeSchema, typeName, length, scale,
							codePage, schema);
					if ((readOnly.equals("C")) && (defaultValue.length() > 0))
						sb.append("CREATE VARIABLE " + putQuote(dstSchema)
								+ "." + putQuote(varName) + " " + varType
								+ " CONSTANT " + defaultValue);
					else if ((readOnly.equals("N"))
							&& (defaultValue.length() > 0))
						sb.append("CREATE VARIABLE " + putQuote(dstSchema)
								+ "." + putQuote(varName) + " " + varType
								+ " DEFAULT " + defaultValue);
					else
						sb.append("CREATE VARIABLE " + putQuote(dstSchema)
								+ "." + putQuote(varName) + " " + varType);
					sb.append(linesep + sqlTerminator + linesep);
				}
				if ((objCount > 0) && (objCount % 20 == 0))
					log(objCount
							+ " numbers of Global Variables extracted for schema "
							+ schema);
				objCount++;
				db2ObjectsWriter[((Integer) plsqlHashTable.get("VARIABLE"
						.toLowerCase())).intValue()].write(sb.toString());
			}
			if (objCount > 0)
				log(objCount
						+ " Total numbers of Global Variables extracted for schema "
						+ schema);
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (Exception e) {
			log("Error in getting Global Variables  for schema " + schema
					+ " SQL=" + sql);
			e.printStackTrace();
		}
	}

	private void genMaterializedViews(String schema) {
		StringBuffer sb = new StringBuffer();
		String sql = "";
		String query = "";
		String dstSchema = getSrctoDstSchema(schema);
		String columnList = "";

		ResultSet Reader = null;

		if (dbSourceName.equalsIgnoreCase("oracle"))
			sql = "SELECT MVIEW_NAME, QUERY FROM DBA_MVIEWS WHERE OWNER = '"
					+ schema + "'";
		else if (dbSourceName.equalsIgnoreCase("db2")) {
			sql = "SELECT V.VIEWNAME, V.TEXT FROM SYSCAT.VIEWS V, SYSCAT.TABLES T WHERE V.VIEWSCHEMA = T.TABSCHEMA AND V.VIEWNAME = T.TABNAME AND T.TYPE = 'S' AND V.VIEWSCHEMA = '"
					+ schema + "'";
		}

		if (sql.equals(""))
			return;

		try {
			PreparedStatement queryStatement = mainConn.prepareStatement(sql);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			int objCount = 0;
			while (Reader.next()) {
				sb.setLength(0);
				String mview_name = Reader.getString(1);
				query = Reader.getString(2);
				sb.append("--#SET :MQT:" + dstSchema + ":" + mview_name
						+ linesep);
				db2DropObjectsWriter.write("DROP TABLE " + putQuote(dstSchema)
						+ "." + putQuote(mview_name) + ";" + linesep);
				if (dbSourceName.equalsIgnoreCase("db2")) {
					sb.append(query + linesep + sqlTerminator + linesep);
				} else {
					columnList = getColumnList(schema, mview_name);
					sb.append("CREATE TABLE " + putQuote(dstSchema) + "."
							+ putQuote(mview_name) + linesep);
					if (columnList.length() > 0) {
						sb.append("(" + columnList + ")" + linesep);
					}
					sb.append("AS (" + linesep + query + linesep + ")"
							+ linesep);
					sb.append("DATA INITIALLY DEFERRED REFRESH IMMEDIATE"
							+ linesep);
					sb.append("ENABLE QUERY OPTIMIZATION" + linesep);
					sb.append("MAINTAINED BY SYSTEM" + linesep + sqlTerminator
							+ linesep + linesep);
					sb.append("REFRESH TABLE " + putQuote(schema) + "."
							+ putQuote(mview_name) + linesep + sqlTerminator
							+ linesep + linesep);
					sb.append("RUNSTATS ON TABLE " + putQuote(schema) + "."
							+ putQuote(mview_name)
							+ "ON ALL COLUMNS WITH DISTRIBUTION" + linesep);
					sb.append("ON ALL COLUMNS AND DETAILED INDEXES ALL"
							+ linesep + sqlTerminator + linesep + linesep);
				}
				if ((objCount > 0) && (objCount % 20 == 0))
					log(objCount
							+ " numbers of materialized views extracted for schema "
							+ schema);
				objCount++;
				db2mviewWriter.write(sb.toString());
			}
			if (objCount > 0)
				log(objCount
						+ " Total numbers of materialized views extracted for schema "
						+ schema);
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (Exception e) {
			log("Error in getting materialized query table for schema "
					+ schema + " SQL=" + sql);
			e.printStackTrace();
		}
	}

	private void genRoles() throws SQLException, IOException {
		String sql = "";
		String ddlSQL = "";
		String templateSQL = "SELECT PRIVILEGE, OWNER, TABLE_NAME, ROLE  FROM ROLE_TAB_PRIVS WHERE ROLE = '&roleName&' AND PRIVILEGE in ('ALTER','INSERT','UPDATE','SELECT','DELETE','INDEX','REFERENCES')";

		ResultSet Reader = null;
		ResultSet Reader2 = null;

		if (dbSourceName.equalsIgnoreCase("oracle")) {
			sql = "SELECT ROLE FROM ROLE_TAB_PRIVS WHERE ROLE NOT IN ('IMP_FULL_DATABASE','EXECUTE_CATALOG_ROLE','DELETE_CATALOG_ROLE','HS_ADMIN_ROLE','GATHER_SYSTEM_STATISTICS','SELECT_CATALOG_ROLE','PLUSTRACE','DBA','XDBADMIN','EXP_FULL_DATABASE', 'WM_ADMIN_ROLE') GROUP BY ROLE";
		}

		if (sql.equals(""))
			return;
		PreparedStatement queryStatement = mainConn.prepareStatement(sql);
		queryStatement.setFetchSize(fetchSize);
		Reader = queryStatement.executeQuery();
		int objCount = 0;
		while (Reader.next()) {
			String roleName = Reader.getString(1);
			db2rolePrivsWriter.write("--#SET :ROLE:ROLE:" + roleName + linesep);
			db2DropObjectsWriter.write("DROP ROLE " + roleName + ";" + linesep);
			db2rolePrivsWriter.write("CREATE ROLE \"" + roleName + "\";"
					+ linesep + linesep);
			ddlSQL = templateSQL.replace("&roleName&", roleName);
			PreparedStatement queryStatement2 = mainConn
					.prepareStatement(ddlSQL);
			Reader2 = queryStatement2.executeQuery();
			while (Reader2.next()) {
				String outStr = getSrctoDstSchema(Reader2.getString(2));
				if ((objCount > 0) && (objCount % 20 == 0))
					log(objCount + " numbers of Roles extracted");
				objCount++;
				db2rolePrivsWriter.write("GRANT " + Reader2.getString(1)
						+ " ON \"" + outStr + "\".\"" + Reader2.getString(3)
						+ "\" TO ROLE \"" + Reader2.getString(4) + "\";"
						+ linesep);
			}
			if (Reader2 != null)
				Reader2.close();
			if (queryStatement2 != null)
				queryStatement2.close();
			ddlSQL = "SELECT USERNAME FROM USER_ROLE_PRIVS WHERE GRANTED_ROLE = '"
					+ roleName + "'";
			queryStatement2 = mainConn.prepareStatement(ddlSQL);
			Reader2 = queryStatement2.executeQuery();
			while (Reader2.next()) {
				String outStr = getSrctoDstSchema(Reader2.getString(1));
				db2rolePrivsWriter.write(linesep + "GRANT ROLE \"" + roleName
						+ "\" TO USER \"" + outStr + "\";" + linesep + linesep);
				objCount++;
			}
			if (Reader2 != null)
				Reader2.close();
			if (queryStatement2 != null)
				queryStatement2.close();
		}
		if (objCount > 0)
			log(objCount + " numbers of grants/roles extracted");
		if (Reader != null)
			Reader.close();
		if (queryStatement != null)
			queryStatement.close();
	}

	private void genAllSequences() {
		String schema2 = "";
		String schema3 = "";
		try {
			schema2 = removeQuote(srcSchName[0]);
			schema3 = "'" + schema2 + "'";
			for (int i = 1; i < totalTables; i++) {
				String schema = removeQuote(srcSchName[i]);
				if (schema.equalsIgnoreCase(schema2))
					continue;
				schema3 = schema3 + ",'" + removeQuote(schema) + "'";
				schema2 = schema;
			}

			if (dbSourceName.equalsIgnoreCase("oracle")) {
				genSequences(schema3);
			} else if (dbSourceName.equalsIgnoreCase("db2")) {
				genDB2Aliases(schema3);
				genDB2Sequences(schema3);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void openPLSQLFiles() {
		String plSQL = "";
		String fileName = "";
		String filName = "";
		String countSQL = "";

		ResultSet Reader = null;

		if (dbSourceName.equalsIgnoreCase("oracle")) {
			if (majorSourceDBVersion <= 8) {
				plSQL = "select distinct type from dba_source union all select 'TRIGGER' AS type from dual";
				countSQL = "select sum(C1) from (select count(distinct type) as \"C1\" from dba_source union all select 1 as \"C1\" from dual)";
			} else {
				plSQL = "select distinct type from dba_source";
				countSQL = "select count(distinct type) from dba_source";
			}
		} else if (dbSourceName.equalsIgnoreCase("idb2")) {
			plSQL = "SELECT 'PROCEDURE' FROM SYSIBM.SYSDUMMY1 UNION SELECT 'TRIGGER' FROM SYSIBM.SYSDUMMY1 UNION SELECT 'FUNCTION' FROM SYSIBM.SYSDUMMY1";

			countSQL = "SELECT 3 FROM SYSIBM.SYSDUMMY1";
		} else if (dbSourceName.equalsIgnoreCase("db2")) {
			plSQL = "SELECT 'MODULE' FROM SYSIBM.SYSDUMMY1 UNION  SELECT 'VARIABLE' FROM SYSIBM.SYSDUMMY1 UNION  SELECT 'GRANTS' FROM SYSIBM.SYSDUMMY1 UNION  SELECT 'JAVAJARS' FROM SYSIBM.SYSDUMMY1 UNION  SELECT 'XMLSCHEMA' FROM SYSIBM.SYSDUMMY1 UNION SELECT 'TYPE' FROM SYSIBM.SYSDUMMY1 UNION SELECT 'ROUTINE' FROM SYSIBM.SYSDUMMY1 UNION SELECT 'PROCEDURE' FROM SYSIBM.SYSDUMMY1 UNION SELECT 'TRIGGER' FROM SYSIBM.SYSDUMMY1 UNION SELECT 'FUNCTION' FROM SYSIBM.SYSDUMMY1";

			countSQL = "SELECT 10 FROM SYSIBM.SYSDUMMY1";
		}

		if (plSQL.equals("")) {
			return;
		}
		try {
			PreparedStatement queryStatement = mainConn
					.prepareStatement(countSQL);
			Reader = queryStatement.executeQuery();
			int n = 0;
			if (Reader.next()) {
				n = Reader.getInt(1);
			}
			if (Reader != null)
				Reader.close();
			if (queryStatement != null) {
				queryStatement.close();
			}
			db2ObjectsWriter = new BufferedWriter[n];

			queryStatement = mainConn.prepareStatement(plSQL);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			int i = 0;
			while (Reader.next()) {
				try {
					String type = Reader.getString(1).trim();
					type = type.replace(" ", "_").toLowerCase();
					filName = "db2" + type + ".db2";
					fileName = OUTPUT_DIR + filName;
					plsqlHashTable.put(type, new Integer(i));
					db2ObjectsWriter[i] = new BufferedWriter(new FileWriter(
							fileName, false));
					String addstr = "";
					if ((dbTargetName.equals("zdb2"))
							|| ((dbTargetName.equals("db2luw")) && (!remoteLoad))) {
						IBMExtractUtilities.putHelpInformation(
								db2ObjectsWriter[i], filName);
						db2ObjectsWriter[i].write("--#SET TERMINATOR "
								+ sqlTerminator + linesep);
						if (db2dbname.equals("SAMPLE")) {
							addstr = "-- TO CHANGE DATABASE NAME, modify IBMExtract.properties file."
									+ linesep;
							db2ObjectsWriter[i].write(addstr);
						}
						if (putConnectStatement) {
							db2ObjectsWriter[i].write("CONNECT TO " + db2dbname
									+ linesep);
							db2ObjectsWriter[i].write(sqlTerminator + linesep);
						}
					}
					if (sqlTerminator.equals("/"))
						db2ObjectsWriter[i].write("SET SQLCOMPAT PLSQL"
								+ linesep + sqlTerminator + linesep);
					i++;
				} catch (IOException e) {
					log("Error creating file " + fileName + ":"
							+ e.getMessage());
				} catch (SQLException ex) {
					log("Error getting openPLSQLFiles " + ex.getMessage());
					ex.printStackTrace();
				}
			}
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void regenerateTriggers() {
		String fileName = "db2trigger.db2";
		String inFileName = OUTPUT_DIR + fileName;
		try {
			if (IBMExtractUtilities.FileExists(inFileName)) {
				String outFileName = OUTPUT_DIR
						+ fileName.substring(0, fileName.lastIndexOf('.'))
						+ "_Original"
						+ fileName.substring(fileName.lastIndexOf('.'));
				IBMExtractUtilities.copyFile(new File(inFileName), new File(
						outFileName));
				String text = OraToDb2Converter
						.getSplitTriggers(IBMExtractUtilities
								.FileContents(outFileName));
				BufferedWriter out = new BufferedWriter(new FileWriter(
						inFileName, false));
				out.write(text);
				out.close();
				log("File " + inFileName + " saved as " + outFileName);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void fixiDB2Code(String fileName) {
		String inFileName = OUTPUT_DIR + fileName;
		String outFileName = OUTPUT_DIR;
		try {
			if (IBMExtractUtilities.FileExists(inFileName)) {
				outFileName = OUTPUT_DIR
						+ fileName.substring(0, fileName.lastIndexOf('.'))
						+ "_Original"
						+ fileName.substring(fileName.lastIndexOf('.'));
				IBMExtractUtilities.copyFile(new File(inFileName), new File(
						outFileName));
				String text = OraToDb2Converter
						.fixiDB2Procedures(IBMExtractUtilities
								.FileContents(outFileName));
				BufferedWriter out = new BufferedWriter(new FileWriter(
						inFileName, false));
				out.write(text);
				out.close();
				log("File " + inFileName + " saved as " + outFileName);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void closePLSQLFiles() {
		int len = db2ObjectsWriter.length;
		for (int i = 0; i < len; i++) {
			try {
				if ((dbTargetName.equals("zdb2"))
						|| ((dbTargetName.equals("db2luw")) && (!remoteLoad))) {
					if (putConnectStatement) {
						db2ObjectsWriter[i].write("TERMINATE" + linesep);
						db2ObjectsWriter[i].write(sqlTerminator + linesep);
					}
				}
				db2ObjectsWriter[i].close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void getPLSQLSource(String schema, String type, String objectName,
			StringBuffer buffer) {
		ResultSet Reader = null;
		String dstSchema = getSrctoDstSchema(schema);
		String plSQL = "select text from dba_source where owner = '"
				+ dstSchema + "' " + "and name = '" + objectName
				+ "' and type = '" + type + "' order by line asc";
		try {
			if (debug)
				log("getPLSQLSource: sql=" + plSQL);
			PreparedStatement queryStatement = mainConn.prepareStatement(plSQL);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			int objCount = 0;
			while (Reader.next()) {
				try {
					if (objCount == 0) {
						buffer.append("CREATE OR REPLACE "
								+ Reader.getString(1));
						objCount++;
					} else {
						buffer.append(Reader.getString(1));
					}
				} catch (SQLException ex) {
					log("Error getting PL/SQL " + ex.getMessage());
					ex.printStackTrace();
				}
			}
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private boolean invalidOracleObject(String owner, String objectType,
			String objectName) {
		String sql = "SELECT 'X' FROM DBA_OBJECTS WHERE OWNER = '" + owner
				+ "' " + "AND OBJECT_TYPE = '" + objectType + "' "
				+ "AND OBJECT_NAME = '" + objectName + "' "
				+ "AND STATUS = 'INVALID'";

		String found = executeSQL(sql, false);
		return found.equals("X");
	}

	private void genPLSQL(String schema) {
		String plSQL = "";
		String plsqlTemplate = "select dbms_metadata.get_ddl('&type&','&name&','&schema&') from dual";

		ResultSet Reader = null;
		ResultSet plsqlReader = null;
		StringBuffer buffer = new StringBuffer();
		StringBuffer chunks = new StringBuffer();
		String dstSchema = getSrctoDstSchema(schema);

		plSQL = "select type, name from dba_source where owner = '" + schema
				+ "' group by type, name";

		if (plSQL.equals(""))
			return;

		try {
			PreparedStatement queryStatement = mainConn.prepareStatement(plSQL);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			int objCount = 0;
			while (Reader.next()) {
				try {
					buffer.setLength(0);
					chunks.setLength(0);
					String origType = Reader.getString(1);
					String name = Reader.getString(2);
					String type = origType.replace(" ", "_");
					String newType = type;
					if (type.equals("PACKAGE")) {
						newType = "PACKAGE_SPEC";
					}
					buffer.append("SET CURRENT SCHEMA = '" + dstSchema + "'"
							+ linesep);
					buffer.append(sqlTerminator + linesep);
					buffer.append("--#SET :" + type + ":" + dstSchema + ":"
							+ name + linesep);
					if (invalidOracleObject(schema, type, name))
						buffer.append("--#WARNING :" + type + " : " + schema
								+ "." + name
								+ " is found to be invalid in source database"
								+ linesep);
					if ((!type.equalsIgnoreCase("trigger"))
							&& (!type.equalsIgnoreCase("package_body"))) {
						db2DropObjectsWriter.write("DROP " + origType + " "
								+ putQuote(dstSchema) + "." + putQuote(name)
								+ ";" + linesep);
					}
					String ddlSQL = plsqlTemplate.replace("&schema&", schema);
					ddlSQL = ddlSQL.replace("&name&", name);
					ddlSQL = ddlSQL.replace("&type&", newType);
					if (debug)
						log("genPLSQL: ddlSQL=" + ddlSQL + " name=" + name
								+ " schema=" + schema);
					try {
						PreparedStatement plsqlStatement = mainConn
								.prepareStatement(ddlSQL);
						plsqlReader = plsqlStatement.executeQuery();
						if (plsqlReader.next()) {
							IBMExtractUtilities.getStringChunks(plsqlReader, 1,
									chunks);
							if (chunks.length() == 0)
								getPLSQLSource(schema, origType, name, chunks);
						}
						if (plsqlReader != null)
							plsqlReader.close();
						if (plsqlStatement != null)
							plsqlStatement.close();
					} catch (Exception ex) {
						if (chunks.length() == 0)
							getPLSQLSource(schema, origType, name, chunks);
					}
					if (chunks.length() > 0) {
						buffer.append(OraToDb2Converter.getDb2PlSql(chunks
								.toString()));
						buffer.append(linesep + sqlTerminator + linesep);
					}
					if ((objCount > 0) && (objCount % 20 == 0))
						log(objCount
								+ " numbers of PL/SQL objects extracted for schema "
								+ schema);
					objCount++;
					db2ObjectsWriter[((Integer) plsqlHashTable.get(type
							.toLowerCase())).intValue()].write(buffer
							.toString());
				} catch (IOException e) {
					log("Error writing PL/SQL in file " + e.getMessage());
				} catch (SQLException ex) {
					log("Error getting PL/SQL " + ex.getMessage());
					ex.printStackTrace();
				}
			}
			if (objCount > 0)
				log(objCount
						+ " Total numbers of PL/SQL objects extracted for schema "
						+ schema);
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private String getRoutineOpts(String schema, String specificName) {
		StringBuffer buffer = new StringBuffer();

		ResultSet Reader = null;
		String plSQL = "SELECT isolation, blocking, insert_buf, reoptvar, queryopt, sqlmathwarn, degree, intra_parallel, refreshage FROM SYSCAT.PACKAGES, SYSCAT.ROUTINEDEP WHERE pkgschema = bschema AND pkgname = bname AND btype = 'K' AND routineschema = '"
				+ schema + "' " + "AND specificname = '" + specificName + "'";
		try {
			if (debug)
				log("getRoutineOpts: sql=" + plSQL);
			PreparedStatement queryStatement = mainConn.prepareStatement(plSQL);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			if (Reader.next()) {
				String blocking = trim(Reader.getString(2));
				if (blocking.equalsIgnoreCase("N"))
					buffer.append("BLOCKING NO");
				else if (blocking.equalsIgnoreCase("U"))
					buffer.append("BLOCKING UNAMBIG");
				else if (blocking.equalsIgnoreCase("B"))
					buffer.append("BLOCKING ALL");
				buffer.append(" DEGREE " + trim(Reader.getString(7)));
				String insert = trim(Reader.getString(2));
				if (insert.equalsIgnoreCase("Y"))
					buffer.append(" INSERT BUF ");
				else if (insert.equalsIgnoreCase("N"))
					buffer.append(" INSERT DEF ");
				buffer.append(" ISOLATION " + trim(Reader.getString(1)));
				buffer.append(" QUERYOPT " + trim(Reader.getString(5)));
				String reoptVar = trim(Reader.getString(4));
				if (reoptVar.equalsIgnoreCase("N"))
					buffer.append(" REOPT NONE ");
				else if (reoptVar.equalsIgnoreCase("A"))
					buffer.append(" REOPT ALWAYS ");
				else if (reoptVar.equalsIgnoreCase("O"))
					buffer.append(" REOPT ONCE ");
			}
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return buffer.toString();
	}

	private String genTypeCols(int code, String schema, String typeModule,
			String typeName) {
		String sql = "";

		ResultSet Reader = null;
		StringBuffer buffer = new StringBuffer();

		if (dbSourceName.equalsIgnoreCase("db2")) {
			if ((majorSourceDBVersion >= 9) && (minorSourceDBVersion >= 7)) {
				if (code == 1) {
					sql = "SELECT FIELDNAME, FIELDTYPESCHEMA, FIELDTYPEMODULENAME, FIELDTYPENAME, LENGTH, SCALE, CODEPAGE FROM SYSCAT.ROWFIELDS WHERE TYPENAME = '"
							+ typeName
							+ "' "
							+ "AND TYPEMODULENAME ='"
							+ typeModule
							+ "' "
							+ "AND TYPESCHEMA='"
							+ schema
							+ "' " + "ORDER BY ORDINAL";
				} else if (code == 2) {
					sql = "SELECT FIELDNAME, FIELDTYPESCHEMA, FIELDTYPEMODULENAME, FIELDTYPENAME, LENGTH, SCALE, CODEPAGE FROM SYSCAT.ROWFIELDS WHERE TYPESCHEMA ='"
							+ schema
							+ "' "
							+ "AND TYPEMODULENAME IS NULL "
							+ "AND TYPENAME = '"
							+ typeName
							+ "' "
							+ "ORDER BY ORDINAL";
				} else {
					sql = "SELECT ATTR_NAME, ATTR_TYPESCHEMA, TYPEMODULENAME, ATTR_TYPENAME , LENGTH, SCALE, CODEPAGE, TARGET_TYPESCHEMA,TARGET_TYPENAME,LOGGED,COMPACT FROM SYSCAT.ATTRIBUTES WHERE TYPENAME = SOURCE_TYPENAME AND TYPESCHEMA = SOURCE_TYPESCHEMA AND TYPESCHEMA='"
							+ schema
							+ "' "
							+ "AND TYPENAME='"
							+ typeName
							+ "' " + "ORDER BY ORDINAL";
				}

			}

		}

		if (sql.equals(""))
			return "";
		try {
			PreparedStatement queryStatement = mainConn.prepareStatement(sql);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			int objCount = 0;
			while (Reader.next()) {
				try {
					String fieldName = trim(Reader.getString(1));
					String fieldtypeSchema = trim(Reader.getString(2));
					String fieldTypeName = trim(Reader.getString(4));
					int length = Reader.getInt(5);
					int scale = Reader.getInt(6);
					int codePage = Reader.getInt(7);
					String dataType = composeType(fieldtypeSchema,
							fieldTypeName, length, scale, codePage, schema);
					if (objCount == 0) {
						buffer.append(putQuote(fieldName) + " " + dataType);
					} else {
						buffer.append("," + linesep + putQuote(fieldName) + " "
								+ dataType);
					}
					objCount++;
				} catch (SQLException ex) {
					log("Error getting field names for type " + ex.getMessage()
							+ sql);
					ex.printStackTrace();
				}
			}
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (SQLException e) {
			log("Error getting field names for type " + sql);
			e.printStackTrace();
		}
		return buffer.length() > 0 ? linesep + buffer.toString() + linesep : "";
	}

	private void genSQLModules(String schema) {
		String sql = "";
		String moduleList = "";

		ResultSet Reader = null;
		StringBuffer buffer = new StringBuffer();
		StringBuffer chunks = new StringBuffer();
		String dstSchema = getSrctoDstSchema(schema);

		if (dbSourceName.equalsIgnoreCase("db2")) {
			if ((majorSourceDBVersion >= 9) && (minorSourceDBVersion >= 7)) {
				moduleList = executeSQL(
						"SELECT MODULENAME FROM SYSCAT.MODULES WHERE MODULESCHEMA ='"
								+ schema + "' AND DIALECT = 'DB2 SQL PL'",
						false);
			}
		}

		if (moduleList.equals(""))
			return;

		String[] modules = moduleList.split("~");
		for (int i = 0; i < modules.length; i++) {
			buffer.setLength(0);
			String module = modules[i];
			buffer.append("CREATE MODULE " + putQuote(dstSchema) + "."
					+ putQuote(modules[i]) + linesep + sqlTerminator + linesep);
			PreparedStatement queryStatement;
			try {
				sql = "SELECT DISTINCT B.CONDNAME,B.SQLSTATE,B.REMARKS, A.PUBLISHED FROM SYSCAT.MODULEOBJECTS A, SYSCAT.CONDITIONS B WHERE A.OBJECTTYPE ='CONDITION' AND A.OBJECTNAME= B.CONDNAME AND A.OBJECTMODULENAME= B.CONDMODULENAME AND A.OBJECTSCHEMA= B.CONDSCHEMA AND A.OBJECTSCHEMA='"
						+ schema
						+ "' "
						+ "AND A.OBJECTMODULENAME ='"
						+ module
						+ "' ";

				queryStatement = mainConn.prepareStatement(sql);
				queryStatement.setFetchSize(fetchSize);
				Reader = queryStatement.executeQuery();
				int objCount = 0;
				while (Reader.next()) {
					try {
						chunks.setLength(0);
						String condName = trim(Reader.getString(1));
						String sqlState = trim(Reader.getString(2));
						String published = trim(Reader.getString(4));
						db2DropObjectsWriter.write("ALTER MODULE "
								+ putQuote(dstSchema) + "." + putQuote(module)
								+ linesep);
						db2DropObjectsWriter.write(" DROP CONDITION "
								+ putQuote(condName) + linesep + sqlTerminator
								+ linesep);
						buffer.append("ALTER MODULE " + putQuote(dstSchema)
								+ "." + putQuote(module) + linesep);
						published = (published != null)
								&& (published.equals("Y")) ? "PUBLISH" : "ADD";
						buffer.append(published + " CONDITION "
								+ putQuote(condName) + " FOR SQLSTATE '"
								+ sqlState + "'" + linesep + sqlTerminator
								+ linesep);
						db2ObjectsWriter[((Integer) plsqlHashTable.get("MODULE"
								.toLowerCase())).intValue()].write(buffer
								.toString());
						if ((objCount > 0) && (objCount % 20 == 0))
							log(objCount
									+ " numbers of SQL Module conditions extracted for module "
									+ schema + "." + module);
						objCount++;
					} catch (IOException e) {
						log("Error writing DB2 SQL Module conditions in file "
								+ e.getMessage() + sql);
					} catch (SQLException ex) {
						log("Error getting DB2 SQL Modules Conditions "
								+ ex.getMessage() + sql);
						ex.printStackTrace();
					}
				}
				if (objCount > 0)
					log(objCount
							+ " Total numbers of DB2 SQL Module Conditions extracted for module "
							+ schema + "." + module);
				if (Reader != null)
					Reader.close();
				if (queryStatement != null)
					queryStatement.close();
			} catch (SQLException e) {
				log("Error in extracting DB2 SQL Modules " + sql);
				e.printStackTrace();
			}

			try {
				sql = "SELECT B.ROUTINENAME, B.ROUTINETYPE,B.SPECIFICNAME,B.LANGUAGE,A.PUBLISHED,B.TEXT FROM SYSCAT.MODULEOBJECTS A, SYSCAT.ROUTINES B WHERE A.OBJECTTYPE IN ('PROCEDURE','FUNCTION') AND A.OBJECTMODULENAME= B.ROUTINEMODULENAME  AND A.OBJECTSCHEMA= B.ROUTINESCHEMA AND A.SPECIFICNAME= B.SPECIFICNAME AND A.OBJECTNAME= B.ROUTINENAME AND A.OBJECTSCHEMA='"
						+ schema
						+ "' "
						+ "AND A.OBJECTMODULENAME ='"
						+ module
						+ "' " + "AND ORIGIN <> 'S'";

				queryStatement = mainConn.prepareStatement(sql);
				queryStatement.setFetchSize(fetchSize);
				Reader = queryStatement.executeQuery();
				int objCount = 0;
				while (Reader.next()) {
					try {
						buffer.setLength(0);
						chunks.setLength(0);
						String routineType = trim(Reader.getString(2));
						routineType = routineType.equals("P") ? "PROCEDURE"
								: "FUNCTION";
						String specificName = trim(Reader.getString(3));
						String language = trim(Reader.getString(4));
						if (!language.equals("SQL")) {
							buffer
									.append("-- Extraction for NON-SQL Procedure from modules is not yet supported. Please report this issue.");
						}
						IBMExtractUtilities.getStringChunks(Reader, 6, chunks);
						db2DropObjectsWriter.write("ALTER MODULE "
								+ putQuote(dstSchema) + "." + putQuote(module)
								+ linesep);
						db2DropObjectsWriter.write(" DROP SPECIFIC "
								+ routineType + " " + putQuote(specificName)
								+ linesep + sqlTerminator + linesep);
						buffer.append(chunks + linesep + sqlTerminator
								+ linesep);
						db2ObjectsWriter[((Integer) plsqlHashTable.get("MODULE"
								.toLowerCase())).intValue()].write(buffer
								.toString());
						if ((objCount > 0) && (objCount % 20 == 0))
							log(objCount
									+ " numbers of SQL Module Procedures / Functions extracted for module "
									+ schema + "." + module);
						objCount++;
					} catch (IOException e) {
						log("Error writing DB2 SQL Module Procedures / Functions  in file "
								+ e.getMessage() + sql);
					} catch (SQLException ex) {
						log("Error getting DB2 SQL Modules Procedures / Functions  "
								+ ex.getMessage() + sql);
						ex.printStackTrace();
					}
				}
				if (objCount > 0)
					log(objCount
							+ " Total numbers of DB2 SQL Module Procedures / Functions  extracted for module "
							+ schema + "." + module);
				if (Reader != null)
					Reader.close();
				if (queryStatement != null)
					queryStatement.close();
			} catch (SQLException e) {
				log("Error in extracting DB2 SQL Modules Procedures / Functions "
						+ sql);
				e.printStackTrace();
			}

			try {
				sql = "SELECT B.TYPENAME, B.METATYPE, B.SOURCESCHEMA, B.SOURCEMODULENAME,B.SOURCENAME, B.LENGTH,B.SCALE, B.CODEPAGE, B.INSTANTIABLE, B.FINAL, B.ARRAY_LENGTH, A.PUBLISHED FROM SYSCAT.MODULEOBJECTS A, SYSCAT.DATATYPES B WHERE A.OBJECTTYPE ='TYPE' AND A.OBJECTMODULENAME= B.TYPEMODULENAME AND A.OBJECTSCHEMA= B.TYPESCHEMA AND A.OBJECTNAME = B.TYPENAME AND A.OBJECTSCHEMA='"
						+ schema
						+ "' "
						+ "AND A.OBJECTMODULENAME ='"
						+ module
						+ "' ";

				queryStatement = mainConn.prepareStatement(sql);
				queryStatement.setFetchSize(fetchSize);
				Reader = queryStatement.executeQuery();
				int objCount = 0;
				while (Reader.next()) {
					try {
						buffer.setLength(0);
						String typeName = trim(Reader.getString(1));
						String metaType = trim(Reader.getString(2));
						String sourceSchema = trim(Reader.getString(3));
						String sourceName = trim(Reader.getString(5));
						int length = Reader.getInt(6);
						int scale = Reader.getInt(7);
						int codePage = Reader.getInt(8);
						int arrayLength = Reader.getInt(11);
						String colType = "";
						String published = trim(Reader.getString(12));
						published = published.equals("Y") ? "PUBLISH" : "ADD";
						db2DropObjectsWriter.write("ALTER MODULE "
								+ putQuote(dstSchema) + "." + putQuote(module)
								+ linesep);
						db2DropObjectsWriter.write(" DROP TYPE "
								+ putQuote(typeName) + linesep + sqlTerminator
								+ linesep);
						buffer.append("ALTER MODULE " + putQuote(dstSchema)
								+ "." + putQuote(module) + linesep);
						if (metaType.equals("F")) {
							colType = genTypeCols(1, schema, module, typeName);
							buffer.append(published + " TYPE "
									+ putQuote(typeName) + " AS ROW ("
									+ colType + ")");
						} else if (metaType.equals("A")) {
							colType = composeType(sourceSchema, sourceName,
									length, scale, codePage, schema);
							buffer.append(published + " TYPE "
									+ putQuote(typeName) + " AS " + colType
									+ " ARRAY [" + arrayLength + "]");
						} else if (metaType.equals("T")) {
							colType = composeType(sourceSchema, sourceName,
									length, scale, codePage, schema);
							String comp = "";
							if ((sourceSchema.equalsIgnoreCase("SYSIBM"))
									&& (!sourceName.equals("BLOB"))
									&& (!sourceName.equals("CLOB"))
									&& (!sourceName.equals("DBCLOB"))) {
								comp = " WITH COMPARISONS ";
							}
							buffer.append(published + " TYPE "
									+ putQuote(typeName) + " AS " + colType
									+ comp);
						} else {
							String sql2 = "SELECT  distinct BSCHEMA||'~'||NVL(BMODULENAME,'NULL')||'~'|| BNAME||'~'||NVL(T.TYPESCHEMA,'NULL')||'~'||NVL(T.TYPEMODULENAME,'NULL')  FROM SYSCAT.DATATYPEDEP D  LEFT OUTER JOIN SYSCAT.DATATYPES T  ON  NVL(D.BMODULENAME,'')  = NVL(T.TYPEMODULENAME,'')  AND NVL (D.BMODULEID,0) = NVL(T.TYPEMODULEID,0)  AND D.BNAME = T.TYPENAME  AND D.BSCHEMA = T.TYPESCHEMA  WHERE D.TYPESCHEMA = '"
									+ schema
									+ "' "
									+ " AND D.TYPEMODULENAME ='"
									+ module
									+ "' "
									+ " AND D.TYPENAME = '"
									+ typeName
									+ "' " + " AND D.BTYPE ='R'";

							String curType = executeSQL(sql2, false);
							String[] types = curType.split("~");
							if ((types[0].equals(types[3]))
									&& (types[1].equals(types[4])))
								colType = putQuote(types[1]) + "."
										+ putQuote(types[2]);
							else
								colType = putQuote(types[3]) + "."
										+ putQuote(types[4]) + "."
										+ putQuote(types[2]);
							buffer.append(published + " TYPE "
									+ putQuote(typeName) + " AS " + colType);
						}
						buffer.append(linesep + sqlTerminator + linesep);
						db2ObjectsWriter[((Integer) plsqlHashTable.get("MODULE"
								.toLowerCase())).intValue()].write(buffer
								.toString());
						if ((objCount > 0) && (objCount % 20 == 0))
							log(objCount
									+ " numbers of SQL Module Types extracted for module "
									+ schema + "." + module);
						objCount++;
					} catch (IOException e) {
						log("Error writing DB2 SQL Module Types  in file "
								+ e.getMessage() + sql);
					} catch (SQLException ex) {
						log("Error getting DB2 SQL Modules Types  "
								+ ex.getMessage() + sql);
						ex.printStackTrace();
					}
				}
				if (objCount > 0)
					log(objCount
							+ " Total numbers of DB2 SQL Module Types  extracted for module "
							+ schema + "." + module);
				if (Reader != null)
					Reader.close();
				if (queryStatement != null)
					queryStatement.close();
			} catch (SQLException e) {
				log("Error in extracting DB2 SQL Modules Types " + sql);
				e.printStackTrace();
			}

			try {
				sql = "SELECT A.PUBLISHED,VARNAME,TYPESCHEMA,TYPEMODULENAME,TYPENAME,LENGTH,SCALE,CODEPAGE, READONLY,REMARKS, DEFAULT FROM SYSCAT.MODULEOBJECTS A, SYSCAT.VARIABLES B WHERE A.OBJECTTYPE ='VARIABLE' AND A.OBJECTMODULENAME= B.VARMODULENAME AND A.OBJECTSCHEMA= B.VARSCHEMA AND A.OBJECTNAME = B.VARNAME AND A.OBJECTSCHEMA='TESTCASE' AND A.OBJECTMODULENAME ='CLASSES' ";

				queryStatement = mainConn.prepareStatement(sql);
				queryStatement.setFetchSize(fetchSize);
				Reader = queryStatement.executeQuery();
				int objCount = 0;
				while (Reader.next()) {
					try {
						buffer.setLength(0);
						String published = trim(Reader.getString(1));
						published = published.equals("Y") ? "PUBLISH" : "ADD";
						String varName = trim(Reader.getString(2));
						String typeSchema = trim(Reader.getString(3));
						String typeName = trim(Reader.getString(5));
						int length = Reader.getInt(6);
						int scale = Reader.getInt(7);
						int codePage = Reader.getInt(8);
						String readOnly = trim(Reader.getString(9));
						String defaultValue = trim(Reader.getString(11));
						if (defaultValue == null)
							defaultValue = "";
						db2DropObjectsWriter.write("ALTER MODULE "
								+ putQuote(dstSchema) + "." + putQuote(module)
								+ linesep);
						db2DropObjectsWriter.write(" DROP VARIABLE "
								+ putQuote(varName) + ";" + linesep);
						buffer.append("ALTER MODULE " + putQuote(dstSchema)
								+ "." + putQuote(module) + linesep);
						String varType = composeType(typeSchema, typeName,
								length, scale, codePage, schema);
						if ((readOnly.equals("C"))
								&& (defaultValue.length() > 0))
							buffer.append(published + " VARIABLE "
									+ putQuote(dstSchema) + "."
									+ putQuote(varName) + " " + varType
									+ " CONSTANT " + defaultValue);
						else if ((readOnly.equals("N"))
								&& (defaultValue.length() > 0))
							buffer.append(published + " VARIABLE "
									+ putQuote(dstSchema) + "."
									+ putQuote(varName) + " " + varType
									+ " DEFAULT " + defaultValue);
						else
							buffer.append(published + " VARIABLE "
									+ putQuote(dstSchema) + "."
									+ putQuote(varName) + " " + varType);
						buffer.append(linesep + sqlTerminator + linesep);
						if ((objCount > 0) && (objCount % 20 == 0))
							log(objCount
									+ " numbers of SQL Module Variables extracted for module "
									+ schema + "." + module);
						objCount++;
						db2ObjectsWriter[((Integer) plsqlHashTable.get("MODULE"
								.toLowerCase())).intValue()].write(buffer
								.toString());
						if ((objCount > 0) && (objCount % 20 == 0))
							log(objCount
									+ " numbers of SQL Module Variables extracted for module "
									+ schema + "." + module);
						objCount++;
					} catch (IOException e) {
						log("Error writing DB2 SQL Module Variables  in file "
								+ e.getMessage() + sql);
					} catch (SQLException ex) {
						log("Error getting DB2 SQL Modules Variables  "
								+ ex.getMessage() + sql);
						ex.printStackTrace();
					}
				}
				if (objCount > 0)
					log(objCount
							+ " Total numbers of DB2 SQL Module Variables extracted for module "
							+ schema + "." + module);
				if (Reader != null)
					Reader.close();
				if (queryStatement != null)
					queryStatement.close();
				try {
					db2DropObjectsWriter.write("DROP MODULE "
							+ putQuote(dstSchema) + "." + putQuote(module)
							+ linesep);
					String comment = executeSQL(
							"SELECT REMARKS FROM SYSCAT.MODULES WHERE MODULESCHEMA ='"
									+ schema
									+ "' "
									+ "AND DIALECT = 'DB2 SQL PL' AND MODULENAME = '"
									+ module + "'", false);

					if ((comment != null) && (comment.length() > 0)) {
						buffer.setLength(0);
						if (comment.length() > 255)
							comment = comment.substring(1, 255);
						comment = comment.replace("'", "''");
						buffer.append("COMMENT ON MODULE "
								+ putQuote(dstSchema) + "." + putQuote(module)
								+ " IS '" + comment + "'" + linesep
								+ sqlTerminator + linesep);
						db2ObjectsWriter[((Integer) plsqlHashTable.get("MODULE"
								.toLowerCase())).intValue()].write(buffer
								.toString());
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			} catch (SQLException e) {
				log("Error in extracting DB2 SQL Modules Variable " + sql);
				e.printStackTrace();
			}
		}
	}

	private void genSQLPL(String schema) {
		String sqlPL = "";
		String routineType = "";

		ResultSet Reader = null;
		StringBuffer buffer = new StringBuffer();
		StringBuffer chunks = new StringBuffer();
		String dstSchema = getSrctoDstSchema(schema);

		if (dbSourceName.equalsIgnoreCase("idb2")) {
			sqlPL = "select specific_schema, specific_name from sysibm.routines where routine_body = 'SQL' and routine_type = 'PROCEDURE' and routine_schema = '"
					+ schema + "'";
		} else if (dbSourceName.equalsIgnoreCase("db2")) {
			sqlPL = "select routineschema, specificname, text, case when routinetype = 'P' then 'PROCEDURE' when routinetype = 'F' then 'FUNCTION' end routinetype from syscat.routines where routinetype in ('F','P') and language = 'SQL' and text is not null and routineschema = '"
					+ schema
					+ "' "
					+ ((majorSourceDBVersion >= 9)
							&& (minorSourceDBVersion >= 7) ? "union all select MODULESCHEMA routineschema, MODULENAME specificname, SOURCEHEADER text, case when moduletype = 'P' then 'PACKAGE' end routinetype from SYSIBM.SYSMODULES where moduletype = 'P' and MODULESCHEMA='"
							+ schema
							+ "' "
							+ "union all "
							+ "select MODULESCHEMA routineschema, MODULENAME specificname, "
							+ "SOURCEBODY text, case when moduletype = 'P' then 'PACKAGE BODY' end routinetype "
							+ "from SYSIBM.SYSMODULES "
							+ "where moduletype = 'P' "
							+ "and MODULESCHEMA='"
							+ schema + "' "
							: "");
		}

		if (sqlPL.equals(""))
			return;

		try {
			PreparedStatement queryStatement = mainConn.prepareStatement(sqlPL);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			int objCount = 0;
			while (Reader.next()) {
				try {
					buffer.setLength(0);
					chunks.setLength(0);
					String specificSchema = Reader.getString(1);
					String specificName = Reader.getString(2);
					db2DropObjectsWriter.write("DROP SPECIFIC PROCEDURE "
							+ putQuote(dstSchema) + "."
							+ putQuote(specificName) + ";" + linesep);
					if (dbSourceName.equalsIgnoreCase("db2")) {
						IBMExtractUtilities.getStringChunks(Reader, 3, chunks);
						routineType = trim(Reader.getString(4));
						buffer.append("SET CURRENT SCHEMA = '" + dstSchema
								+ "'" + linesep + sqlTerminator + linesep);
						buffer.append("SET PATH = SYSTEM PATH, "
								+ putQuote(dstSchema) + linesep + sqlTerminator
								+ linesep);
						String preopts = getRoutineOpts(specificSchema,
								specificName);
						if (preopts.length() > 0)
							buffer.append("CALL SET_ROUTINE_OPTS('"
									+ trim(preopts) + "')" + linesep
									+ sqlTerminator + linesep);
						if (routineType.equalsIgnoreCase("FUNCTION"))
							buffer.append("--#SET :FUNCTION:" + dstSchema + ":"
									+ specificName + linesep);
						else if (routineType.equalsIgnoreCase("PROCEDURE"))
							buffer.append("--#SET :PROCEDURE:" + dstSchema
									+ ":" + specificName + linesep);
						else if (routineType.equalsIgnoreCase("PACKAGE"))
							buffer.append("--#SET :PACKAGE:" + dstSchema + ":"
									+ specificName + linesep);
						else
							buffer.append("--#SET :PACKAGE BODY:" + dstSchema
									+ ":" + specificName + linesep);
						buffer.append(chunks);
						buffer.append(linesep + sqlTerminator + linesep);
						db2ObjectsWriter[((Integer) plsqlHashTable
								.get("PROCEDURE".toLowerCase())).intValue()]
								.write(buffer.toString());
					} else {
						db2ObjectsWriter[((Integer) plsqlHashTable
								.get("PROCEDURE".toLowerCase())).intValue()]
								.write(getProcedureSource(specificSchema,
										specificName));
					}
					if ((objCount > 0) && (objCount % 20 == 0))
						log(objCount
								+ " numbers of SQL PL procedures extracted for schema "
								+ schema);
					objCount++;
				} catch (IOException e) {
					log("Error writing PL/SQL in file " + e.getMessage());
				} catch (SQLException ex) {
					log("Error getting PL/SQL " + ex.getMessage());
					ex.printStackTrace();
				}
			}
			if (objCount > 0)
				log(objCount
						+ " Total numbers of SQL PL Procedures extracted for schema "
						+ schema);
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void genDB2Types(String schema) {
		String sql = "";
		String typeStr = "";

		ResultSet Reader = null;
		StringBuffer buffer = new StringBuffer();
		String dstSchema = getSrctoDstSchema(schema);

		if (dbSourceName.equalsIgnoreCase("db2")) {
			sql = "SELECT typename, sourceschema, sourcename, length, scale, codepage, metatype, array_length FROM SYSCAT.DATATYPES WHERE typeschema = '"
					+ schema
					+ "' "
					+ "AND metatype IN ('T','A','F','R') "
					+ "AND typemodulename is null " + "ORDER BY create_time";
		}

		if (sql.equals(""))
			return;

		try {
			PreparedStatement queryStatement = mainConn.prepareStatement(sql);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			int objCount = 0;
			while (Reader.next()) {
				try {
					buffer.setLength(0);
					String typeName = Reader.getString(1);
					String sourceSchema = Reader.getString(2);
					String sourceName = Reader.getString(3);
					int length = Reader.getInt(4);
					int scale = Reader.getInt(5);
					int codePage = Reader.getInt(6);
					String metaType = Reader.getString(7);
					if (typeName != null)
						typeName = typeName.trim();
					if (sourceSchema != null)
						sourceSchema = sourceSchema.trim();
					if (sourceName != null)
						sourceName = sourceName.trim();
					db2DropObjectsWriter.write("DROP TYPE "
							+ putQuote(dstSchema) + "." + putQuote(typeName)
							+ ";" + linesep);
					if (metaType.equals("T")) {
						String comp = "";
						if ((sourceSchema.equalsIgnoreCase("SYSIBM"))
								&& (!sourceName.equals("BLOB"))
								&& (!sourceName.equals("CLOB"))
								&& (!sourceName.equals("DBCLOB"))) {
							comp = " WITH COMPARISONS ";
						}
						typeStr = "CREATE DISTINCT TYPE "
								+ putQuote(dstSchema)
								+ "."
								+ putQuote(typeName)
								+ linesep
								+ "AS "
								+ composeType(sourceSchema, sourceName, length,
										scale, codePage, schema) + comp;
					} else if (metaType.equals("A")) {
						int arrayLength = Reader.getInt(8);
						typeStr = "CREATE TYPE "
								+ putQuote(dstSchema)
								+ "."
								+ putQuote(typeName)
								+ linesep
								+ "AS "
								+ composeType(sourceSchema, sourceName, length,
										scale, codePage, schema) + " ARRAY ["
								+ arrayLength + "]";
					} else if (metaType.equals("F")) {
						String colType = genTypeCols(2, schema, null, typeName);
						typeStr = "CREATE TYPE " + putQuote(dstSchema) + "."
								+ putQuote(typeName) + linesep + "AS ROW ("
								+ colType + ")";
					} else if (metaType.equals("R")) {
						String colType = genTypeCols(3, schema, null, typeName);
						typeStr = "CREATE TYPE " + putQuote(dstSchema) + "."
								+ putQuote(typeName) + linesep + "AS ("
								+ colType + ")" + linesep + "MODE DB2SQL";
					}

					db2ObjectsWriter[((Integer) plsqlHashTable.get("TYPE"
							.toLowerCase())).intValue()].write(typeStr
							+ linesep + sqlTerminator + linesep);
					if ((objCount > 0) && (objCount % 20 == 0))
						log(objCount
								+ " numbers of TYPES extracted for schema "
								+ schema);
					objCount++;
				} catch (IOException e) {
					log("Error writing Types in file " + e.getMessage());
				} catch (SQLException ex) {
					log("Error getting Types " + ex.getMessage());
					ex.printStackTrace();
				}
			}
			if (objCount > 0)
				log(objCount + " Total numbers of Types extracted for schema "
						+ schema);
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void genSQLFunctions(String schema) {
		String sql = "";

		ResultSet Reader = null;
		StringBuffer buffer = new StringBuffer();
		String dstSchema = getSrctoDstSchema(schema);

		if (dbSourceName.equalsIgnoreCase("idb2")) {
			sql = "select function_schem, function_name, specific_name, function_type from sysibm.sqlfunctions where function_schem = '"
					+ schema + "'";
		}

		if (sql.equals(""))
			return;

		try {
			PreparedStatement queryStatement = mainConn.prepareStatement(sql);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			int objCount = 0;
			while (Reader.next()) {
				try {
					buffer.setLength(0);
					String specificSchema = Reader.getString(1);
					String specificName = Reader.getString(3);
					String functionType = Reader.getString(4);
					db2DropObjectsWriter.write("DROP SPECIFIC FUNCTION "
							+ putQuote(dstSchema) + "."
							+ putQuote(specificName) + ";" + linesep);
					db2ObjectsWriter[((Integer) plsqlHashTable.get("FUNCTION"
							.toLowerCase())).intValue()]
							.write(getFunctionSource(specificSchema,
									specificName, functionType));
					if ((objCount > 0) && (objCount % 20 == 0))
						log(objCount
								+ " numbers of SQL Functions extracted for schema "
								+ schema);
					objCount++;
				} catch (IOException e) {
					log("Error writing Functions in file " + e.getMessage());
				} catch (SQLException ex) {
					log("Error getting Functions " + ex.getMessage());
					ex.printStackTrace();
				}
			}
			if (objCount > 0)
				log(objCount
						+ " Total numbers of SQL Functions extracted for schema "
						+ schema);
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private String getViewComments(String schemaName, String tableName) {
		PreparedStatement prepStatement = null;
		ResultSet rs1 = null;
		String sql = "";
		String sql2 = "";
		String type = "VIEW";
		String dstSchema = getSrctoDstSchema(schemaName);
		StringBuffer buffer = new StringBuffer();

		if (dbSourceName.equalsIgnoreCase("oracle")) {
			sql = "SELECT COMMENTS FROM DBA_TAB_COMMENTS WHERE OWNER = '"
					+ removeQuote(schemaName) + "' AND TABLE_TYPE = '" + type
					+ "' AND TABLE_NAME = '" + removeQuote(tableName)
					+ "' AND COMMENTS IS NOT NULL";

			sql2 = "SELECT COLUMN_NAME, COMMENTS FROM DBA_COL_COMMENTS WHERE OWNER = '"
					+ removeQuote(schemaName)
					+ "' AND TABLE_NAME = '"
					+ removeQuote(tableName) + "' AND COMMENTS IS NOT NULL";
		} else if (dbSourceName.equalsIgnoreCase("idb2")) {
			sql = "SELECT TABLE_TEXT FROM SYSIBM.SQLTABLES WHERE TABLE_SCHEM = '"
					+ removeQuote(schemaName)
					+ "' AND TABLE_TYPE = '"
					+ type
					+ "' AND TABLE_NAME = '"
					+ removeQuote(tableName)
					+ "' AND TABLE_TEXT IS NOT NULL";

			sql2 = "SELECT COLUMN_NAME, COLUMN_TEXT FROM SYSIBM.SQLCOLUMNS WHERE TABLE_SCHEM = '"
					+ removeQuote(schemaName)
					+ "' AND TABLE_NAME = '"
					+ removeQuote(tableName) + "' AND COLUMN_TEXT IS NOT NULL";
		}

		if (sql.equals("")) {
			return "";
		}
		try {
			prepStatement = mainConn.prepareStatement(sql);
			rs1 = prepStatement.executeQuery();
			if (rs1.next()) {
				String comment = rs1.getString(1);
				if ((comment != null) && (comment.length() > 0)) {
					if (comment.length() > 255)
						comment = comment.substring(1, 255);
					comment = comment.replace("'", "''");
					buffer.append("COMMENT ON " + type + " " + dstSchema + "."
							+ tableName + " IS '" + comment + "'" + linesep
							+ sqlTerminator + linesep);
				}
			}
			if (rs1 != null)
				rs1.close();
			if (prepStatement != null) {
				prepStatement.close();
			}
			prepStatement = mainConn.prepareStatement(sql2);
			rs1 = prepStatement.executeQuery();
			while (rs1.next()) {
				String columnName = rs1.getString(1);
				String comment = rs1.getString(2);
				if ((comment == null) || (comment.length() <= 0))
					continue;
				if (comment.length() > 255)
					comment = comment.substring(1, 255);
				comment = comment.replace("'", "''");
				buffer.append("COMMENT ON COLUMN " + dstSchema + "."
						+ tableName + ".\"" + columnName + "\" IS '" + comment
						+ "'" + linesep + sqlTerminator + linesep);
			}

			if (rs1 != null)
				rs1.close();
			if (prepStatement != null)
				prepStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return buffer.toString();
	}

	private StringBuffer fixIDB2ShortName(String schema, StringBuffer buffer) {
		String sql = "";

		ResultSet Reader = null;

		if (dbSourceName.equalsIgnoreCase("idb2")) {
			sql = "select table_name, system_table_name from qsys2.systables where table_schema = '"
					+ schema + "'";
		}

		if (sql.equals(""))
			return buffer;

		if (mapiDB2TableNames == null) {
			mapiDB2TableNames = new Properties();
			try {
				PreparedStatement queryStatement = mainConn
						.prepareStatement(sql);
				queryStatement.setFetchSize(fetchSize);
				Reader = queryStatement.executeQuery();
				while (Reader.next()) {
					try {
						String tableName = trim(Reader.getString(1));
						String shortName = trim(Reader.getString(2));
						mapiDB2TableNames.setProperty(shortName, tableName);
					} catch (Exception ex) {
						log("Error executing " + sql);
						ex.printStackTrace();
					}
				}
				if (Reader != null)
					Reader.close();
				if (queryStatement != null)
					queryStatement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		for (Enumeration e = mapiDB2TableNames.keys(); e.hasMoreElements();) {
			int pos = -1;
			String key = (String) e.nextElement();
			String value = mapiDB2TableNames.getProperty(key);
			do {
				pos = buffer.indexOf(key);
				if (pos <= 0)
					continue;
				if (key.equals(value)) {
					pos = -1;
				} else {
					int end = pos + key.length();
					buffer.replace(pos, end, value);
				}
			} while (pos != -1);
		}
		return buffer;
	}

	private String getViewSource(String schema, String viewName) {
		ResultSet Reader = null;
		StringBuffer buffer = new StringBuffer();
		StringBuffer sb = new StringBuffer();
		String dstSchema = getSrctoDstSchema(schema);
		String columnList = "";
		String plSQL = "";
		String headView = "";

		if (dbSourceName.equalsIgnoreCase("oracle")) {
			headView = "CREATE OR REPLACE VIEW " + putQuote(dstSchema) + "."
					+ putQuote(viewName) + linesep;
			plSQL = "select text from dba_views where owner = '" + schema
					+ "' and view_name = '" + viewName + "'";
		} else if (dbSourceName.equalsIgnoreCase("idb2")) {
			headView = "CREATE VIEW " + putQuote(dstSchema) + "."
					+ putQuote(viewName) + linesep;
			plSQL = "select view_definition from sysibm.views where table_name = '"
					+ viewName + "' and table_schema = '" + schema + "'";
		} else if (dbSourceName.equalsIgnoreCase("db2")) {
			headView = "";
			plSQL = "select text from syscat.views where viewname = '"
					+ viewName + "' and viewschema = '" + schema + "'";
		}

		if (plSQL.equals("")) {
			return "";
		}
		try {
			if (debug)
				log("getViewSource: sql=" + plSQL);
			if (!dbSourceName.equalsIgnoreCase("db2"))
				columnList = "(" + getColumnList(schema, viewName) + ")"
						+ linesep + " AS " + linesep;
			PreparedStatement queryStatement = mainConn.prepareStatement(plSQL);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			int objCount = 0;
			while (Reader.next()) {
				try {
					sb.setLength(0);
					IBMExtractUtilities.getStringChunks(Reader, 1, sb);
					sb = fixIDB2ShortName(schema, sb);
					if (objCount == 0) {
						buffer.append(headView + columnList + sb.toString());
						objCount++;
					} else {
						buffer.append(sb.toString());
					}
				} catch (Exception ex) {
					log("Error getting PL/SQL " + ex.getMessage());
					ex.printStackTrace();
				}
			}
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return buffer.toString();
	}

	private void genViews(String schema) throws SQLException, IOException {
		String viewSQL = "";
		String viewTemplate = "";

		ResultSet Reader = null;
		ResultSet ViewDDLReader = null;
		StringBuffer buffer = new StringBuffer();
		String dstSchema = getSrctoDstSchema(schema);

		if (dbSourceName.equalsIgnoreCase("oracle")) {
			viewTemplate = "select dbms_metadata.get_ddl('VIEW','&viewName&','&schemaName&') from dual";
			viewSQL = "select view_name from dba_views where owner = '"
					+ schema + "' and view_name not like 'AQ$%'";
			if (debug)
				log("genViews SQL=" + viewSQL);
		} else if (dbSourceName.equalsIgnoreCase("idb2")) {
			viewTemplate = "select '' from sysibm.sysdummy1";
			viewSQL = "select table_name from sysibm.views where table_schema = '"
					+ schema + "' and table_name not like 'SYS%'";
			if (debug)
				log("genViews SQL=" + viewSQL);
		} else if (dbSourceName.equalsIgnoreCase("db2")) {
			viewTemplate = "select '' from sysibm.sysdummy1";
			viewSQL = "select viewname from syscat.views where viewschema = '"
					+ schema + "' and valid = 'Y'";
			if (debug) {
				log("genViews SQL=" + viewSQL);
			}
		}
		if (viewSQL.equals(""))
			return;

		PreparedStatement queryStatement = mainConn.prepareStatement(viewSQL);
		queryStatement.setFetchSize(fetchSize);
		Reader = queryStatement.executeQuery();
		int objCount = 0;
		while (Reader.next()) {
			buffer.setLength(0);
			buffer.append("SET CURRENT SCHEMA = '" + dstSchema + "'" + linesep);
			buffer.append(sqlTerminator + linesep);
			String viewName = Reader.getString(1);
			String ddlSQL = viewTemplate.replace("&schemaName&", schema);
			ddlSQL = ddlSQL.replace("&viewName&", viewName);
			if (debug) {
				log("viewTemplate=" + viewTemplate);
				log("genViews SQL=" + ddlSQL + " view=" + viewName + " schema="
						+ schema);
			}
			PreparedStatement viewStatement = mainConn.prepareStatement(ddlSQL);
			try {
				String viewDDL = null;
				try {
					ViewDDLReader = viewStatement.executeQuery();
					if (ViewDDLReader.next()) {
						viewDDL = ViewDDLReader.getString(1);
						if ((viewDDL == null) || (viewDDL.length() == 0))
							viewDDL = getViewSource(schema, viewName);
					}
				} catch (Exception ex) {
					viewDDL = getViewSource(schema, viewName);
				}
				if ((viewDDL != null) && (!viewDDL.equals(""))) {
					buffer.append("--#SET :VIEW:" + dstSchema + ":" + viewName
							+ linesep);
					if ((dbSourceName.equalsIgnoreCase("oracle"))
							&& (invalidOracleObject(schema, "VIEW", viewName)))
						buffer.append("--#WARNING : VIEW " + schema + "."
								+ viewName
								+ " is found to be invalid in source database"
								+ linesep);
					db2DropObjectsWriter.write("DROP VIEW "
							+ putQuote(dstSchema) + "." + putQuote(viewName)
							+ ";" + linesep);
					buffer.append(OraToDb2Converter.getDb2PlSql(viewDDL));

					buffer.append(linesep + sqlTerminator + linesep);
				}
				db2ViewsWriter.write(buffer.toString());
				db2ViewsWriter.write(getViewComments(schema, viewName));
				if ((objCount > 0) && (objCount % 20 == 0))
					log(objCount + " numbers of views extracted for schema "
							+ schema);
				objCount++;
			} catch (Exception e) {
				log("genViews SQL=" + ddlSQL);
				e.printStackTrace();
			}
			if (ViewDDLReader != null)
				ViewDDLReader.close();
			if (viewStatement != null)
				viewStatement.close();
		}
		if (objCount > 0)
			log(objCount + " Total numbers of views extracted for schema "
					+ schema);
		if (Reader != null)
			Reader.close();
		if (queryStatement != null)
			queryStatement.close();
	}

	private String composeType(String typeSchema, String typeName, int length,
			int scale, int codePage, String pSchemaName) {
		String typeStr = "";
		if ((!typeSchema.equalsIgnoreCase("SYSIBM"))
				&& (!typeSchema.equalsIgnoreCase(pSchemaName)))
			typeStr = typeStr + "\"" + typeSchema + "\".";
		if (typeSchema.equalsIgnoreCase("SYSIBM"))
			typeStr = typeStr + typeName;
		else
			typeStr = typeStr + "\"" + typeName + "\"";
		if ((typeSchema.equalsIgnoreCase("SYSIBM"))
				&& ((typeName.equalsIgnoreCase("CHARACTER"))
						|| (typeName.equalsIgnoreCase("VARCHAR"))
						|| (typeName.equalsIgnoreCase("BLOB"))
						|| (typeName.equalsIgnoreCase("CLOB")) || (typeName
						.equalsIgnoreCase("DECIMAL")))) {
			if (length == -1) {
				typeStr = typeStr + "()";
			} else if (typeName.equalsIgnoreCase("DECIMAL"))
				typeStr = typeStr + "(" + length + "," + scale + ")";
			else {
				typeStr = typeStr + "(" + length + ")";
			}
		}
		if (((typeName.equalsIgnoreCase("CHARACTER")) || (typeName
				.equalsIgnoreCase("VARCHAR")))
				&& (codePage == 0)) {
			typeStr = typeStr + " FOR BIT DATA";
		}
		return typeStr;
	}

	private void genDB2XSRSchema(String schema) {
		int num = 0;
		String xsrSQL = "";

		ResultSet Reader = null;
		ResultSet Reader2 = null;
		StringBuffer buffer = new StringBuffer();
		StringBuffer chunks = new StringBuffer();
		BufferedWriter writer = null;

		if (dbSourceName.equalsIgnoreCase("db2")) {
			if (majorSourceDBVersion > 9)
				return;
			xsrSQL = "SELECT OBJECTSCHEMA, OBJECTNAME, SCHEMALOCATION, OBJECTINFO FROM SYSCAT.XSROBJECTS WHERE OWNER = '"
					+ schema + "'";
		}

		if (xsrSQL.equals(""))
			return;

		try {
			new File(OUTPUT_DIR + "xsr").mkdirs();
			PreparedStatement queryStatement = mainConn
					.prepareStatement(xsrSQL);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			while (Reader.next()) {
				buffer.setLength(0);
				chunks.setLength(0);
				String objectSchema = trim(Reader.getString(1));
				String objectName = trim(Reader.getString(2));
				String schemaLocation = trim(Reader.getString(3));
				IBMExtractUtilities.getStringChunks(Reader, 4, chunks);
				String schemaLocation2 = schemaLocation == null ? ""
						: schemaLocation;
				String xmlFile = OUTPUT_DIR + "xsr" + filesep
						+ objectSchema.toLowerCase() + "_"
						+ objectName.toLowerCase() + ".xml";
				String xsdFile = OUTPUT_DIR + "xsr" + filesep
						+ objectSchema.toLowerCase() + "_"
						+ objectName.toLowerCase() + ".xsd";
				buffer.append("--#SET :XMLSCHEMA:" + objectSchema + ":"
						+ objectName + linesep);
				buffer.append("REGISTER XMLSCHEMA " + putQuote(schemaLocation2)
						+ linesep);
				buffer.append("FROM " + xmlFile + " AS "
						+ putQuote(objectSchema) + "." + putQuote(objectName)
						+ linesep + ";" + linesep);
				buffer.append("--#SET :XMLCOMPLETE:" + objectSchema + ":"
						+ objectName + linesep);
				buffer.append("COMPLETE XMLSCHEMA " + putQuote(objectSchema)
						+ "." + putQuote(objectName) + linesep);
				buffer.append("WITH " + xmlFile + linesep + ";" + linesep);
				db2ObjectsWriter[((Integer) plsqlHashTable.get("XMLSCHEMA"
						.toLowerCase())).intValue()].write(buffer.toString());
				db2DropObjectsWriter.write("DROP XMLSCHEMA "
						+ putQuote(objectSchema) + "." + putQuote(objectName)
						+ ";" + linesep);
				if ((num > 0) && (num % 20 == 0))
					log(num + " numbers of XML Schema extracted for schema "
							+ schema);
				num++;
				writer = new BufferedWriter(new FileWriter(xmlFile, false));
				writer.write(chunks.toString());
				writer.close();
				if (schemaLocation == null) {
					xsrSQL = "SELECT COMPONENT FROM SYSCAT.XSROBJECTCOMPONENTS WHERE OBJECTSCHEMA = '"
							+ objectSchema
							+ "' "
							+ "AND OBJECTNAME = '"
							+ objectName + "' " + "AND SCHEMALOCATION IS NULL";
				} else {
					xsrSQL = "SELECT COMPONENT FROM SYSCAT.XSROBJECTCOMPONENTS WHERE OBJECTSCHEMA = '"
							+ objectSchema
							+ "' "
							+ "AND OBJECTNAME = '"
							+ objectName
							+ "' "
							+ "AND SCHEMALOCATION = '"
							+ schemaLocation + "'";
				}

				PreparedStatement queryStatement2 = mainConn
						.prepareStatement(xsrSQL);
				queryStatement2.setFetchSize(fetchSize);
				Reader2 = queryStatement2.executeQuery();
				if (Reader2.next()) {
					chunks.setLength(0);
					writer = new BufferedWriter(new FileWriter(xsdFile, false));
					byte[] byteBuffer = new byte[1024000];
					int bytesRead = 0;
					try {
						InputStream input = Reader2.getBinaryStream(1);
						while ((bytesRead = input.read(byteBuffer)) != -1) {
							writer.write(new String(byteBuffer, 0, bytesRead));
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					writer.close();
				}
				if (Reader2 != null)
					Reader2.close();
				if (queryStatement2 != null)
					queryStatement2.close();
			}
			if (num > 0)
				log(num + " numbers of XML Schema extracted for schema "
						+ schema);
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (Exception e) {
			log("seqSQL=" + xsrSQL);
			e.printStackTrace();
		}
	}

	private void genDB2Aliases(String schema) {
		int num = 0;
		String aliasSQL = "";

		ResultSet Reader = null;
		StringBuffer buffer = new StringBuffer();

		if (dbSourceName.equalsIgnoreCase("db2")) {
			if (majorSourceDBVersion < 8)
				return;
			aliasSQL = "SELECT tabschema, tabname, base_tabschema, base_tabname FROM SYSCAT.TABLES WHERE type = 'A' AND tabschema IN ("
					+ schema + ") " + "ORDER BY create_time";
		}

		if (aliasSQL.equals(""))
			return;

		try {
			PreparedStatement queryStatement = mainConn
					.prepareStatement(aliasSQL);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			while (Reader.next()) {
				num++;
				buffer.setLength(0);
				String tabSchema = trim(Reader.getString(1));
				String tabName = trim(Reader.getString(2));
				String baseTabSchema = trim(Reader.getString(3));
				String baseTabName = trim(Reader.getString(4));

				String dstSchema = getSrctoDstSchema(tabSchema);
				if (dstSchema == null)
					dstSchema = tabSchema;
				buffer.append("--#SET :ALIAS:" + dstSchema + ":" + tabName
						+ linesep);
				buffer.append("CREATE ALIAS " + putQuote(dstSchema) + "."
						+ putQuote(tabName) + linesep);
				if (baseTabSchema.equalsIgnoreCase(tabSchema))
					buffer.append("FOR " + putQuote(baseTabName) + linesep);
				else
					buffer.append("FOR " + putQuote(baseTabSchema) + "."
							+ putQuote(baseTabName) + linesep);
				buffer.append(";" + linesep);
				db2SynonymWriter.write(buffer.toString());
				db2DropSynWriter.write("DROP ALIAS " + putQuote(dstSchema)
						+ "." + putQuote(tabName) + ";" + linesep);
			}
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (Exception e) {
			log("seqSQL=" + aliasSQL);
			e.printStackTrace();
		}
	}

	private void genDB2Sequences(String schema) {
		int seq_num = 0;
		String seqSQL = "";

		ResultSet Reader = null;
		StringBuffer buffer = new StringBuffer();

		if (dbSourceName.equalsIgnoreCase("db2")) {
			if (majorSourceDBVersion < 8)
				return;
			seqSQL = "SELECT seqschema, seqname, typeschema, typename, increment, maxvalue, minvalue, cycle, cache, order, datatypeid, precision FROM SYSCAT.SEQUENCES AS S JOIN SYSCAT.DATATYPES AS D ON S.datatypeid = D.typeid WHERE origin = 'U' AND seqtype = 'S' AND seqschema IN ("
					+ schema + ")";
		}

		if (seqSQL.equals(""))
			return;

		try {
			PreparedStatement queryStatement = mainConn
					.prepareStatement(seqSQL);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			while (Reader.next()) {
				seq_num++;
				buffer.setLength(0);
				String seqSchema = trim(Reader.getString(1));
				String seqName = Reader.getString(2);
				String typeSchema = trim(Reader.getString(3));
				String typeName = Reader.getString(4);
				String increment = Reader.getString(5);
				String maxvalue = Reader.getString(6);
				String minvalue = Reader.getString(7);
				String cycle = Reader.getString(8);
				int cache = Reader.getInt(9);
				String order = Reader.getString(10);
				int precision = Reader.getInt(11);

				String dstSchema = getSrctoDstSchema(seqSchema);
				if (dstSchema == null)
					dstSchema = seqSchema;
				String lastSeqStr = executeSQL("SELECT NEXT VALUE FOR "
						+ seqSchema + "." + seqName + " FROM SYSIBM.SYSDUMMY1",
						false);
				buffer.append("--#SET :SEQUENCE:" + dstSchema + ":" + seqName
						+ linesep);
				buffer.append("CREATE SEQUENCE "
						+ putQuote(dstSchema)
						+ "."
						+ putQuote(seqName)
						+ " AS "
						+ composeType(typeSchema, typeName, precision, 0, -1,
								seqSchema) + linesep);

				buffer.append("START WITH " + lastSeqStr + linesep);
				buffer.append("MAXVALUE " + maxvalue + linesep);
				buffer.append("MINVALUE " + minvalue + linesep);
				buffer.append("INCREMENT BY " + increment + linesep);
				if (cycle.equalsIgnoreCase("Y"))
					buffer.append("CYCLE " + linesep);
				else
					buffer.append("NO CYCLE " + linesep);
				if (order.equalsIgnoreCase("Y"))
					buffer.append("ORDER " + linesep);
				else
					buffer.append("NO ORDER " + linesep);
				if (cache < 2)
					buffer.append("NO CACHE " + linesep);
				else
					buffer.append("CACHE " + cache + linesep);
				buffer.append(";" + linesep);
				db2SeqWriter.write(buffer.toString());
				db2DropSeqWriter.write("DROP SEQUENCE \"" + dstSchema + "\".\""
						+ seqName + "\";" + linesep);
			}
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (Exception e) {
			log("seqSQL=" + seqSQL);
			e.printStackTrace();
		}
	}

	private void genSequences(String schema) throws SQLException, IOException {
		int seq_num = 0;

		String seqSQL = "";
		String cycle_flag = "";
		String order_flag = "";

		ResultSet Reader = null;
		StringBuffer buffer = new StringBuffer();

		if (dbSourceName.equalsIgnoreCase("oracle")) {
			seqSQL = "select last_number, min_value, max_value, increment_by, cache_size, cycle_flag, order_flag, sequence_owner, sequence_name from dba_sequences where sequence_owner IN ("
					+ schema + ")";
		}

		if (seqSQL.equals(""))
			return;

		PreparedStatement queryStatement = mainConn.prepareStatement(seqSQL);
		queryStatement.setFetchSize(fetchSize);
		Reader = queryStatement.executeQuery();
		while (Reader.next()) {
			seq_num++;
			buffer.setLength(0);
			long last_number = Reader.getLong(1);
			long min_value = Reader.getLong(2);
			long max_value;
			try {
				max_value = Reader.getLong(3);
			} catch (Exception es) {
				max_value = 2147483647L;
			}
			long increment_by = Reader.getLong(4);
			long cache_size = Reader.getLong(5);
			cycle_flag = Reader.getString(6);
			order_flag = Reader.getString(7);
			String seqOwner = Reader.getString(8);
			String seqName = Reader.getString(9);
			String dstSchema = getSrctoDstSchema(seqOwner);
			if (dstSchema == null)
				dstSchema = seqOwner;
			if (cache_size == 0L)
				cache_size = 5L;
			buffer.append("-- Oracle Sequence Name : " + seqName + linesep);
			buffer.append("--#SET :SEQUENCE:" + dstSchema + ":" + seqName
					+ linesep);
			if ((releaseLevel != -1.0F) && (releaseLevel >= 9.7F))
				buffer.append("CREATE SEQUENCE \"" + dstSchema + "\".\""
						+ seqName + "\" AS NUMBER(27)" + linesep);
			else
				buffer.append("CREATE SEQUENCE \"" + dstSchema + "\".\""
						+ seqName + "\"" + linesep);
			buffer.append("MINVALUE " + last_number + linesep);
			if ((releaseLevel != -1.0F) && (releaseLevel >= 9.7F)) {
				buffer.append("MAXVALUE " + Reader.getString(3) + linesep);
			} else {
				if (max_value > 2147483647L) {
					buffer.append("-- Oracle MAX VALUE IS " + max_value
							+ ". Using 2147483647 as max value for DB2"
							+ seqName);
					max_value = 2147483647L;
				}
				buffer.append("MAXVALUE " + max_value + linesep);
			}
			buffer.append("INCREMENT BY " + increment_by + linesep);
			buffer.append("CACHE " + cache_size + linesep);
			if (cycle_flag.equalsIgnoreCase("N"))
				buffer.append("NOCYCLE" + linesep);
			else
				buffer.append("CYCLE" + linesep);
			if (order_flag.equalsIgnoreCase("N"))
				buffer.append("NOORDER" + linesep);
			else
				buffer.append("ORDER" + linesep);
			buffer.append(";" + linesep);
			db2SeqWriter.write(buffer.toString());
			db2DropSeqWriter.write("DROP SEQUENCE \"" + dstSchema + "\".\""
					+ seqName + "\";" + linesep);
		}
		if (Reader != null)
			Reader.close();
		if (queryStatement != null)
			queryStatement.close();
	}

	private void genDB2CheckScript() throws IOException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);
		if (osType.equals("WIN")) {
			buffer.append("::  Copyright(r) IBM Corporation" + linesep);
			buffer.append("::" + linesep);
			buffer.append(":: Vikram Khatri (vikram.khatri@us.ibm.com)"
					+ linesep);
			buffer.append("::" + linesep);
			buffer
					.append(":: The purpose of this script is to generate SET INTEGRITY commands in right order."
							+ linesep);
			buffer
					.append(":: This script will be run automatically from db2gen script."
							+ linesep);
			buffer.append("::" + linesep);
		} else {
			buffer.append("#!/bin/ksh" + linesep);
			buffer.append("#  Copyright(r) IBM Corporation" + linesep);
			buffer.append("#" + linesep);
			buffer.append("# Vikram Khatri (vikram.khatri@us.ibm.com)"
					+ linesep);
			buffer.append("#" + linesep);
			buffer
					.append("# The purpose of this script is to generate SET INTEGRITY commands in right order."
							+ linesep);
			buffer
					.append("# This script will be run automatically from db2gen script."
							+ linesep);
			buffer.append("#" + linesep);
			buffer.append("echo " + linesep);
		}
		if (remoteLoad) {
			buffer
					.append("db2 connect to " + db2dbname + " USER " + dstUid
							+ " USING " + IBMExtractUtilities.Decrypt(dstPwd)
							+ linesep);
		} else {
			buffer.append("db2 CONNECT TO " + db2dbname + linesep);
		}
		buffer
				.append("db2 -tx +w \"WITH GEN(tabname, seq) AS (SELECT RTRIM(TABSCHEMA)||'.' ||RTRIM(TABNAME) AS TABNAME, ");
		buffer
				.append("ROW_NUMBER() OVER (PARTITION BY STATUS) as seq FROM SYSCAT.TABLES WHERE STATUS='C'), r(a, seq1) AS ");
		buffer
				.append("(SELECT CAST(TABNAME as VARCHAR(32000)), SEQ FROM gen WHERE seq=1 UNION ALL ");
		buffer
				.append("SELECT CAST(r.a ||','||RTRIM(gen.tabname) AS VARCHAR(32000)), gen.seq FROM gen, r WHERE (r.seq1+1)=gen.seq), r1 AS ");
		buffer
				.append("(SELECT a, seq1 FROM r) SELECT 'SET INTEGRITY FOR ' || a || ' IMMEDIATE CHECKED;' ");
		buffer
				.append("FROM r1 WHERE seq1=(SELECT MAX(seq1) FROM r1)\" > tmp.sql"
						+ linesep);
		buffer.append("db2 -tvf tmp.sql" + linesep);
		db2CheckScriptWriter.write(buffer.toString());
	}

	private void genDB2DropScript() throws IOException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);

		String db2 = dbTargetName.equals("zdb2") ? "java com.ibm.db2.clp.db2"
				: "db2";
		if (osType.equals("WIN")) {
			buffer.append("::  Copyright(r) IBM Corporation" + linesep);
			buffer.append("::" + linesep);
			buffer.append(":: Vikram Khatri (vikram.khatri@us.ibm.com)"
					+ linesep);
			buffer.append("::" + linesep);
			buffer
					.append(":: This script can be run to drop objects in the right order."
							+ linesep);
			buffer
					.append(":: This script can be run either from GUI or from command line."
							+ linesep);
			buffer.append("::" + linesep);

			buffer.append("SET OUTPUT=%~n0_OUTPUT.TXT" + linesep);

			buffer.append("@echo off" + linesep);
			buffer.append("cls" + linesep);

			buffer
					.append("ECHO Executed by: %USERNAME% Machine: %COMPUTERNAME% On %OS% %DATE% %TIME%"
							+ linesep);
			buffer
					.append("ECHO Executed by: %USERNAME% Machine: %COMPUTERNAME% On %OS% %DATE% %TIME% > %OUTPUT%"
							+ linesep);
			buffer.append("ECHO." + linesep);
			buffer
					.append("ECHO -------------------------------------------------------------------"
							+ linesep);
			buffer.append("ECHO Drop Objects in DB2" + linesep);
			buffer
					.append("ECHO -------------------------------------------------------------------"
							+ linesep);
			buffer.append("ECHO." + linesep);

			buffer.append("echo Executing Script db2dropobjects.cmd > %OUTPUT%"
					+ linesep);
			if (!db2Instance.equals(""))
				buffer.append("SET DB2INSTANCE=" + db2Instance + linesep);
			if (remoteLoad) {
				buffer.append("echo Connecting to " + db2dbname + linesep);
				buffer.append("db2 connect to " + db2dbname + " USER " + dstUid
						+ " USING " + IBMExtractUtilities.Decrypt(dstPwd)
						+ " >> %OUTPUT%" + linesep);
			}
			buffer
					.append("echo Running db2dropfkeys.sql script to drop foreign keys"
							+ linesep);
			buffer.append("db2 -tvf db2dropfkeys.sql >> %OUTPUT%" + linesep);
			if (dbSourceName.equalsIgnoreCase("oracle")) {
				buffer
						.append("echo Running db2dropobjects.sql script to drop PL/SQL Objects"
								+ linesep);
				buffer.append("db2 -tvf db2dropobjects.sql >> %OUTPUT%"
						+ linesep);
			}
			buffer
					.append("echo Running db2droptables.sql script to drop tables"
							+ linesep);
			buffer.append("db2 -tvf db2droptables.sql >> %OUTPUT%" + linesep);
			buffer
					.append("echo Running db2dropsequences.sql script to drop all sequences"
							+ linesep);
			buffer
					.append("db2 -tvf db2dropsequences.sql >> %OUTPUT%"
							+ linesep);
			if (dbSourceName.equalsIgnoreCase("oracle")) {
				buffer
						.append("echo Running db2dropsynonyms.sql to drop synonyms"
								+ linesep);
				buffer.append("db2 -tvf db2dropsynonyms.sql >> %OUTPUT%"
						+ linesep);
			}
			buffer
					.append("echo Running db2droptsbp.sql script to drop table space and buffer pools"
							+ linesep);
			buffer.append("db2 -tvf db2droptsbp.sql >> %OUTPUT%" + linesep);
			if (remoteLoad) {
				buffer.append("db2 connect reset >> %OUTPUT%" + linesep);
			}
			buffer.append("echo. " + linesep);
			buffer
					.append("echo Check the log file %OUTPUT% for any errors or issues"
							+ linesep);
			buffer.append("echo. " + linesep);
			buffer.append(":end " + linesep);
		} else {
			if (dbTargetName.equals("zdb2"))
				buffer.append("#!/bin/sh" + linesep);
			else
				buffer.append("#!/bin/ksh" + linesep);
			buffer.append("#  Copyright(r) IBM Corporation" + linesep);
			buffer.append("#" + linesep);
			buffer.append("# Vikram Khatri (vikram.khatri@us.ibm.com)"
					+ linesep);
			buffer.append("#" + linesep);
			buffer
					.append("# This script can be run to drop objects in the right order."
							+ linesep);
			buffer
					.append("# This script can be run either from GUI or from command line."
							+ linesep);
			buffer.append("#" + linesep);
			buffer.append("echo " + linesep);
			buffer.append("OUTPUT=${0%.*}.log" + linesep);
			buffer.append("echo Executing Script db2dropobjects.sh > $OUTPUT"
					+ linesep);
			if (!db2Instance.equals(""))
				buffer.append("DB2INSTANCE=" + db2Instance + linesep);
			if ((dbTargetName.equals("db2luw")) && (remoteLoad)) {
				buffer.append("echo Connecting to " + db2dbname + linesep);
				buffer.append("db2 connect to " + db2dbname + " USER " + dstUid
						+ " USING " + dstPwd + " >> $OUTFILE " + linesep);
			}
			buffer
					.append("echo $(date) Running db2dropfkeys.sql script to drop foreign keys"
							+ linesep);
			buffer.append(db2 + " -tvf db2dropfkeys.sql >> $OUTPUT" + linesep);
			if (dbSourceName.equalsIgnoreCase("oracle")) {
				buffer
						.append("echo $(date) Running db2dropobjects.sql script to drop pl/sql objects"
								+ linesep);
				buffer.append(db2 + " -tvf db2dropobjects.sql >> $OUTPUT"
						+ linesep);
			}
			buffer
					.append("echo $(date) Running db2droptables.sql script to drop tables"
							+ linesep);
			buffer.append(db2 + " -tvf db2droptables.sql >> $OUTPUT" + linesep);
			buffer
					.append("echo $(date) Running db2dropsequences.sql script to drop all sequences"
							+ linesep);
			buffer.append(db2 + " -tvf db2dropsequences.sql >> $OUTPUT"
					+ linesep);
			if (dbSourceName.equals("oracle")) {
				buffer
						.append("echo $(date) Running db2dropsynonyms.sql to drop synonyms"
								+ linesep);
				buffer.append(db2 + " -tvf db2dropsynonyms.sql >> $OUTPUT"
						+ linesep);
			}
			if (dbTargetName.equals("db2luw")) {
				buffer
						.append("echo $(date) Running db2tsbp.sql script to create buffer pools and table spaces"
								+ linesep);
				buffer.append(db2 + " -tvf db2tsbp.sql >> $OUTPUT" + linesep);
			}
			if ((dbTargetName.equals("db2luw")) && (remoteLoad)) {
				buffer.append("db2 connect reset >> $OUTPUT" + linesep);
			}
			buffer.append("echo " + linesep);
			buffer
					.append("echo $(date) Check the log file $OUTPUT for any errors or issues"
							+ linesep);
			buffer.append("echo " + linesep);
		}
		db2DropScriptWriter.write(buffer.toString());
	}

	private void genDB2Script() throws IOException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);

		String db2 = dbTargetName.equals("zdb2") ? "java com.ibm.db2.clp.db2"
				: "db2";
		if (osType.equals("WIN")) {
			buffer.append("::  Copyright(r) IBM Corporation" + linesep);
			buffer.append("::" + linesep);
			buffer.append(":: Vikram Khatri (vikram.khatri@us.ibm.com)"
					+ linesep);
			buffer.append("::" + linesep);
			buffer.append(":: This script is the deployment script." + linesep);
			buffer
					.append(":: The name of the script can be either db2gen.cmd or db2ddl.cmd or db2load.cmd"
							+ linesep);
			buffer
					.append(":: This script can be run either from GUI or from command line."
							+ linesep);
			buffer.append("::" + linesep);

			buffer.append("SET OUTPUT=%~n0_OUTPUT.TXT" + linesep);

			buffer.append("@echo off" + linesep);
			buffer.append("cls" + linesep);

			buffer
					.append("ECHO Executed by: %USERNAME% Machine: %COMPUTERNAME% On %OS% %DATE% %TIME%"
							+ linesep);
			buffer
					.append("ECHO Executed by: %USERNAME% Machine: %COMPUTERNAME% On %OS% %DATE% %TIME% > %OUTPUT%"
							+ linesep);
			buffer.append("ECHO." + linesep);
			buffer
					.append("ECHO -------------------------------------------------------------------"
							+ linesep);
			buffer.append("ECHO Create Tables and Load Data in DB2" + linesep);
			buffer
					.append("ECHO -------------------------------------------------------------------"
							+ linesep);
			buffer.append("ECHO." + linesep);

			if ((ddlGen) && (dataUnload))
				buffer.append("echo Executing Script db2gen.cmd > %OUTPUT%"
						+ linesep);
			else if ((ddlGen) && (!dataUnload))
				buffer.append("echo Executing Script db2ddl.cmd > %OUTPUT%"
						+ linesep);
			else if ((!ddlGen) && (dataUnload))
				buffer.append("echo Executing Script db2load.cmd > %OUTPUT%"
						+ linesep);
			if (!db2Instance.equals(""))
				buffer.append("SET DB2INSTANCE=" + db2Instance + linesep);
			if (remoteLoad) {
				buffer.append("echo Connecting to " + db2dbname + linesep);
				buffer.append("db2 connect to " + db2dbname + " USER " + dstUid
						+ " USING " + IBMExtractUtilities.Decrypt(dstPwd)
						+ " >> %OUTPUT%" + linesep);
			}
			if (ddlGen) {
				buffer
						.append("echo Running db2tsbp.sql script to create buffer pools and tablespaces"
								+ linesep);
				buffer.append("db2 -tvf db2tsbp.sql >> %OUTPUT%" + linesep);
				buffer.append("echo Running db2udf.sql script to create UDFs"
						+ linesep);
				buffer.append("db2 -tvf db2udf.sql >> %OUTPUT%" + linesep);
				buffer
						.append("echo Running db2tables.sql script to create all tables"
								+ linesep);
				buffer.append("db2 -tvf db2tables.sql >> %OUTPUT%" + linesep);
				if (dbSourceName.equalsIgnoreCase("idb2")) {
					buffer
							.append("echo Running db2views.db2 script to create views"
									+ linesep);
					buffer
							.append("db2 -tvf db2views.db2 >> %OUTPUT%"
									+ linesep);
				}
				buffer.append("echo Running default script db2default.sql"
						+ linesep);
				buffer.append("db2 -tvf db2default.sql >> %OUTPUT%" + linesep);
				buffer
						.append("echo Running db2check.sql script to create check constraints"
								+ linesep);
				buffer.append("db2 -tvf db2check.sql >> %OUTPUT%" + linesep);
				buffer
						.append("echo Running db2cons.sql script to create primary keys and indexes"
								+ linesep);
				buffer.append("db2 -tvf db2cons.sql >> %OUTPUT%" + linesep);
				buffer
						.append("echo Running db2sequences.sql script to create sequences"
								+ linesep);
				buffer
						.append("db2 -tvf db2sequences.sql >> %OUTPUT%"
								+ linesep);
				if (dbSourceName.equals("oracle")) {
					buffer
							.append("echo Running db2synonyms.db2 script to create synonyms"
									+ linesep);
					buffer.append("db2 -tvf db2synonyms.db2 >> %OUTPUT%"
							+ linesep);
				}
			}
			if (dataUnload) {
				buffer
						.append("echo Running db2load.sql script to create to load the data"
								+ linesep);
				buffer.append("db2 -tvf db2load.sql >> %OUTPUT%" + linesep);
				buffer
						.append("echo Running db2checkRemoval.cmd script to do SET INTEGRITY"
								+ linesep);
				buffer.append("call db2checkRemoval.cmd >> %OUTPUT%" + linesep);
				buffer
						.append("echo Running db2tabcount.sql script to count rows from all tables"
								+ linesep);
				buffer.append("db2 -tvf db2tabcount.sql >> %OUTPUT%" + linesep);
				buffer
						.append("echo Running db2tabstatus.sql script to show status of tables after load"
								+ linesep);
				buffer
						.append("db2 -tvf db2tabstatus.sql >> %OUTPUT%"
								+ linesep);
			}
			if (ddlGen) {
				buffer
						.append("echo Running db2uniq.sql script to create all unique constraints"
								+ linesep);
				buffer.append("db2 -tvf db2uniq.sql >> %OUTPUT%" + linesep);
				buffer
						.append("echo Running db2fkeys.sql script to create all foreign keys"
								+ linesep);
				buffer.append("db2 -tvf db2fkeys.sql >> %OUTPUT%" + linesep);
			}
			if (remoteLoad) {
				buffer.append("db2 connect reset >> %OUTPUT%" + linesep);
			}
			buffer.append("echo. " + linesep);
			buffer
					.append("echo Check the log file %OUTPUT% for any errors or issues"
							+ linesep);
			buffer.append("echo. " + linesep);
			buffer.append(":end " + linesep);
		} else {
			if (dbTargetName.equals("zdb2"))
				buffer.append("#!/bin/sh" + linesep);
			else
				buffer.append("#!/bin/ksh" + linesep);
			buffer.append("#  Copyright(r) IBM Corporation" + linesep);
			buffer.append("#" + linesep);
			buffer.append("# Vikram Khatri (vikram.khatri@us.ibm.com)"
					+ linesep);
			buffer.append("#" + linesep);
			buffer.append("# This script is the deployment script." + linesep);
			buffer
					.append("# The name of the script can be either db2gen.sh or db2ddl.sh or db2load.sh"
							+ linesep);
			buffer
					.append("# This script can be run either from GUI or from command line."
							+ linesep);
			buffer.append("#" + linesep);
			buffer.append("echo " + linesep);
			buffer.append("OUTPUT=${0%.*}.log" + linesep);
			if ((ddlGen) && (dataUnload))
				buffer.append("echo Executing Script db2gen.sh > $OUTPUT"
						+ linesep);
			else if ((ddlGen) && (!dataUnload))
				buffer.append("echo Executing Script db2ddl.sh > $OUTPUT"
						+ linesep);
			else if ((!ddlGen) && (dataUnload))
				buffer.append("echo Executing Script db2load.sh > $OUTPUT"
						+ linesep);
			if (!db2Instance.equals(""))
				buffer.append("DB2INSTANCE=" + db2Instance + linesep);
			if ((dbTargetName.equals("db2luw")) && (remoteLoad)) {
				buffer.append("echo Connecting to " + db2dbname + linesep);
				buffer.append("db2 connect to " + db2dbname + " USER " + dstUid
						+ " USING " + dstPwd + " >> $OUTFILE " + linesep);
			}
			if (ddlGen) {
				if (dbTargetName.equals("db2luw")) {
					buffer
							.append("echo $(date) Running db2tsbp.sql script to create buffer pools and table spaces"
									+ linesep);
					buffer.append(db2 + " -tvf db2tsbp.sql >> $OUTPUT"
							+ linesep);
				}
				buffer
						.append("echo $(date) Running db2udf.sql script to create UDFs"
								+ linesep);
				buffer.append(db2 + " -tvf db2udf.sql >> $OUTPUT" + linesep);
				buffer
						.append("echo $(date) Running db2tables.sql script to create all tables"
								+ linesep);
				buffer.append(db2 + " -tvf db2tables.sql >> $OUTPUT" + linesep);
				if (dbSourceName.equalsIgnoreCase("idb2")) {
					buffer
							.append("echo $(date) Running db2views.db2 script to create views"
									+ linesep);
					buffer.append(db2 + " -tvf db2views.db2 >> $OUTPUT"
							+ linesep);
				}
				buffer
						.append("echo $(date) Running default script db2default.sql"
								+ linesep);
				buffer
						.append(db2 + " -tvf db2default.sql >> $OUTPUT"
								+ linesep);
				buffer
						.append("echo $(date) Running db2sequences.sql script to create sequences"
								+ linesep);
				buffer.append(db2 + " -tvf db2sequences.sql >> $OUTPUT"
						+ linesep);
				if (dbSourceName.equals("oracle")) {
					buffer
							.append("echo $(date) Running db2synonyms.sql script to create sequences"
									+ linesep);
					buffer.append(db2 + " -tvf db2synonyms.sql >> $OUTPUT"
							+ linesep);
				}
			}
			if (dataUnload) {
				if (dbTargetName.equals("zdb2")) {
					buffer
							.append("echo $(date) Cleaning up DISC, LERR and CERR datasets"
									+ linesep);
					if ((appJAR == null) || (appJAR.length() == 0)) {
						appJAR = new File("..").getCanonicalPath()
								+ "/IBMDataMovementTool.jar";
					}
					buffer
							.append("java -cp "
									+ appJAR
									+ ":$JZOS_HOME/ibmjzos.jar "
									+ "-Djava.ext.dirs=${JZOS_HOME}:${JAVA_HOME}/lib/ext ibm.Cleanup"
									+ linesep);
				}

				buffer
						.append("echo $(date) Running db2load.sql script to create to load the data"
								+ linesep);
				buffer.append(db2 + " -tvf db2load.sql | tee -a $OUTPUT"
						+ linesep);
				if (dbTargetName.equals("db2luw")) {
					buffer
							.append("echo $(date) Running db2checkRemoval.sh script to do SET INTEGRITY"
									+ linesep);
					buffer
							.append(". ./db2checkRemoval.sh >> $OUTPUT"
									+ linesep);
				}
			}
			if (ddlGen) {
				buffer
						.append("echo $(date) Running db2check.sql script to create check constraints"
								+ linesep);
				buffer.append(db2 + " -tvf db2check.sql >> $OUTPUT" + linesep);
				buffer
						.append("echo $(date) Running db2cons.sql script to create primary keys and indexes"
								+ linesep);
				buffer.append(db2 + " -tvf db2cons.sql >> $OUTPUT" + linesep);
				buffer
						.append("echo $(date) Running db2uniq.sql script to create all unique constraints"
								+ linesep);
				buffer.append(db2 + " -tvf db2uniq.sql >> $OUTPUT" + linesep);
				buffer
						.append("echo $(date) Running db2fkeys.sql script to create all foreign keys"
								+ linesep);
				buffer.append(db2 + " -tvf db2fkeys.sql >> $OUTPUT" + linesep);
			}
			if (dataUnload) {
				if (dbTargetName.equals("zdb2")) {
					buffer.append("echo \"CONNECT TO " + db2dbname
							+ ";\" > tmp.sql" + linesep);
					buffer.append(db2 + " -tf db2runstats.sql >> tmp.sql"
							+ linesep);
					buffer
							.append("grep -E \"CALL|CONNECT TO\" tmp.sql > runstats.sql "
									+ linesep);
					buffer.append("echo \"TERMINATE;\" >> runstats.sql"
							+ linesep);
					buffer.append("echo $(date) Running runstats.sql script"
							+ linesep);
					buffer.append(db2 + " -tf runstats.sql >> $OUTPUT"
							+ linesep);
					buffer.append("echo \"CONNECT TO " + db2dbname
							+ ";\" > tmp.sql" + linesep);
					buffer.append(db2 + " -tf db2checkpending.sql >> tmp.sql"
							+ linesep);
					buffer
							.append("grep -E \"CALL|CONNECT TO\" tmp.sql > checkdata.sql "
									+ linesep);
					buffer.append("echo \"TERMINATE;\" >> checkdata.sql"
							+ linesep);
					buffer.append("rm -f tmp.sql" + linesep);
					buffer.append("echo $(date) Running checkdata.sql script"
							+ linesep);
					buffer.append(db2 + " -tf checkdata.sql >> $OUTPUT"
							+ linesep);
				}
				buffer
						.append("echo $(date) Running db2tabcount.sql script to count rows from all tables"
								+ linesep);
				buffer.append(db2 + " -tvf db2tabcount.sql >> $OUTPUT"
						+ linesep);
				buffer
						.append("echo $(date) Running db2tabstatus.sql script to show status of tables after load"
								+ linesep);
				buffer.append(db2 + " -tf db2tabstatus.sql >> $OUTPUT"
						+ linesep);
			}
			if ((dbTargetName.equals("db2luw")) && (remoteLoad)) {
				buffer.append("db2 connect reset >> $OUTPUT" + linesep);
			}
			buffer.append("echo " + linesep);
			buffer
					.append("echo $(date) Check the log file $OUTPUT for any errors or issues"
							+ linesep);
			buffer.append("echo " + linesep);
		}
		db2ScriptWriter.write(buffer.toString());
	}

	public void run() {
		for (int i = 0; i < totalTables; i++) {
			int bladeIndex = i % threads;
			blades[bladeIndex].add(new Integer(i));
		}

		log("Starting Blades");
		for (int i = 0; i < threads; i++) {
			blades[i].start();
		}

		long start = System.currentTimeMillis();
		boolean done = false;
		while (!done) {
			done = true;
			for (int i = 0; i < threads; i++) {
				List queue = blades[i].getQueue();
				int size = -1;
				synchronized (queue) {
					size = queue.size();
				}
				if (size <= 0)
					continue;
				done = false;
				try {
					synchronized (empty) {
						empty.wait();
					}

				} catch (InterruptedException ex) {
				}

			}

		}

		long end = System.currentTimeMillis();
		double total = (end - start) / 1000L;
		log("==== Total time: " + total + " sec ");

		for (int i = 0; i < threads; i++) {
			try {
				Thread.sleep(250L);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			blades[i].shutdown();
		}

		try {
			for (int i = 0; i < totalTables; i++) {
				if (!dataUnload)
					continue;
				if (dbTargetName.equals("db2luw")) {
					fp[i].close();
				}
			}
			if (dataUnload) {
				if ((dbTargetName.equals("zdb2"))
						|| ((dbTargetName.equals("db2luw")) && (!remoteLoad))) {
					if (putConnectStatement) {
						db2LoadWriter.write("TERMINATE;" + linesep);
						db2RunstatWriter.write("TERMINATE;" + linesep);
						db2CheckPendingWriter.write("TERMINATE;" + linesep);
						db2TabStatusWriter.write("TERMINATE;" + linesep);
						db2TabCountWriter.write("TERMINATE;" + linesep);
						db2LoadTerminateWriter.write("TERMINATE;" + linesep);
					}
				}
				db2LoadWriter.close();
				db2RunstatWriter.close();
				db2CheckPendingWriter.close();
				db2TabStatusWriter.close();
				db2TabCountWriter.close();
				db2LoadTerminateWriter.close();
			}
			log("Starting extract of other metadata. Please wait ....");
			if (ddlGen) {
				Object set = new HashSet(Arrays.asList(srcSchName));
				String[] copy = (String[]) (String[]) ((Set) set)
						.toArray(new String[((Set) set).size()]);
				if ((dbSourceName.equalsIgnoreCase("oracle"))
						&& (db2_compatibility)) {
					try {
						genRoles();
						openPLSQLFiles();
						for (int idx = 0; idx < copy.length; idx++) {
							if (debug) {
								log("Starting extract for schema "
										+ removeQuote(copy[idx]));
							}
							genSynonyms(removeQuote(copy[idx]));
							genPrivs(removeQuote(copy[idx]));
							genViews(removeQuote(copy[idx]));
							genMaterializedViews(removeQuote(copy[idx]));
							genPLSQL(removeQuote(copy[idx]));
							if (majorSourceDBVersion <= 8)
								genTriggers(removeQuote(copy[idx]));
						}
						closePLSQLFiles();
						if (regenerateTriggers)
							regenerateTriggers();
					} catch (Exception e) {
						e.printStackTrace();
					}
					genAllSequences();
				} else if (dbSourceName.equalsIgnoreCase("idb2")) {
					try {
						openPLSQLFiles();
						for (int idx = 0; idx < copy.length; idx++) {
							if (debug) {
								log("Starting extract for schema "
										+ removeQuote(copy[idx]));
							}
							genSQLFunctions(removeQuote(copy[idx]));
							genViews(removeQuote(copy[idx]));
							genSQLPL(removeQuote(copy[idx]));
							genTriggers(removeQuote(copy[idx]));
						}
						closePLSQLFiles();
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else if (dbSourceName.equalsIgnoreCase("db2")) {
					genAllSequences();
					try {
						openPLSQLFiles();
						genInstallJavaJars();
						for (int idx = 0; idx < copy.length; idx++) {
							if (debug) {
								log("Starting extract for schema "
										+ removeQuote(copy[idx]));
							}
							genMaterializedViews(removeQuote(copy[idx]));
							genDB2GVariables(removeQuote(copy[idx]));
							genDB2Grants(removeQuote(copy[idx]));
							getNonSQLProcedureSource(removeQuote(copy[idx]));
							genDB2Types(removeQuote(copy[idx]));
							genDB2XSRSchema(removeQuote(copy[idx]));
							genTriggers(removeQuote(copy[idx]));
							genViews(removeQuote(copy[idx]));
							genSQLPL(removeQuote(copy[idx]));
							genSQLModules(removeQuote(copy[idx]));
						}
						closePLSQLFiles();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				genDB2TSBP();
				if ((dbTargetName.equals("zdb2"))
						|| ((dbTargetName.equals("db2luw")) && (!remoteLoad))) {
					if (putConnectStatement) {
						db2TablesWriter.write("TERMINATE;" + linesep);
						db2FKWriter.write("TERMINATE;" + linesep);
						db2DropWriter.write("TERMINATE;" + linesep);
						db2ConsWriter.write("TERMINATE;" + linesep);
						db2FKDropWriter.write("TERMINATE;" + linesep);
						db2CheckWriter.write("TERMINATE;" + linesep);
						db2UniqWriter.write("TERMINATE;" + linesep);
						db2SeqWriter.write("TERMINATE;" + linesep);
						db2DropSeqWriter.write("TERMINATE;" + linesep);
						db2ViewsWriter.write("TERMINATE" + linesep);
						db2ViewsWriter.write(sqlTerminator + linesep);
						db2DropObjectsWriter.write("TERMINATE;" + linesep);
						if ((dbSourceName.equals("oracle"))
								|| (dbSourceName.equals("db2"))) {
							db2SynonymWriter.write("TERMINATE;" + linesep);
							db2DropSynWriter.write("TERMINATE;" + linesep);
							db2mviewWriter.write("TERMINATE" + linesep);
							db2mviewWriter.write(sqlTerminator + linesep);
						}
						if (dbSourceName.equals("oracle")) {
							db2rolePrivsWriter.write("TERMINATE;" + linesep);
							db2objPrivsWriter.write("TERMINATE;" + linesep);
						}
						db2udfWriter.write("TERMINATE;" + linesep);
						db2tsbpWriter.write("TERMINATE;" + linesep);
						db2droptsbpWriter.write("TERMINATE;" + linesep);
						db2DefaultWriter.write("TERMINATE;" + linesep);
					}
				}
				db2TablesWriter.close();
				db2FKWriter.close();
				db2DropWriter.close();
				db2ConsWriter.close();
				db2FKDropWriter.close();
				db2CheckWriter.close();
				db2UniqWriter.close();
				db2SeqWriter.close();
				db2DropSeqWriter.close();
				db2ViewsWriter.close();
				db2DropObjectsWriter.close();
				if ((dbSourceName.equals("oracle"))
						|| (dbSourceName.equals("db2"))) {
					db2SynonymWriter.close();
					db2DropSynWriter.close();
					db2mviewWriter.close();
				}
				if (dbSourceName.equals("oracle")) {
					db2rolePrivsWriter.close();
					db2objPrivsWriter.close();
				}
				db2udfWriter.close();
				db2tsbpWriter.close();
				db2droptsbpWriter.close();
				db2DefaultWriter.close();
				db2TruncNameWriter.close();
			}
			if ((dataUnload) || (ddlGen)) {
				genDB2CheckScript();
				db2CheckScriptWriter.close();
				genDB2Script();
				genDB2DropScript();
				db2ScriptWriter.close();
				db2DropScriptWriter.close();
			}
			try {
				if (mainConn != null)
					mainConn.commit();
				mainConn.close();
				for (int i = 0; i < threads; i++) {
					if (blades[i].bladeConn != null)
						blades[i].bladeConn.commit();
					blades[i].bladeConn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		if (dbSourceName.equalsIgnoreCase("idb2")) {
			fixiDB2Code("db2function.db2");
			fixiDB2Code("db2views.db2");
			fixiDB2Code("db2procedure.db2");
			fixiDB2Code("db2trigger.db2");
		}
		log("Work completed");
	}

	private static void log(String msg) {
		if (osType.equals("z/OS")) {
			System.out.println(timestampFormat.format(new java.util.Date())
					+ ":" + msg);
		} else
			System.out
					.println("[" + timestampFormat.format(new java.util.Date())
							+ "] " + msg);
	}

	public static void main(String[] args) {
		if (args.length < 11) {
			System.out
					.println("usage: java -Xmx600m -DOUTPUT_DIR=. ibm.GenerateExtract table_prop_file colsep dbSourceName threads server dbname portnum uid pwd ddlgen(true/false) dataunload(true/false) fetchSize [loadreplace(true/false)]");

			System.exit(-1);
		}
		IBMExtractUtilities.replaceStandardOutput("IBMDataMovementTool.log");
		IBMExtractUtilities
				.replaceStandardError("IBMDataMovementToolError.log");
		TABLES_PROP_FILE = args[0];
		colsep = args[1];
		dbSourceName = args[2];
		threads = Integer.parseInt(args[3]);
		server = args[4];
		dbName = args[5];
		port = Integer.parseInt(args[6]);
		String uid = args[7];
		String pwd = args[8];
		if (IBMExtractUtilities.isHexString(pwd)) {
			pwd = IBMExtractUtilities.Decrypt(pwd);
		}
		ddlGen = Boolean.valueOf(args[9]).booleanValue();

		dataUnload = Boolean.valueOf(args[10]).booleanValue();
		fetchSize = Integer.parseInt(args[11]);
		if (args.length >= 13)
			loadReplace = Boolean.valueOf(args[12]).booleanValue();
		String version = GenerateExtract.class.getPackage()
				.getImplementationVersion();
		if (version != null)
			log("Version " + version);
		log("OS Type:" + System.getProperty("os.name"));
		log("Java Version:" + System.getProperty("java.version") + ": "
				+ System.getProperty("java.vm.version") + ": "
				+ System.getProperty("java.vm.name") + ": "
				+ System.getProperty("java.vm.vendor") + ": "
				+ System.getProperty("sun.arch.data.model") + " bit");

		log("Default encoding " + System.getProperty("file.encoding"));
		log("TABLES_PROP_FILE:" + TABLES_PROP_FILE);
		log("DATAMAP_PROP_FILE:" + DATAMAP_PROP_FILE);
		if (dbSourceName.equalsIgnoreCase("domino"))
			dbName = server;
		log("colsep:" + colsep);
		log("dbSourceName:" + dbSourceName);
		log("threads:" + threads);
		log("server:" + server);
		log("dbName:" + dbName);
		log("port:" + port);
		log("uid:" + uid);
		if ((dbSourceName.equalsIgnoreCase("access"))
				|| (dbSourceName.equalsIgnoreCase("mysql"))) {
			if (dbSourceName.equalsIgnoreCase("access"))
				ddlGen = false;
			if (fetchSize != 0) {
				log("Warning: For source database="
						+ dbSourceName
						+ ", you should consider setting "
						+ "fetchsize=0 to be able to fetch large tables. Otherwise, you may run into "
						+ "outofmemory errors.");
			}
		}

		log("fetchSize:" + fetchSize);
		log("Timezone = " + System.getProperty("user.timezone") + " Offset="
				+ IBMExtractUtilities.getTimeZoneOffset());
		login.setProperty("user", uid);
		login.setProperty("password", pwd);

		if (dbSourceName.equalsIgnoreCase("mssql")) {
			login.setProperty("sendStringParametersAsUnicode", "true");
			login.setProperty("selectMethod", "cursor");
		}
		log(IBMExtractUtilities.getDBVersion(dbSourceName, server, port,
				dbName, uid, pwd));
		GenerateExtract pg = new GenerateExtract();
		pg.run();
	}

	public class BladeRunner extends Thread {
		private int number;
		private int spaceMult;
		long increment_value;
		long last_value;
		private ArrayList queue = new ArrayList();
		private boolean done = false;
		private int filesizelimit = 524288000;
		private long rowNum = 0L;
		private long lobNumber = 1L;
		private String dataDD;
		BufferedOutputStream blobWriter;
		BufferedOutputStream clobWriter;
		BufferedOutputStream xmlWriter;
		byte[] binaryData = new byte[33000];
		ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream(
				36000);
		private Connection bladeConn;

		private String getLobSeq(String name) {
			String lobName = "" + this.lobNumber;
			int i = lobName.length();
			while (i < 10) {
				lobName = "0" + lobName;
				i++;
			}
			this.lobNumber += 1L;
			return name + lobName;
		}

		public BladeRunner(int number, Connection conn) {
			this.number = number;
			this.bladeConn = conn;
			String url = IBMExtractUtilities.getURL(
					GenerateExtract.dbSourceName, GenerateExtract.server,
					GenerateExtract.port, GenerateExtract.dbName);
			if (GenerateExtract.dbTargetName.equals("zdb2"))
				this.dataDD = ZFile.allocDummyDDName();
			try {
				if (this.bladeConn != null) {
					this.bladeConn.close();
				}
				if (GenerateExtract.dbSourceName.equalsIgnoreCase("domino")) {
					this.bladeConn = DriverManager.getConnection(url);
				} else {
					this.bladeConn = DriverManager.getConnection(url,
							GenerateExtract.login);
					this.bladeConn.setAutoCommit(GenerateExtract.autoCommit);
					if (GenerateExtract.dbSourceName.equalsIgnoreCase("oracle")) {
						IBMExtractUtilities.CheckOracleRequisites(
								this.bladeConn, GenerateExtract.login
										.getProperty("user"));
					}
				}
			} catch (Exception ex) {
				GenerateExtract.log("sql exception connecting " + url + " "
						+ ex.getMessage());
				System.exit(-1);
			}
		}

		public void add(Object call) {
			synchronized (this.queue) {
				this.queue.add(call);
				this.queue.notify();
			}
		}

		public List getQueue() {
			return this.queue;
		}

		public void shutdown() {
			this.done = true;
			synchronized (this.queue) {
				this.queue.notifyAll();
			}
		}

		public void run() {
			setName("Blade_" + this.number);
			GenerateExtract.log("Starting " + Thread.currentThread().getName());

			long count = 0L;
			long last = System.currentTimeMillis();
			while (!this.done) {
				int size = -1;
				Object call = null;

				synchronized (this.queue) {
					call = this.queue.get(0);
				}

				if (call != null) {
					int key = ((Integer) call).intValue();
					count = dumpData(key);
					try {
						this.byteOutputStream.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					long now = System.currentTimeMillis();
					if (((GenerateExtract.dataUnload) && (GenerateExtract.ddlGen))
							|| ((GenerateExtract.dataUnload) && (!GenerateExtract.ddlGen)))
						GenerateExtract
								.log("Blade_"
										+ this.number
										+ " unloaded "
										+ count
										+ " rows in "
										+ (now - last)
										+ " ms for "
										+ GenerateExtract
												.removeQuote(GenerateExtract.schemaName[key])
										+ "."
										+ GenerateExtract
												.removeQuote(GenerateExtract.srcTableName[key]));
					else if ((!GenerateExtract.dataUnload)
							&& (GenerateExtract.ddlGen))
						GenerateExtract
								.log("Blade_"
										+ this.number
										+ " DDL Creation took "
										+ (now - last)
										+ " ms for "
										+ GenerateExtract
												.removeQuote(GenerateExtract.schemaName[key])
										+ "."
										+ GenerateExtract
												.removeQuote(GenerateExtract.srcTableName[key]));
					last = now;
					count = 0L;
				}

				synchronized (this.queue) {
					size = this.queue.size();
					if (size > 0) {
						call = this.queue.remove(0);
						size--;
					}
				}

				if (size < 1) {
					synchronized (GenerateExtract.empty) {
						GenerateExtract.empty.notify();
					}
				}

				try {
					synchronized (this.queue) {
						if (this.queue.size() < 1) {
							this.queue.wait();
						}
					}
				} catch (InterruptedException ex) {
					GenerateExtract.log("interrupted: " + ex);
				}
			}
			GenerateExtract.log("done " + Thread.currentThread().getName());
		}

		private void getDB2Type(GenerateExtract.DataMap map, int id) {
			String srcType = map.sourceDataType.replace(' ', '_');
			String targetType = (String) GenerateExtract.propDatamap
					.get(GenerateExtract.dbSourceName.toUpperCase() + "."
							+ srcType);
			map.varlength = false;
			if ((targetType == null) || (targetType == "")) {
				GenerateExtract.log("Missing data map for "
						+ GenerateExtract.dbSourceName.toUpperCase() + "."
						+ map.sourceDataType + " in file "
						+ GenerateExtract.DATAMAP_PROP_FILE + " for table "
						+ GenerateExtract.srcTableName[id]);

				GenerateExtract.log("Please add the entry in file "
						+ GenerateExtract.DATAMAP_PROP_FILE + " for datatype "
						+ map.sourceDataType);
				map.db2DataType = ("UNDEFINED /* Original data type was "
						+ map.sourceDataType + " */");
			} else {
				if ((GenerateExtract.dbSourceName.equalsIgnoreCase("mssql"))
						|| (GenerateExtract.dbSourceName
								.equalsIgnoreCase("oracle"))) {
					if (!GenerateExtract.graphic) {
						targetType = targetType.replaceFirst("GRAPHIC", "CHAR");
					}
					if (GenerateExtract.dbclob) {
						targetType = targetType.replaceFirst("DBCLOB", "CLOB");
					}
					if ((GenerateExtract.dbSourceName
							.equalsIgnoreCase("oracle"))
							&& (GenerateExtract.db2_compatibility)) {
						targetType = targetType.replaceFirst("NUMERIC",
								"NUMBER");
						if (srcType.equalsIgnoreCase("DATE"))
							targetType = srcType;
						if ((srcType.startsWith("VARCHAR2"))
								|| (srcType.startsWith("NVARCHAR2")))
							targetType = targetType.replaceFirst("VARCHAR",
									"VARCHAR2");
					}
				}
				if (GenerateExtract.dbTargetName.equalsIgnoreCase("zdb2")) {
					targetType = targetType.replaceFirst("CHAR FOR BIT DATA",
							"BINARY");
				}
				String[] tok = targetType.split(";");
				map.db2DataType = tok[0];
				if (tok.length > 1) {
					int i = 1;
					while (i < tok.length) {
						String[] tok2 = tok[i].split("=");
						switch (i) {
						case 1:
							if ((!tok2[0].equalsIgnoreCase("varlength"))
									|| (!tok2[1].equalsIgnoreCase("true")))
								break;
							map.varlength = true;
							break;
						case 2:
							if (!tok2[0].equalsIgnoreCase("default"))
								break;
							map.defaultlength = Integer.parseInt(tok2[1]);
							break;
						case 3:
							if (!tok2[0].equalsIgnoreCase("useactualdata")) {
								break;
							}
							map.useActualData = Boolean.valueOf(tok2[1])
									.booleanValue();
							break;
						}

						i++;
					}
				}
			}
		}

		private String getDeleteRule(DatabaseMetaData dm, int rule) {
			String tmp;
			if (rule == 0) {
				tmp = "CASCADE";
			} else {
				if (rule == 3) {
					tmp = "NO ACTION";
				} else {
					if (rule == 1) {
						tmp = "RESTRICT";
					} else {
						if (rule == 2)
							tmp = "SET NULL";
						else
							tmp = "";
					}
				}
			}
			return tmp;
		}

		private String getUpdateRule(DatabaseMetaData dm, int rule) {
			String tmp;
			if (rule == 3) {
				tmp = "NO ACTION";
			} else {
				if (rule == 1)
					tmp = "RESTRICT";
				else
					tmp = "";
			}
			return tmp;
		}

		private void genDB2UDFs() throws Exception {
			String udf = "CREATE FUNCTION DB2.NEWGUID() "
					+ GenerateExtract.linesep
					+ "RETURNS CHAR(32) "
					+ GenerateExtract.linesep
					+ "NOT DETERMINISTIC "
					+ GenerateExtract.linesep
					+ "RETURN hex(generate_unique()) || hex(CHR(CAST(RAND()*255 AS SMALLINT))) || "
					+ GenerateExtract.linesep
					+ "       hex(CHR(CAST(RAND()*255 AS SMALLINT))) || "
					+ GenerateExtract.linesep
					+ "       hex(CHR(CAST(RAND()*255 AS SMALLINT))) "
					+ GenerateExtract.linesep + "; " + GenerateExtract.linesep;

			GenerateExtract.db2udfWriter.write(udf);
		}

		private void genDB2Fkeys(int id, ResultSetMetaData md)
				throws SQLException, IOException {
			String schema = GenerateExtract
					.removeQuote(GenerateExtract.srcSchName[id]);

			StringBuffer buffer = new StringBuffer();
			StringBuffer dropkey = new StringBuffer();
			DatabaseMetaData dmetadata = this.bladeConn.getMetaData();

			String catalogName = null;

			PreparedStatement stat = null;

			String pkTableSchema = GenerateExtract.schemaName[id].toUpperCase();
			String table;
			if (GenerateExtract.dbSourceName.equals("postgres")) {
				table = GenerateExtract
						.removeQuote(GenerateExtract.srcTableName[id]
								.toLowerCase());
			} else {
				table = GenerateExtract
						.removeQuote(GenerateExtract.srcTableName[id]);
			}
			ResultSet fkeys;
			if (GenerateExtract.dbSourceName.equals("oracle")) {
				String sql = "SELECT NULL AS PKTABLE_CAT, t3.owner AS PKTABLE_SCHEM, t3.table_name AS PKTABLE_NAME, t3.column_name AS PKCOLUMN_NAME, NULL AS FKTABLE_CAT, t1.owner AS FKTABLE_SCHEM, t1.table_name AS FKTABLE_NAME, t1.column_name AS FKCOLUMN_NAME, t1.position AS KEY_SEQ, decode(t4.delete_rule,'CASCADE', 0, 'NO ACTION', 3, 'RESTRICT', 1, 'SET NULL', 2, 0) AS UPDATE_RULE, decode(t2.delete_rule,'CASCADE', 0, 'NO ACTION', 3, 'RESTRICT', 1, 'SET NULL', 2, 0) AS DELETE_RULE, t1.constraint_name AS FK_NAME, t3.constraint_name AS PK_NAME, NULL AS DEFERRABILITY from dba_cons_columns t1, dba_constraints t2, dba_cons_columns t3, dba_constraints t4 where t1.table_name = t2.table_name and t1.constraint_name = t2.constraint_name and t1.owner = t2.owner and t2.CONSTRAINT_TYPE  = 'R' and t4.constraint_name = t2.r_constraint_name and t4.OWNER = t2.R_OWNER and t4.table_name = t3.table_name and t4.CONSTRAINT_NAME = t3.CONSTRAINT_NAME and t4.owner = t3.owner and t1.position = t3.position and t1.owner = '"
						+ schema
						+ "' "
						+ "and t1.table_name = '"
						+ table
						+ "' "
						+ "order by t1.table_name, t1.constraint_name, t1.position";

				stat = this.bladeConn.prepareStatement(sql);
				fkeys = stat.executeQuery();
			} else {
				if (GenerateExtract.dbSourceName.equals("db2")) {
					fkeys = dmetadata.getImportedKeys("", schema, table);
				} else {
					if (GenerateExtract.dbSourceName.equals("zdb2")) {
						fkeys = dmetadata.getImportedKeys("", schema, table);
					} else {
						if (GenerateExtract.dbSourceName.equals("idb2")) {
							catalogName = md.getCatalogName(1);
							fkeys = dmetadata.getImportedKeys(catalogName,
									schema, table);
						} else {
							if (GenerateExtract.dbSourceName.equals("hxtt")) {
								fkeys = dmetadata.getImportedKeys(null, null,
										table);
							} else
								fkeys = dmetadata.getImportedKeys(
										GenerateExtract.dbName, null, table);
						}
					}
				}
			}
			buffer.setLength(0);
			dropkey.setLength(0);

			while (fkeys.next()) {
				String fkConsName = fkeys.getString(12);
				if (fkConsName != null)
					fkConsName = fkConsName.toUpperCase();
				pkTableSchema = fkeys.getString(2);
				if (pkTableSchema != null)
					pkTableSchema = pkTableSchema.toUpperCase();
				else
					pkTableSchema = schema;
				String pkTableName = fkeys.getString(3);
				if (pkTableName != null)
					pkTableName = pkTableName.toUpperCase();
				String pkColumnName;
				if (GenerateExtract.retainColName) {
					pkColumnName = GenerateExtract.getTruncName(0, "", fkeys
							.getString(4).toUpperCase(), 128);
				} else {
					pkColumnName = GenerateExtract.getTruncName(0, "", fkeys
							.getString(4).toUpperCase(), 30);
				}
				String fkTableSchema = fkeys.getString(6);
				if (fkTableSchema != null)
					fkTableSchema = fkTableSchema.toUpperCase();
				else
					fkTableSchema = schema;
				String fkTableName = fkeys.getString(7).toUpperCase();
				String fkColumnName;
				if (GenerateExtract.retainColName) {
					fkColumnName = GenerateExtract.getTruncName(0, "", fkeys
							.getString(8).toUpperCase(), 128);
				} else {
					fkColumnName = GenerateExtract.getTruncName(0, "", fkeys
							.getString(8).toUpperCase(), 30);
				}
				int keyseq = fkeys.getShort(9);
				int updateRule = fkeys.getShort(10);
				int deleteRule = fkeys.getShort(11);
				if (keyseq == 1) {
					if (buffer.length() > 0) {
						int start1 = buffer.indexOf(",abcxyz");
						buffer = buffer.delete(start1, start1 + 7);
						int start2 = buffer.indexOf(",xyzabc");
						buffer = buffer.delete(start2, start2 + 7);
						GenerateExtract.db2FKWriter.write(buffer.toString());
					}
					buffer.setLength(0);
					String tmp = GenerateExtract
							.removeQuote(GenerateExtract.tableName[id]);
					tmp = tmp.replace("-", "_");
					String newFKConsName = GenerateExtract.getTruncName(0, "",
							"FK" + GenerateExtract.numFkey + "_" + tmp, 18);
					if (GenerateExtract.retainConstraintsName) {
						if ((fkConsName != null) && (fkConsName.length() != 0)) {
							newFKConsName = fkConsName;
						}
					}
					buffer.append("--#SET :FOREIGN_KEYS:"
							+ GenerateExtract.removeQuote(fkTableSchema) + ":"
							+ newFKConsName + GenerateExtract.linesep);
					buffer.append("ALTER TABLE \"" + fkTableSchema + "\".\""
							+ fkTableName + "\"" + GenerateExtract.linesep);
					buffer.append("ADD CONSTRAINT " + newFKConsName
							+ " FOREIGN KEY" + GenerateExtract.linesep);
					buffer.append("(" + GenerateExtract.linesep);
					buffer.append("\"" + fkColumnName + "\",abcxyz");
					buffer.append(GenerateExtract.linesep);
					buffer.append(")" + GenerateExtract.linesep);
					buffer.append("REFERENCES " + pkTableSchema + ".\""
							+ pkTableName + "\"" + GenerateExtract.linesep);
					buffer.append("(" + GenerateExtract.linesep);
					buffer.append("\"" + pkColumnName + "\",xyzabc");
					buffer.append(GenerateExtract.linesep);
					buffer.append(")" + GenerateExtract.linesep);
					String upd = getUpdateRule(dmetadata, updateRule);
					String del = getDeleteRule(dmetadata, deleteRule);
					if ((GenerateExtract.dbTargetName.equals("db2luw"))
							&& (!upd.equals("")))
						buffer.append("ON UPDATE " + upd
								+ GenerateExtract.linesep);
					if (!del.equals(""))
						buffer.append("ON DELETE " + del
								+ GenerateExtract.linesep);
					buffer.append(";" + GenerateExtract.linesep);

					dropkey.setLength(0);
					dropkey.append("ALTER TABLE \"" + fkTableSchema + "\".\""
							+ fkTableName + "\"" + GenerateExtract.linesep);
					dropkey.append("DROP CONSTRAINT " + newFKConsName
							+ GenerateExtract.linesep);
					dropkey.append(";" + GenerateExtract.linesep);
					dropkey.append(GenerateExtract.linesep);
					GenerateExtract.db2FKDropWriter.write(dropkey.toString());
					continue;
				}
				buffer.insert(buffer.indexOf("abcxyz") - 1, ","
						+ GenerateExtract.putQuote(fkColumnName));
				buffer.insert(buffer.indexOf("xyzabc") - 1, ","
						+ GenerateExtract.putQuote(pkColumnName));
			}

			if (buffer.length() > 0) {
				int start1 = buffer.indexOf(",abcxyz");
				buffer = buffer.delete(start1, start1 + 7);
				int start2 = buffer.indexOf(",xyzabc");
				buffer = buffer.delete(start2, start2 + 7);
				GenerateExtract.db2FKWriter.write(buffer.toString());
			}

			if (stat != null)
				stat.close();
			if (fkeys != null)
				fkeys.close();
		}

		private boolean getOraUniqueIndex(String schema, String table,
				String index) throws SQLException {
			int indexColCount = 0;
			boolean result = true;
			String unique = "";

			String sql = "SELECT COUNT(*) FROM DBA_TAB_COLUMNS A, DBA_IND_COLUMNS C WHERE A.OWNER = C.TABLE_OWNER AND A.TABLE_NAME = C.TABLE_NAME AND A.COLUMN_NAME = C.COLUMN_NAME AND C.INDEX_NAME = '"
					+ GenerateExtract.removeQuote(index)
					+ "' "
					+ "AND C.TABLE_OWNER = '"
					+ schema
					+ "' "
					+ "AND C.TABLE_NAME = '"
					+ table
					+ "' "
					+ "AND A.NULLABLE = 'Y'";

			String sql2 = "SELECT UNIQUENESS FROM DBA_INDEXES WHERE TABLE_OWNER = '"
					+ schema
					+ "' "
					+ "AND TABLE_NAME = '"
					+ table
					+ "' "
					+ "AND INDEX_NAME = '"
					+ GenerateExtract.removeQuote(index)
					+ "' ";

			String sql3 = "SELECT COUNT(*) FROM DBA_TAB_COLUMNS A, DBA_IND_COLUMNS C WHERE A.OWNER = C.TABLE_OWNER AND A.TABLE_NAME = C.TABLE_NAME AND A.COLUMN_NAME = C.COLUMN_NAME AND C.INDEX_NAME = '"
					+ GenerateExtract.removeQuote(index)
					+ "' "
					+ "AND C.TABLE_OWNER = '"
					+ schema
					+ "' "
					+ "AND C.TABLE_NAME = '" + table + "' ";

			PreparedStatement partStatement = this.bladeConn
					.prepareStatement(sql2);
			ResultSet rs = partStatement.executeQuery();
			while (rs.next()) {
				unique = rs.getString(1);
			}
			if (rs != null)
				rs.close();
			if (partStatement != null) {
				partStatement.close();
			}

			result = !unique.equalsIgnoreCase("UNIQUE");
			if (!result) {
				partStatement = this.bladeConn.prepareStatement(sql3);
				rs = partStatement.executeQuery();
				while (rs.next()) {
					indexColCount = rs.getInt(1);
				}
				if (rs != null)
					rs.close();
				if (partStatement != null) {
					partStatement.close();
				}
				if (indexColCount == 1) {
					boolean notNullColumn = true;
					partStatement = this.bladeConn.prepareStatement(sql);
					rs = partStatement.executeQuery();
					while (rs.next()) {
						if (rs.getInt(1) == 1)
							notNullColumn = false;
					}
					if (rs != null)
						rs.close();
					if (partStatement != null)
						partStatement.close();
					if (!notNullColumn)
						result = true;
				}
			}
			return result;
		}

		private String getDB2XMLIndexPattern(String schemaName, String indexName) {
			String xmlPattern = "";
			String hashed = "";
			String ignore = "";

			String sql = "SELECT DATATYPE, LENGTH, HASHED, TYPEMODEL, PATTERN FROM SYSCAT.INDEXXMLPATTERNS WHERE INDSCHEMA = '"
					+ schemaName + "' " + "AND INDNAME = '" + indexName + "' ";

			if (GenerateExtract.debug)
				GenerateExtract.log("getDB2XMLIndexPattern=" + sql);
			try {
				PreparedStatement partStatement = this.bladeConn
						.prepareStatement(sql);
				ResultSet rs = partStatement.executeQuery();
				if (rs.next()) {
					if (rs.getString("HASHED").equalsIgnoreCase("Y"))
						hashed = " HASHED ";
					if (rs.getString("TYPEMODEL").equalsIgnoreCase("Q"))
						ignore = " IGNORE INVALID VALUES ";
					else if (rs.getString("TYPEMODEL").equalsIgnoreCase("R"))
						ignore = " REJECT INVALID VALUES ";
					if (rs.getInt("LENGTH") > 0) {
						xmlPattern = "GENERATE KEY USING XMLPATTERN '"
								+ rs.getString("PATTERN") + "' AS SQL "
								+ rs.getString("DATATYPE") + "("
								+ rs.getString("LENGTH") + ")";
					} else {
						xmlPattern = "GENERATE KEY USING XMLPATTERN '"
								+ rs.getString("PATTERN") + "' AS SQL "
								+ rs.getString("DATATYPE");
					}
					xmlPattern = xmlPattern + hashed + ignore;
				}
				if (rs != null)
					rs.close();
				if (partStatement != null)
					partStatement.close();
			} catch (Exception e) {
				xmlPattern = "";
			}
			return xmlPattern;
		}

		private String getFunctionIndexExpression(String owner, String indexName) {
			String sql = "select column_expression from dba_ind_expressions where index_owner = '"
					+ owner + "' " + "and index_name = '" + indexName + "'";

			String expression = GenerateExtract.this.executeSQL(sql, false);
			return expression;
		}

		private void genIndexes(String schema, String dstSchema, String table,
				String pkName) throws SQLException, IOException {
			String colTag = "";

			StringBuffer buffer = new StringBuffer();
			ResultSet indexes = null;
			PreparedStatement partStatement = null;
			String sql = "SELECT NULL AS TABLE_CAT, INDEX_OWNER AS TABLE_SCHEMA, TABLE_NAME AS TABLE_NAME, NULL AS NON_UNIQUE, NULL AS INDEX_QUALIFIER, INDEX_NAME AS INDEX_NAME, NULL AS TYPE, COLUMN_POSITION AS ORDINAL_POSITION, COLUMN_NAME AS COLUMN_NAME, DESCEND AS ASC_OR_DSC, NULL AS CARDINALITY, NULL AS PAGES, NULL AS FITER_CONDITION FROM DBA_IND_COLUMNS WHERE  INDEX_OWNER = '"
					+ schema
					+ "' "
					+ "AND TABLE_NAME = '"
					+ table
					+ "' "
					+ "ORDER BY 6, 8";

			if (GenerateExtract.dbSourceName.equalsIgnoreCase("oracle")) {
				partStatement = this.bladeConn.prepareStatement(sql);
				indexes = partStatement.executeQuery();
			} else if (GenerateExtract.dbSourceName.equalsIgnoreCase("mssql")) {
				indexes = this.bladeConn.getMetaData().getIndexInfo(
						GenerateExtract.dbName, schema, table, false, true);
			} else if (GenerateExtract.dbSourceName.equalsIgnoreCase("zdb2")) {
				indexes = this.bladeConn.getMetaData().getIndexInfo(null,
						schema, table, false, true);
			} else if (GenerateExtract.dbSourceName.equalsIgnoreCase("idb2")) {
				indexes = this.bladeConn.getMetaData().getIndexInfo(null,
						schema, table, false, true);
			} else if (GenerateExtract.dbSourceName.equalsIgnoreCase("db2")) {
				indexes = this.bladeConn.getMetaData().getIndexInfo(null,
						schema, table, false, true);
			} else if (GenerateExtract.dbSourceName.equalsIgnoreCase("hxtt")) {
				indexes = this.bladeConn.getMetaData().getIndexInfo(null, null,
						table, false, true);
			} else {
				indexes = this.bladeConn.getMetaData().getIndexInfo(
						GenerateExtract.dbName, null, table, false, true);
			}

			buffer.setLength(0);
			String indexName = "~";
			String oldIndName = "~";
			boolean once = false;
			boolean nonUnique = true;
			while (indexes.next()) {
				indexName = indexes.getString(6);
				if (indexName == null)
					continue;
				String columnName;
				if (GenerateExtract.retainColName) {
					columnName = GenerateExtract.getTruncName(0, "", indexes
							.getString(9), 128);
				} else {
					columnName = GenerateExtract.getTruncName(0, "", indexes
							.getString(9), 30);
				}
				short position = indexes.getShort(8);
				String direction = indexes.getString(10);
				if (GenerateExtract.dbSourceName.equalsIgnoreCase("oracle")) {
					if (columnName.startsWith("SYS")) {
						String expression = getFunctionIndexExpression(schema,
								indexName);
						colTag = " -- '" + expression
								+ "' Not supported in DB2 yet";
					}
					if (!oldIndName.equals(indexName)) {
						nonUnique = getOraUniqueIndex(schema, table, indexName);
						once = true;
					}
				} else {
					nonUnique = indexes.getBoolean(4);
				}
				boolean pkfound;
				if (GenerateExtract.dbSourceName.equalsIgnoreCase("idb2"))
					pkfound = indexName.equalsIgnoreCase(table);
				else
					pkfound = indexName.equalsIgnoreCase(pkName);
				if (position == 1) {
					if (buffer.length() > 0) {
						buffer.append(GenerateExtract.linesep);
						if (GenerateExtract.dbTargetName.equals("db2luw")) {
							buffer.append(")" + GenerateExtract.linesep);

							if (GenerateExtract.dbSourceName.equals("db2")) {
								String xmlPattern = getDB2XMLIndexPattern(
										schema, oldIndName);
								if (!xmlPattern.equals(""))
									buffer.append(xmlPattern
											+ GenerateExtract.linesep);
							}
							if (GenerateExtract.compressIndex) {
								buffer
										.append("ALLOW REVERSE SCANS COMPRESS YES"
												+ GenerateExtract.linesep
												+ ";"
												+ GenerateExtract.linesep);
							} else
								buffer.append("ALLOW REVERSE SCANS"
										+ GenerateExtract.linesep + ";"
										+ GenerateExtract.linesep);
						} else {
							buffer.append(")" + GenerateExtract.linesep + ";"
									+ GenerateExtract.linesep);
						}
						buffer.append(GenerateExtract.linesep);
						if (!oldIndName.equalsIgnoreCase(pkName)) {
							GenerateExtract.db2ConsWriter.write(buffer
									.toString());
						}
					}
					oldIndName = indexName;
					buffer.setLength(0);
					if (pkfound)
						continue;
					if (nonUnique) {
						String newIndexName = GenerateExtract.getTruncName(0,
								"", "IX" + GenerateExtract.numIndex + "_"
										+ GenerateExtract.removeQuote(table),
								18);
						newIndexName = newIndexName.replace("-", "_");
						if (GenerateExtract.retainConstraintsName) {
							if ((indexName != null)
									&& (indexName.length() != 0)) {
								newIndexName = indexName;
							}
						}
						buffer.append("--#SET :INDEX:"
								+ GenerateExtract.removeQuote(dstSchema) + ":"
								+ newIndexName + GenerateExtract.linesep);
						buffer.append("CREATE INDEX " + dstSchema + ".\""
								+ newIndexName + "\" ON " + dstSchema + ".\""
								+ table.toUpperCase() + "\""
								+ GenerateExtract.linesep);
					} else {
						String newIndexName = GenerateExtract.getTruncName(0,
								"", "UQ" + GenerateExtract.numIndex + "_"
										+ GenerateExtract.removeQuote(table),
								18);
						newIndexName = newIndexName.replace("-", "_");
						if (GenerateExtract.retainConstraintsName) {
							if ((indexName != null)
									&& (indexName.length() != 0)) {
								newIndexName = indexName;
							}
						}
						buffer.append("--#SET :UNIQUE_INDEX:"
								+ GenerateExtract.removeQuote(dstSchema) + ":"
								+ newIndexName + GenerateExtract.linesep);
						buffer.append("CREATE UNIQUE INDEX " + dstSchema
								+ ".\"" + newIndexName + "\" ON " + dstSchema
								+ ".\"" + table.toUpperCase() + "\""
								+ GenerateExtract.linesep);
					}

					buffer.append("(" + GenerateExtract.linesep);
					buffer
							.append(GenerateExtract.putQuote(columnName)
									+ colTag);
					continue;
				}

				if ((pkfound) || (position <= 0))
					continue;
				buffer.append("," + GenerateExtract.linesep);
				buffer.append(GenerateExtract.putQuote(columnName) + colTag);
			}

			if (indexes != null)
				indexes.close();
			if (partStatement != null)
				partStatement.close();
			if (buffer.length() > 0) {
				buffer.append(GenerateExtract.linesep);
				if (GenerateExtract.dbTargetName.equals("db2luw")) {
					buffer.append(")" + GenerateExtract.linesep);

					if (GenerateExtract.dbSourceName.equals("db2")) {
						String xmlPattern = getDB2XMLIndexPattern(schema,
								oldIndName);
						if (!xmlPattern.equals(""))
							buffer.append(xmlPattern + GenerateExtract.linesep);
					}
					if (GenerateExtract.loadstats)
						buffer.append("ALLOW REVERSE SCANS COLLECT STATISTICS"
								+ GenerateExtract.linesep);
					else
						buffer.append("ALLOW REVERSE SCANS"
								+ GenerateExtract.linesep);
					if (GenerateExtract.compressIndex) {
						buffer.append("COMPRESS YES" + GenerateExtract.linesep
								+ ";" + GenerateExtract.linesep);
					} else
						buffer.append(";" + GenerateExtract.linesep);
				} else {
					buffer.append(")" + GenerateExtract.linesep + ";"
							+ GenerateExtract.linesep);
				}
				buffer.append(GenerateExtract.linesep);
				if (!oldIndName.equalsIgnoreCase(pkName)) {
					GenerateExtract.db2ConsWriter.write(buffer.toString());
				}
			}
		}

		private void genDB2TableKeys(int id) throws SQLException, IOException {
			Hashtable pkCols = new Hashtable();
			short keySeq = 0;
			String schema = GenerateExtract
					.removeQuote(GenerateExtract.srcSchName[id]);
			String dstSchema = GenerateExtract.schemaName[id].toUpperCase();
			String table = GenerateExtract
					.removeQuote(GenerateExtract.srcTableName[id]);
			String isTempTable = "";

			String pkName = "";
			boolean found = false;
			StringBuffer buffer = new StringBuffer();
			if (GenerateExtract.dbSourceName.equalsIgnoreCase("postgres"))
				table = GenerateExtract
						.removeQuote(GenerateExtract.srcTableName[id]
								.toLowerCase());
			else
				table = GenerateExtract
						.removeQuote(GenerateExtract.srcTableName[id]);
			ResultSet pkeys;
			if (GenerateExtract.dbSourceName.equalsIgnoreCase("oracle")) {
				pkeys = this.bladeConn.getMetaData().getPrimaryKeys(null,
						schema, table);
			} else {
				if (GenerateExtract.dbSourceName.equalsIgnoreCase("hxtt")) {
					pkeys = this.bladeConn.getMetaData().getPrimaryKeys(null,
							null, table);
				} else {
					if (GenerateExtract.dbSourceName.equalsIgnoreCase("zdb2")) {
						pkeys = this.bladeConn.getMetaData().getPrimaryKeys(
								null, schema, table);
					} else {
						if (GenerateExtract.dbSourceName
								.equalsIgnoreCase("idb2")) {
							pkeys = this.bladeConn.getMetaData()
									.getPrimaryKeys(null, schema, table);
						} else {
							if (GenerateExtract.dbSourceName
									.equalsIgnoreCase("db2"))
								pkeys = this.bladeConn.getMetaData()
										.getPrimaryKeys(null, schema, table);
							else
								pkeys = this.bladeConn.getMetaData()
										.getPrimaryKeys(GenerateExtract.dbName,
												null, table);
						}
					}
				}
			}
			buffer.setLength(0);

			if (GenerateExtract.dbSourceName.equalsIgnoreCase("oracle")) {
				isTempTable = isOracleTabTemporary(schema, table);
			}
			if (isTempTable.equals("")) {
				pkCols.clear();
				while (pkeys.next()) {
					pkName = pkeys.getString(6);
					keySeq = pkeys.getShort(5);
					String columnName;
					if (GenerateExtract.retainColName) {
						columnName = GenerateExtract.getTruncName(0, "", pkeys
								.getString(4), 128);
					} else {
						columnName = GenerateExtract.getTruncName(0, "", pkeys
								.getString(4), 30);
					}

					pkCols.put(new Integer(keySeq), columnName);
				}

				String newPKName = "PK_" + table.toUpperCase();
				newPKName = newPKName.replace("-", "_");
				if (newPKName.length() > 18)
					newPKName = newPKName.substring(0, 18);
				if (GenerateExtract.retainConstraintsName) {
					if ((pkName != null) && (pkName.length() != 0))
						newPKName = pkName;
				}
				buffer.append("--#SET :PRIMARY_KEY:"
						+ GenerateExtract.removeQuote(dstSchema) + ":"
						+ newPKName + GenerateExtract.linesep);
				buffer.append("ALTER TABLE " + dstSchema + "."
						+ GenerateExtract.tableName[id].toUpperCase()
						+ GenerateExtract.linesep);
				buffer.append("ADD CONSTRAINT " + newPKName + " PRIMARY KEY"
						+ GenerateExtract.linesep);
				buffer.append("(" + GenerateExtract.linesep);
				Vector v = new Vector(pkCols.keySet());
				Collections.sort(v);
				for (Enumeration e = v.elements(); e.hasMoreElements();) {
					if (found)
						buffer.append(",");
					String val = (String) pkCols.get((Integer) e.nextElement());
					buffer.append(GenerateExtract.putQuote(val));
					found = true;
				}
				if (pkeys != null)
					pkeys.close();
				buffer.append(GenerateExtract.linesep);
				if (GenerateExtract.compressIndex) {
					if (isTempTable.equals(""))
						buffer.append(")" + GenerateExtract.linesep
								+ "COMPRESS YES" + GenerateExtract.linesep
								+ ";" + GenerateExtract.linesep);
					else
						buffer.append(")" + GenerateExtract.linesep + ";"
								+ GenerateExtract.linesep);
				} else {
					buffer.append(")" + GenerateExtract.linesep + ";"
							+ GenerateExtract.linesep);
				}
				buffer.append(GenerateExtract.linesep);
				if (found)
					GenerateExtract.db2ConsWriter.write(buffer.toString());
			} else {
				pkCols.clear();
				while (pkeys.next()) {
					pkName = pkeys.getString(6);
					String columnName;
					if (GenerateExtract.retainColName) {
						columnName = GenerateExtract.getTruncName(0, "", pkeys
								.getString(4), 128);
					} else {
						columnName = GenerateExtract.getTruncName(0, "", pkeys
								.getString(4), 30);
					}
					pkCols.put(new Integer(keySeq), columnName);
				}
				String newPKName = "PK_" + table.toUpperCase();
				newPKName = newPKName.replace("-", "_");
				if (newPKName.length() > 18)
					newPKName = newPKName.substring(0, 18);
				newPKName = GenerateExtract.removeQuote(newPKName);
				if (GenerateExtract.retainConstraintsName) {
					if ((pkName != null) && (pkName.length() != 0))
						newPKName = pkName;
				}
				buffer.append("--#SET :UNIQUE_INDEX:"
						+ GenerateExtract.removeQuote(dstSchema) + ":"
						+ newPKName + GenerateExtract.linesep);
				buffer.append("CREATE UNIQUE INDEX " + dstSchema + "."
						+ newPKName + " ON " + dstSchema + "."
						+ GenerateExtract.tableName[id].toUpperCase()
						+ GenerateExtract.linesep);
				buffer.append("(" + GenerateExtract.linesep);
				Vector v = new Vector(pkCols.keySet());
				Collections.sort(v);
				for (Enumeration e = v.elements(); e.hasMoreElements();) {
					if (found)
						buffer.append(",");
					String val = (String) pkCols.get((Integer) e.nextElement());
					buffer.append(GenerateExtract.putQuote(val));
					found = true;
				}
				if (pkeys != null)
					pkeys.close();
				buffer.append(GenerateExtract.linesep);
				if (GenerateExtract.compressIndex) {
					if (isTempTable.equals(""))
						buffer.append(")" + GenerateExtract.linesep
								+ "COMPRESS YES" + GenerateExtract.linesep
								+ ";" + GenerateExtract.linesep);
					else
						buffer.append(")" + GenerateExtract.linesep + ";"
								+ GenerateExtract.linesep);
				} else {
					buffer.append(")" + GenerateExtract.linesep + ";"
							+ GenerateExtract.linesep);
				}
				buffer.append(GenerateExtract.linesep);
				if (found)
					GenerateExtract.db2ConsWriter.write(buffer.toString());
			}
			genIndexes(schema, dstSchema, table, pkName);
		}

		private String massageOraDate(String def) {
			if ((def != null) && (def.length() != 0))
				def = def.trim();
			if (def.equalsIgnoreCase("sysdate")) {
				if ((GenerateExtract.releaseLevel != -1.0F)
						&& (GenerateExtract.releaseLevel >= 9.7F)) {
					return " SYSDATE ";
				}
				return " CURRENT TIMESTAMP ";
			}

			return "'" + formatDate(def) + "'";
		}

		private String formatsqlServerDate(String strDate) {
			Pattern p = Pattern.compile(".*(\\d{4})\\-(\\d{2})\\-(\\d{2}).*");
			Matcher m = p.matcher(strDate);
			if (m.matches()) {
				return "'" + m.group(1) + "-" + m.group(2) + "-" + m.group(3)
						+ "-00.00.00.000000'";
			}
			return strDate;
		}

		private void genDB2DefaultValues(String dstSchemaName,
				String schemaName, String tabName, String colName,
				String nullType, String dataType) throws Exception {
			DatabaseMetaData dbmeta = this.bladeConn.getMetaData();
			ResultSet rs2 = null;
			String defValue = "";
			String dstSchema = dstSchemaName.toUpperCase();
			dstSchema = GenerateExtract.removeQuote(dstSchema);

			if (GenerateExtract.dbSourceName.equalsIgnoreCase("postgres")) {
				rs2 = dbmeta.getColumns(null, GenerateExtract
						.removeQuote(schemaName.toLowerCase()), GenerateExtract
						.removeQuote(tabName.toLowerCase()), colName
						.toLowerCase());
				while (rs2.next()) {
					defValue = rs2.getString("COLUMN_DEF");
				}
				if (rs2 != null)
					rs2.close();
				if ((defValue != null) && (!defValue.equals(""))) {
					if (defValue.startsWith("nextval")) {
						defValue = "";
					} else {
						defValue = defValue.replace('(', ' ');
						defValue = defValue.replace(')', ' ');
						if (defValue.equalsIgnoreCase("true"))
							defValue = "1";
						else if (defValue.equalsIgnoreCase("false"))
							defValue = "0";
						else if ((defValue.equalsIgnoreCase("\"current_user\""))
								|| (defValue.indexOf("current_user") > 0))
							defValue = "CURRENT_USER";
						else if (defValue.indexOf("::bpchar") > 0)
							defValue = defValue.substring(0, defValue
									.indexOf("::bpchar"));
						else if (defValue.indexOf("::text") > 0)
							defValue = defValue.substring(0, defValue
									.indexOf("::text"));
						else if (defValue.indexOf("::character varying") > 0)
							defValue = defValue.substring(0, defValue
									.indexOf("::character varying"));
						else if ((defValue.indexOf("'now'") > 0)
								|| (defValue.indexOf("now") > 0)) {
							if (dataType.equalsIgnoreCase("TIMESTAMP"))
								defValue = " CURRENT TIMESTAMP ";
							else if (dataType.equalsIgnoreCase("TIME"))
								defValue = " CURRENT TIME ";
							else if (dataType.equalsIgnoreCase("DATE"))
								defValue = " CURRENT DATE ";
							else
								defValue = " CURRENT TIMESTAMP ";
						}
						defValue = " WITH DEFAULT " + defValue;
					}
				}
			} else if (GenerateExtract.dbSourceName.equalsIgnoreCase("oracle")) {
				String sql = "SELECT DEFAULT_LENGTH, DATA_DEFAULT FROM DBA_TAB_COLUMNS WHERE TABLE_NAME = '"
						+ GenerateExtract.removeQuote(tabName)
						+ "' "
						+ "AND OWNER = '"
						+ GenerateExtract.removeQuote(schemaName)
						+ "' "
						+ "AND COLUMN_NAME = '"
						+ GenerateExtract.removeQuote(colName) + "'";

				PreparedStatement defStatement = this.bladeConn
						.prepareStatement(sql);
				rs2 = defStatement.executeQuery();

				if (GenerateExtract.debug)
					GenerateExtract.log("genDB2DefaultValues=" + sql);
				while (rs2.next()) {
					String def_length = rs2.getString(1);
					String defaultValue = rs2.getString(2);
					if ((defaultValue == null) || (defaultValue.equals("")))
						continue;
					if ((dataType.startsWith("DATE"))
							|| (dataType.startsWith("TIMESTAMP"))) {
						defValue = " WITH DEFAULT "
								+ massageOraDate(defaultValue);
						continue;
					}

					if (defaultValue.charAt(0) == '(') {
						defaultValue = defaultValue.substring(1, defaultValue
								.lastIndexOf(')'));
					}
					defValue = " WITH DEFAULT " + defaultValue + " ";
				}

				if (rs2 != null)
					rs2.close();
				if (defStatement != null)
					defStatement.close();
			} else if (GenerateExtract.dbSourceName.equalsIgnoreCase("zdb2")) {
				String sql = "SELECT DEFAULT, DEFAULTVALUE FROM SYSIBM.SYSCOLUMNS WHERE TBNAME = '"
						+ GenerateExtract.removeQuote(tabName)
						+ "' "
						+ "AND TBCREATOR = '"
						+ GenerateExtract.removeQuote(schemaName)
						+ "' "
						+ "AND NAME = '"
						+ GenerateExtract.removeQuote(colName)
						+ "'";

				PreparedStatement defStatement = this.bladeConn
						.prepareStatement(sql);
				rs2 = defStatement.executeQuery();
				if (GenerateExtract.debug)
					GenerateExtract.log("genDB2DefaultValues=" + sql);
				while (rs2.next()) {
					String def = rs2.getString(1);
					if (def.equalsIgnoreCase("1")) {
						String defaultValue = rs2.getString(2);
						if ((dataType.startsWith("VARCHAR"))
								|| (dataType.startsWith("CHAR"))
								|| (dataType.startsWith("TIMESTAMP"))
								|| (dataType.startsWith("DATE"))
								|| (dataType.startsWith("TIME"))
								|| (dataType.startsWith("CLOB"))) {
							defValue = " WITH DEFAULT '" + defaultValue + "'";
							continue;
						}
						defValue = " WITH DEFAULT " + defaultValue + " ";
						continue;
					}
					if (def.equalsIgnoreCase("U")) {
						defValue = " WITH DEFAULT USER ";
						continue;
					}
					if (!def.equalsIgnoreCase("Y"))
						continue;
					if (nullType.equals("")) {
						defValue = " WITH DEFAULT NULL ";
						continue;
					}
					defValue = " WITH DEFAULT ";
				}

				if (rs2 != null)
					rs2.close();
				if (defStatement != null)
					defStatement.close();
			} else if (GenerateExtract.dbSourceName.equalsIgnoreCase("db2")) {
				String sql = "SELECT DEFAULT FROM SYSCAT.COLUMNS WHERE TABNAME = '"
						+ GenerateExtract.removeQuote(tabName)
						+ "' "
						+ "AND TABSCHEMA = '"
						+ GenerateExtract.removeQuote(schemaName)
						+ "' "
						+ "AND COLNAME = '"
						+ GenerateExtract.removeQuote(colName) + "'";

				PreparedStatement defStatement = this.bladeConn
						.prepareStatement(sql);
				rs2 = defStatement.executeQuery();
				if (GenerateExtract.debug)
					GenerateExtract.log("genDB2DefaultValues=" + sql);
				while (rs2.next()) {
					String def = rs2.getString(1);
					defValue = " WITH DEFAULT " + def + " ";
				}
				if (rs2 != null)
					rs2.close();
				if (defStatement != null)
					defStatement.close();
			} else if (GenerateExtract.dbSourceName.equalsIgnoreCase("idb2")) {
				String sql = "SELECT COLUMN_DEF FROM SYSIBM.SQLCOLUMNS WHERE TABLE_NAME = '"
						+ GenerateExtract.removeQuote(tabName)
						+ "' "
						+ "AND TABLE_SCHEM = '"
						+ GenerateExtract.removeQuote(schemaName)
						+ "' "
						+ "AND COLUMN_NAME = '"
						+ GenerateExtract.removeQuote(colName) + "'";

				PreparedStatement defStatement = this.bladeConn
						.prepareStatement(sql);
				rs2 = defStatement.executeQuery();
				if (GenerateExtract.debug)
					GenerateExtract.log("genDB2DefaultValues=" + sql);
				while (rs2.next()) {
					String def = rs2.getString(1);
					defValue = " WITH DEFAULT " + def + " ";
				}
				if (rs2 != null)
					rs2.close();
				if (defStatement != null)
					defStatement.close();
			} else if ((GenerateExtract.dbSourceName.equalsIgnoreCase("mssql"))
					|| (GenerateExtract.dbSourceName.equalsIgnoreCase("mysql"))) {
				rs2 = dbmeta.getColumns(null, GenerateExtract
						.removeQuote(schemaName), GenerateExtract
						.removeQuote(tabName), GenerateExtract
						.removeQuote(colName));
				while (rs2.next()) {
					defValue = rs2.getString("COLUMN_DEF");
				}
				if (rs2 != null)
					rs2.close();
				if ((defValue != null) && (!defValue.equals(""))) {
					defValue = defValue.replace('(', ' ');
					defValue = defValue.replace(')', ' ');
					if (defValue.trim().equalsIgnoreCase("getdate")) {
						defValue = GenerateExtract.dbTargetName.equals("zdb2") ? ""
								: " CURRENT TIMESTAMP ";
					} else if (defValue.trim().equalsIgnoreCase("user_name")) {
						defValue = GenerateExtract.dbTargetName.equals("zdb2") ? " CURRENT SQLID "
								: " CURRENT USER ";
					}

					if (dataType.equalsIgnoreCase("TIMESTAMP"))
						defValue = " WITH DEFAULT "
								+ formatsqlServerDate(defValue);
					else
						defValue = " WITH DEFAULT " + defValue;
					if (defValue.trim().endsWith("newid")) {
						if (!GenerateExtract.udfcreated) {
							genDB2UDFs();
						}
						String tmpStr = GenerateExtract.removeQuote(tabName
								.toUpperCase());
						tmpStr = tmpStr.replace("-", "_");
						String trigName = "TRIG" + GenerateExtract.triggerCount
								+ "_" + tmpStr;
						trigName = GenerateExtract.retainColName ? GenerateExtract
								.getTruncName(0, "", trigName, 128)
								: GenerateExtract.getTruncName(0, "", trigName,
										18);
						trigName = schemaName.toUpperCase() + ".\"" + trigName
								+ "\"";
						GenerateExtract.db2DropWriter.write("DROP TRIGGER "
								+ GenerateExtract.putQuote(dstSchema) + "."
								+ GenerateExtract.putQuote(trigName) + ";"
								+ GenerateExtract.linesep);
						GenerateExtract.db2DefaultWriter
								.write("--#SET :TRIGGER:" + dstSchema + ":"
										+ trigName + GenerateExtract.linesep);
						GenerateExtract.db2DefaultWriter
								.write("CREATE TRIGGER "
										+ GenerateExtract.putQuote(dstSchema)
										+ "."
										+ GenerateExtract.putQuote(trigName)
										+ " NO CASCADE BEFORE INSERT "
										+ GenerateExtract.linesep + "ON "
										+ schemaName.toUpperCase() + "."
										+ tabName.toUpperCase()
										+ " REFERENCING NEW AS NEW "
										+ GenerateExtract.linesep
										+ "FOR EACH ROW MODE DB2SQL "
										+ GenerateExtract.linesep);

						if (GenerateExtract.dbTargetName.equals("db2luw"))
							GenerateExtract.db2DefaultWriter.write("SET "
									+ colName + " = DB2.NEWGUID()"
									+ GenerateExtract.linesep);
						else
							GenerateExtract.db2DefaultWriter
									.write("VALUES DB2.NEWGUID() INTO "
											+ colName + GenerateExtract.linesep);
						GenerateExtract.db2DefaultWriter.write(";"
								+ GenerateExtract.linesep
								+ GenerateExtract.linesep);
						defValue = " WITH DEFAULT ";
					}
				}
			}
			if ((defValue != null) && (!defValue.equals(""))) {
				colName = GenerateExtract.retainColName ? GenerateExtract
						.getTruncName(0, "", colName, 128) : GenerateExtract
						.getTruncName(0, "", colName, 30);
				GenerateExtract.db2DefaultWriter.write("--#SET :DEFAULT:"
						+ GenerateExtract.removeQuote(schemaName.toUpperCase())
						+ ":"
						+ GenerateExtract.removeQuote(tabName.toUpperCase())
						+ "_"
						+ GenerateExtract.removeQuote(colName.toUpperCase())
						+ GenerateExtract.linesep);

				GenerateExtract.db2DefaultWriter.write("ALTER TABLE "
						+ GenerateExtract.putQuote(dstSchema) + "."
						+ GenerateExtract.putQuote(tabName.toUpperCase())
						+ " ALTER COLUMN \"" + colName + "\" SET " + defValue
						+ GenerateExtract.linesep + ";"
						+ GenerateExtract.linesep);

				GenerateExtract.db2DefaultWriter.write(GenerateExtract.linesep);
			}
		}

		private int ModifyTable(String str) {
			String regex = "CHAR\\((\\d.*)\\)|VARCHAR\\((\\d.*)\\)|NUMERIC\\((\\d.*)\\)|BLOB\\((\\d.*)\\)|CLOB\\((\\d.*)\\)|CLOB|BLOB|INT|INTEGER|FLOAT|SMALLINT|BIGINT|LONG VARCHAR\\((\\d.*)\\)|LONG VARGRAPHIC\\((\\d.*)\\)|DOUBLE|TIMESTAMP|DATE|TIME|NUMERIC\\((\\d.*,\\d.*)\\)|GRAPHIC\\((\\d.*)\\)|VARGRAPHIC\\((\\d.*)\\)";

			String regex2 = ".*\\(([0-9]*)\\)|.*\\(([0-9]*,[0-9]*)\\)";

			int p = 0;
			Matcher matcher = null;
			Matcher matcher2 = null;
			Pattern pattern = Pattern.compile(regex, 10);
			matcher = pattern.matcher(str);
			int tableSize = 0;
			while (matcher.find()) {
				String tok1 = matcher.group().toLowerCase();
				Pattern pattern2 = Pattern.compile(regex2);
				matcher2 = pattern2.matcher(tok1);
				p = 0;
				if (matcher2.find()) {
					if (matcher2.group(1) != null) {
						String[] tok2 = matcher2.group(1).split(",");
						try {
							p = Integer.parseInt(tok2[0]);
						} catch (Exception e) {
							p = 5;
						}
						if (tok2.length > 1) {
						}
					} else if (matcher2.group(2) != null) {
						String[] tok2 = matcher2.group(2).split(",");
						try {
							p = Integer.parseInt(tok2[0]);
						} catch (Exception e) {
							p = 5;
						}
						if (tok2.length > 1) {
						}
					}
				}
				if ((tok1.startsWith("int")) || (tok1.startsWith("integer"))) {
					tableSize += 4;
					continue;
				}
				if (tok1.startsWith("smallint")) {
					tableSize += 2;
					continue;
				}
				if (tok1.startsWith("bigint")) {
					tableSize += 8;
					continue;
				}
				if (tok1.startsWith("real")) {
					tableSize += 4;
					continue;
				}
				if (tok1.startsWith("double")) {
					tableSize += 8;
					continue;
				}
				if (tok1.startsWith("char")) {
					tableSize += p;
					continue;
				}
				if (tok1.startsWith("varchar")) {
					tableSize += p;
					continue;
				}
				if (tok1.startsWith("graphic")) {
					tableSize += p * 2;
					continue;
				}
				if (tok1.startsWith("vargraphic")) {
					tableSize += p * 2;
					continue;
				}
				if (tok1.startsWith("date")) {
					tableSize += 4;
					continue;
				}
				if (tok1.startsWith("time")) {
					tableSize += 3;
					continue;
				}
				if (tok1.startsWith("xml")) {
					tableSize += 84;
					continue;
				}
				if (tok1.startsWith("long varchar")) {
					tableSize += 24;
					continue;
				}
				if (tok1.startsWith("long vargraphic")) {
					tableSize += 24;
					continue;
				}
				if ((tok1.startsWith("blob")) || (tok1.startsWith("clob"))) {
					if (p == 0)
						p = 1048576;
					if ((p > 0) && (p <= 1024)) {
						tableSize += 72;
						continue;
					}
					if ((p > 1024) && (p <= 8192)) {
						tableSize += 96;
						continue;
					}
					if ((p > 8192) && (p <= 65536)) {
						tableSize += 120;
						continue;
					}
					if ((p > 65536) && (p <= 524000)) {
						tableSize += 144;
						continue;
					}
					if ((p > 524000) && (p <= 4190000)) {
						tableSize += 168;
						continue;
					}
					if ((p > 4190000) && (p <= 134000000)) {
						tableSize += 200;
						continue;
					}
					if ((p > 134000000) && (p <= 536000000)) {
						tableSize += 224;
						continue;
					}
					if ((p > 536000000) && (p <= 1470000000)) {
						tableSize += 280;
						continue;
					}
					if ((p > 1470000000) && (p <= 2147483647)) {
						tableSize += 316;
						continue;
					}
				}
				if (!tok1.startsWith("numeric"))
					continue;
				tableSize += p / 2 + 1;
			}

			return tableSize;
		}

		public String ModifyTableAll(String str, String schemaName,
				String tabName) throws IOException {
			String regex = "VARCHAR\\((\\d.*)\\)|VARGRAPHIC\\((\\d.*)\\)|LONG VARCHAR\\((\\d.*)\\)|LONG VARGRAPHIC\\((\\d.*)\\)";
			String regex2 = ".*\\((\\d.*)\\)";

			int oldTableSize = 0;
			str = str.replaceAll("VARCHAR\\(1\\)", "CHAR(1)");
			StringBuffer sb = new StringBuffer();
			boolean found = false;
			sb.setLength(0);
			sb.append(str);

			Matcher matcher = null;
			Matcher matcher2 = null;
			int tableSize;
			while ((tableSize = ModifyTable(sb.toString())) > 30000) {
				if (oldTableSize == 0)
					oldTableSize = tableSize;
				Pattern pattern = Pattern.compile(regex, 10);
				matcher = pattern.matcher(sb.toString());
				found = false;
				while (matcher.find()) {
					String tok1 = matcher.group().toLowerCase();
					Pattern pattern2 = Pattern.compile(regex2);
					matcher2 = pattern2.matcher(tok1);
					int p = 0;
					if (matcher2.find()) {
						p = Integer.parseInt(matcher2.group(1));
					}

					if (matcher.group().toLowerCase().startsWith("varchar")) {
						if (p > 1000) {
							sb.replace(matcher.start(), matcher.end(),
									"LONG VARCHAR");
							found = true;
							break;
						}
					}
					if ((!matcher.group().toLowerCase()
							.startsWith("vargraphic"))
							|| (p <= 500)) {
						continue;
					}
					sb.replace(matcher.start(), matcher.end(),
							"LONG VARGRAPHIC");
					found = true;
				}

				if (!found)
					break;
			}
			if (oldTableSize > 0) {
				sb.insert(0, "-- The original approx table size was "
						+ oldTableSize + GenerateExtract.linesep);
				sb.insert(0, "-- or to LONG VARGRAPHIC column"
						+ GenerateExtract.linesep);
				sb
						.insert(
								0,
								"-- Some of the VARCHAR or VARGRAPHIC columns have been converted to LONG VARCHAR"
										+ GenerateExtract.linesep);
			}
			if ((GenerateExtract.dbSourceName.equalsIgnoreCase("oracle"))
					&& (GenerateExtract.db2_compatibility))
				sb.insert(0,
						"-- Estimated size of the table is wrong. I need to fix this."
								+ GenerateExtract.linesep);
			sb.insert(0, "-- Approximate Table Size " + tableSize
					+ GenerateExtract.linesep);
			return sb.toString();
		}

		private void checkUniqueConstraint(int id) throws SQLException,
				IOException {
			int position = 1;
			String uniqSQL = "";
			String column = "";
			String schema = GenerateExtract
					.removeQuote(GenerateExtract.schemaName[id].toUpperCase());
			String table = GenerateExtract
					.removeQuote(GenerateExtract.srcTableName[id].toUpperCase());
			PreparedStatement queryStatement = null;
			ResultSet Reader = null;
			StringBuffer buffer = new StringBuffer();

			if (GenerateExtract.dbSourceName.equalsIgnoreCase("oracle")) {
				uniqSQL = "SELECT COL.COLUMN_NAME, COL.POSITION, CON.CONSTRAINT_NAME FROM DBA_CONS_COLUMNS COL, DBA_CONSTRAINTS CON WHERE COL.OWNER = '"
						+ schema
						+ "' AND COL.TABLE_NAME = '"
						+ table
						+ "' AND CONSTRAINT_TYPE <> 'R' AND COL.OWNER = CON.OWNER AND COL.TABLE_NAME = CON.TABLE_NAME "
						+ "AND COL.CONSTRAINT_NAME = CON.CONSTRAINT_NAME AND CON.CONSTRAINT_TYPE = 'U' ORDER BY COL.CONSTRAINT_NAME, COL.POSITION";

				uniqSQL = "";
			}

			if (uniqSQL.equals(""))
				return;

			if (GenerateExtract.debug)
				GenerateExtract.log("checkUniqueConstraint=" + uniqSQL);
			queryStatement = this.bladeConn.prepareStatement(uniqSQL);
			queryStatement.setFetchSize(GenerateExtract.fetchSize);
			Reader = queryStatement.executeQuery();
			buffer.setLength(0);
			while (Reader.next()) {
				column = Reader.getString(1);
				position = Reader.getInt(2);
				String consName = Reader.getString(3);
				if (position == 1) {
					if (buffer.length() > 0) {
						buffer.append(GenerateExtract.linesep);
						buffer.append(")" + GenerateExtract.linesep + ";"
								+ GenerateExtract.linesep);
						buffer.append(GenerateExtract.linesep);
						GenerateExtract.db2UniqWriter.write(buffer.toString());
					}
					buffer.setLength(0);
					String newConsName = GenerateExtract.getTruncName(0, "",
							"UK" + GenerateExtract.numUniq + "_" + table, 128);
					if (GenerateExtract.retainConstraintsName) {
						if ((consName != null) && (consName.length() != 0))
							newConsName = consName;
					}
					buffer.append("--#SET :UNIQUE_INDEX:"
							+ GenerateExtract
									.removeQuote(GenerateExtract.schemaName[id]
											.toUpperCase()) + ":" + newConsName
							+ GenerateExtract.linesep);
					buffer.append("ALTER TABLE "
							+ GenerateExtract.schemaName[id].toUpperCase()
							+ "." + GenerateExtract.tableName[id].toUpperCase()
							+ " ADD CONSTRAINT " + newConsName + " UNIQUE "
							+ GenerateExtract.linesep);

					buffer.append("(" + GenerateExtract.linesep);
					buffer.append(column);
					continue;
				}
				buffer.append("," + GenerateExtract.linesep);
				buffer.append(column);
			}

			if (buffer.length() > 0) {
				buffer.append(GenerateExtract.linesep);
				buffer.append(")" + GenerateExtract.linesep + ";"
						+ GenerateExtract.linesep);
				buffer.append(GenerateExtract.linesep);
				GenerateExtract.db2UniqWriter.write(buffer.toString());
			}
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		}

		private void genDB2CheckConstraints(int id) throws SQLException,
				IOException {
			boolean found = false;
			int numCheck = 1;
			String checkSQL = "";
			String checkCondition = "";
			String schema = GenerateExtract
					.removeQuote(GenerateExtract.srcSchName[id].toUpperCase());
			String table = GenerateExtract
					.removeQuote(GenerateExtract.srcTableName[id].toUpperCase());

			ResultSet Reader = null;
			StringBuffer buffer = new StringBuffer();

			if (GenerateExtract.dbSourceName.equalsIgnoreCase("oracle")) {
				checkSQL = "SELECT SEARCH_CONDITION, CONSTRAINT_NAME FROM DBA_CONSTRAINTS WHERE OWNER = '"
						+ schema
						+ "' "
						+ "AND TABLE_NAME = '"
						+ table
						+ "' "
						+ "AND CONSTRAINT_TYPE = 'C'";
			} else if (GenerateExtract.dbSourceName.equalsIgnoreCase("mssql")) {
				checkSQL = "SELECT CHECK_CLAUSE, A.CONSTRAINT_NAME FROM   INFORMATION_SCHEMA.CHECK_CONSTRAINTS A,        INFORMATION_SCHEMA.TABLE_CONSTRAINTS B WHERE  A.CONSTRAINT_NAME = B.CONSTRAINT_NAME AND    B.TABLE_NAME = '"
						+ table
						+ "' "
						+ "AND    TABLE_SCHEMA = '"
						+ schema
						+ "' ";
			} else if (GenerateExtract.dbSourceName.equalsIgnoreCase("idb2")) {
				checkSQL = "SELECT A.CHECK_CLAUSE, A.CONSTRAINT_NAME FROM   SYSIBM.CHECK_CONSTRAINTS A,        SYSIBM.TABLE_CONSTRAINTS B WHERE  A.CONSTRAINT_SCHEMA = B.CONSTRAINT_SCHEMA AND    A.CONSTRAINT_NAME = B.CONSTRAINT_NAME AND    B.TABLE_NAME = '"
						+ table
						+ "' "
						+ "AND    B.TABLE_SCHEMA = '"
						+ schema
						+ "' " + "AND    B.CONSTRAINT_TYPE = 'CHECK'";
			} else if (GenerateExtract.dbSourceName.equalsIgnoreCase("db2")) {
				checkSQL = "SELECT TEXT, CONSTNAME FROM   SYSCAT.CHECKS WHERE  TYPE = 'C' AND    TABNAME = '"
						+ table + "' " + "AND    OWNER = '" + schema + "' ";
			}

			if (checkSQL.equals(""))
				return;

			if (GenerateExtract.debug)
				GenerateExtract.log("genDB2CheckConstraints=" + checkSQL);
			PreparedStatement queryStatement = this.bladeConn
					.prepareStatement(checkSQL);
			queryStatement.setFetchSize(GenerateExtract.fetchSize);
			Reader = queryStatement.executeQuery();
			while (Reader.next()) {
				checkCondition = Reader.getString(1);
				String consName = Reader.getString(2);
				int jk = checkCondition.indexOf("NOT NULL");
				if (jk <= 0) {
					buffer.setLength(0);
					String tmp = table.replace("-", "_");
					String newConsName = GenerateExtract.getTruncName(0, "",
							"CK" + numCheck + "_" + tmp, 128);
					if (GenerateExtract.retainConstraintsName) {
						if ((consName != null) && (consName.length() != 0))
							newConsName = consName;
					}
					buffer.append("--#SET :CHECK_CONSTRAINTS:"
							+ GenerateExtract
									.removeQuote(GenerateExtract.schemaName[id]
											.toUpperCase()) + ":" + newConsName
							+ GenerateExtract.linesep);
					buffer.append("ALTER TABLE "
							+ GenerateExtract.schemaName[id].toUpperCase()
							+ "." + GenerateExtract.tableName[id].toUpperCase()
							+ GenerateExtract.linesep);
					if (GenerateExtract.dbSourceName.equalsIgnoreCase("mssql")) {
						int pos1 = checkCondition.indexOf('[');
						int pos2 = checkCondition.indexOf(']');
						checkCondition = checkCondition.replace('[', '"');
						checkCondition = checkCondition.replace(']', '"');
						if ((pos1 > 0) && (pos2 > 0)) {
							checkCondition = checkCondition.substring(0, pos1)
									+ checkCondition.substring(pos1, pos2)
											.toUpperCase()
									+ checkCondition.substring(pos2);
						}

					}

					buffer.append("ADD CONSTRAINT " + newConsName + " CHECK ("
							+ checkCondition.trim() + ")"
							+ GenerateExtract.linesep + ";"
							+ GenerateExtract.linesep);
					numCheck++;
					GenerateExtract.db2CheckWriter.write(buffer.toString());
					GenerateExtract.db2CheckWriter
							.write(GenerateExtract.linesep);
				}

			}

			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		}

		private void getIdentityAttributes(String table, String column)
				throws SQLException {
			String identitySQL = "";

			ResultSet Reader = null;
			PreparedStatement queryStatement;

			if (GenerateExtract.dbSourceName.equalsIgnoreCase("mssql")) {
				identitySQL = "SELECT CONVERT(BIGINT,INCREMENT_VALUE) INCREMENT_VALUE, CONVERT(BIGINT,LAST_VALUE) LAST_VALUE FROM   SYS.IDENTITY_COLUMNS WHERE  OBJECT_NAME(OBJECT_ID) = '"
						+ GenerateExtract.removeQuote(table)
						+ "' "
						+ "AND    NAME = '" + column + "' ";
			}

			if (identitySQL.equals("")) {
				this.increment_value = 1L;
				this.last_value = -1L;
				return;
			}
			if (GenerateExtract.debug)
				GenerateExtract.log("getIdentityAttributes=" + identitySQL);
			try {
				queryStatement = this.bladeConn.prepareStatement(identitySQL);
				Reader = queryStatement.executeQuery();
				while (Reader.next()) {
					this.increment_value = Reader.getLong(1);
					this.last_value = Reader.getLong(2);
				}
				if (Reader != null)
					Reader.close();
				if (queryStatement != null)
					queryStatement.close();
			} catch (SQLException e) {
				identitySQL = "SELECT CONVERT(BIGINT,IDENT_INCR(TABLE_NAME)) AS INCREMENT_VALUE, CONVERT(BIGINT,IDENT_CURRENT(TABLE_NAME)) AS LAST_VALUE FROM INFORMATION_SCHEMA.TABLES WHERE OBJECTPROPERTY(OBJECT_ID(TABLE_NAME), 'TableHasIdentity') = 1 AND TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = '"
						+ GenerateExtract.removeQuote(table) + "' ";

				if (GenerateExtract.debug)
					GenerateExtract.log("getIdentityAttributes=" + identitySQL);
				try {
					queryStatement = this.bladeConn
							.prepareStatement(identitySQL);
					Reader = queryStatement.executeQuery();
					while (Reader.next()) {
						this.increment_value = Reader.getLong(1);
						this.last_value = Reader.getLong(2);
					}
					if (Reader != null)
						Reader.close();
					if (queryStatement != null)
						queryStatement.close();
				} catch (SQLException e1) {
					throw e1;
				}
			}
		}

		private String formatDate(String strDate) {
			String sql = "";
			if (strDate.toLowerCase().startsWith("to_date")) {
				sql = "SELECT TO_CHAR(" + strDate
						+ ",'YYYY-MM-DD-HH24.MI.SS\".00000\"') FROM DUAL";
			} else {
				sql = "SELECT TO_CHAR(TO_DATE(" + strDate
						+ "),'YYYY-MM-DD-HH24.MI.SS\".00000\"') FROM DUAL";
			}

			PreparedStatement countStatement = null;
			try {
				countStatement = this.bladeConn.prepareStatement(sql);
				ResultSet rs = countStatement.executeQuery();
				if (rs.next()) {
					String retDate = rs.getString(1);
					if (rs != null)
						rs.close();
					if (countStatement != null)
						countStatement.close();
					return retDate;
				}
				if (rs != null)
					rs.close();
				if (countStatement != null)
					countStatement.close();
			} catch (SQLException e) {
				return "";
			}
			return "";
		}

		private String OracleTempOnCommitIndicator(String schemaName,
				String tabName) {
			String commitTok = "ON COMMIT PRESERVE ROWS";
			String sql = "select decode(duration, 'SYS$SESSION','ON COMMIT PRESERVE ROWS','SYS$TRANSACTION','ON COMMIT DELETE ROWS', NULL) COMMIT_TOKEN from dba_tables WHERE OWNER = '"
					+ GenerateExtract.removeQuote(schemaName)
					+ "' "
					+ "AND TABLE_NAME = '"
					+ GenerateExtract.removeQuote(tabName)
					+ "' "
					+ "AND TEMPORARY = 'Y'";
			try {
				PreparedStatement pstat = this.bladeConn.prepareStatement(sql);
				ResultSet rs1 = pstat.executeQuery();
				if (rs1.next()) {
					commitTok = rs1.getString(1);
				}
				if (rs1 != null)
					rs1.close();
				if (pstat != null)
					pstat.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			return commitTok;
		}

		private String isOracleTabTemporary(String schemaName, String tabName)
				throws SQLException {
			String isTemp = "";
			String sql = "SELECT 'X' FROM ALL_OBJECTS WHERE OWNER = '"
					+ GenerateExtract.removeQuote(schemaName) + "' "
					+ "AND OBJECT_NAME = '"
					+ GenerateExtract.removeQuote(tabName) + "' "
					+ "AND TEMPORARY = 'Y'";

			PreparedStatement pstat = this.bladeConn.prepareStatement(sql);
			ResultSet rs1 = pstat.executeQuery();
			if (rs1.next()) {
				isTemp = " GLOBAL TEMPORARY ";
			}
			if (rs1 != null)
				rs1.close();
			if (pstat != null)
				pstat.close();
			return isTemp;
		}

		private String getOraTableSpaces(String schemaName, String tableName) {
			String dataTS = "";
			String idxTS = "";
			String longTS = "";
			PreparedStatement statement = null;
			ResultSet rs1 = null;

			String sql1 = "SELECT DECODE(TABLESPACE_NAME,'SYSTEM','DB2SYSTEM',TABLESPACE_NAME) FROM DBA_TABLES WHERE OWNER = '"
					+ GenerateExtract.removeQuote(schemaName)
					+ "' "
					+ "AND TABLE_NAME = '"
					+ GenerateExtract.removeQuote(tableName) + "' ";

			String sql2 = "SELECT DECODE(TABLESPACE_NAME,'SYSTEM','DB2SYSTEM',TABLESPACE_NAME) FROM DBA_INDEXES WHERE OWNER = '"
					+ GenerateExtract.removeQuote(schemaName)
					+ "' "
					+ "AND TABLE_NAME = '"
					+ GenerateExtract.removeQuote(tableName) + "' ";

			String sql3 = "SELECT DECODE(TABLESPACE_NAME,'SYSTEM','DB2SYSTEM',TABLESPACE_NAME) FROM DBA_LOBS WHERE OWNER = '"
					+ GenerateExtract.removeQuote(schemaName)
					+ "' "
					+ "AND TABLE_NAME = '"
					+ GenerateExtract.removeQuote(tableName) + "' ";
			try {
				statement = this.bladeConn.prepareStatement(sql1);
				rs1 = statement.executeQuery();
				if (rs1.next()) {
					dataTS = rs1.getString(1);
					dataTS = "IN " + dataTS + " ";
				}
				if (rs1 != null)
					rs1.close();
				if (statement != null)
					statement.close();
			} catch (SQLException e) {
				dataTS = "";
				e.printStackTrace();
			}

			try {
				statement = this.bladeConn.prepareStatement(sql2);
				rs1 = statement.executeQuery();
				if (rs1.next()) {
					idxTS = rs1.getString(1);
					idxTS = "INDEX IN " + idxTS + " ";
				}
				if (rs1 != null)
					rs1.close();
				if (statement != null)
					statement.close();
			} catch (SQLException e) {
				idxTS = "";
				e.printStackTrace();
			}

			try {
				statement = this.bladeConn.prepareStatement(sql3);
				rs1 = statement.executeQuery();
				if (rs1.next()) {
					longTS = rs1.getString(1);
					longTS = "LONG IN " + longTS + " ";
				}
				if (rs1 != null)
					rs1.close();
				if (statement != null)
					statement.close();
			} catch (SQLException e) {
				longTS = "";
				e.printStackTrace();
			}
			return dataTS + idxTS + longTS;
		}

		private String getOracleDataType(String schemaName, String tabName,
				String colName) throws SQLException {
			String data_type = "";
			String sql1 = "SELECT DATA_TYPE FROM   ALL_TAB_COLUMNS WHERE  OWNER = '"
					+ GenerateExtract.removeQuote(schemaName)
					+ "' "
					+ "AND    TABLE_NAME = '"
					+ GenerateExtract.removeQuote(tabName)
					+ "' "
					+ "AND    COLUMN_NAME = '" + colName + "' ";

			PreparedStatement partStatement1 = this.bladeConn
					.prepareStatement(sql1);
			ResultSet rs1 = partStatement1.executeQuery();
			if (GenerateExtract.debug)
				GenerateExtract.log("getOracleDataType=" + sql1);
			if (rs1.next()) {
				data_type = rs1.getString(1);
			}
			if (rs1 != null)
				rs1.close();
			if (partStatement1 != null)
				partStatement1.close();
			return data_type;
		}

		private String getInitialValue(int numCols, String type) {
			String modifier = "";
			for (int i = 0; i < numCols; i++) {
				modifier = modifier
						+ (i == 0 ? type : new StringBuilder().append(",")
								.append(type).toString());
			}
			return modifier;
		}

		private String getOraPartitionColumns(String schemaName, String tabName) {
			int i = 0;
			String columnName = "";
			String sql1 = "SELECT C.COLUMN_NAME FROM   ALL_PART_KEY_COLUMNS C WHERE  C.OWNER = '"
					+ GenerateExtract.removeQuote(schemaName)
					+ "' "
					+ "AND    C.NAME = '"
					+ GenerateExtract.removeQuote(tabName)
					+ "' "
					+ "AND    C.OBJECT_TYPE = 'TABLE'";
			try {
				PreparedStatement partStatement1 = this.bladeConn
						.prepareStatement(sql1);
				ResultSet rs1 = partStatement1.executeQuery();
				while (rs1.next()) {
					columnName = columnName
							+ (i++ > 0 ? ","
									+ GenerateExtract
											.putQuote(rs1.getString(1))
									: GenerateExtract
											.putQuote(rs1.getString(1)));
				}
				if (rs1 != null)
					rs1.close();
				if (partStatement1 != null)
					partStatement1.close();
			} catch (Exception e) {
				columnName = "Column Name Error";
				e.printStackTrace();
			}
			return columnName;
		}

		private String OraPartitionType(String schemaName, String tabName) {
			String partType = "";
			String sql = "SELECT PARTITIONING_TYPE FROM DBA_PART_TABLES WHERE OWNER = '"
					+ GenerateExtract.removeQuote(schemaName)
					+ "' "
					+ "AND TABLE_NAME = '"
					+ GenerateExtract.removeQuote(tabName) + "'";
			try {
				PreparedStatement partStatement1 = this.bladeConn
						.prepareStatement(sql);
				ResultSet rs1 = partStatement1.executeQuery();
				if (rs1.next()) {
					partType = rs1.getString(1);
				}
				if (rs1 != null)
					rs1.close();
				if (partStatement1 != null)
					partStatement1.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			return partType;
		}

		private String genOraPartitions(String schemaName, String tabName)
				throws SQLException {
			String part_data = "";
			String partType = OraPartitionType(schemaName, tabName);

			StringBuffer buffer = new StringBuffer();

			String sql1 = "SELECT C.COLUMN_NAME FROM   ALL_PART_KEY_COLUMNS C WHERE  C.OWNER = '"
					+ GenerateExtract.removeQuote(schemaName)
					+ "' "
					+ "AND    C.NAME = '"
					+ GenerateExtract.removeQuote(tabName)
					+ "' "
					+ "AND    C.OBJECT_TYPE = 'TABLE'";

			if ((partType == null) || (partType.length() == 0))
				return "";
			if ((partType.equalsIgnoreCase("RANGE"))
					&& (!GenerateExtract.extractPartitions))
				return "-- Range Partitions are supported in DB2 but you chose not to extract them"
						+ GenerateExtract.linesep;
			if ((partType.equalsIgnoreCase("HASH"))
					&& (!GenerateExtract.extractHashPartitions))
				return "-- Hash Partitions are supported in DB2 but you chose not to extract them"
						+ GenerateExtract.linesep;
			if (partType.equalsIgnoreCase("LIST"))
				return "-- List Partitions are not supported in DB2"
						+ GenerateExtract.linesep;
			String partColNames = getOraPartitionColumns(schemaName, tabName);
			int colNums = partColNames.split(",").length;
			PreparedStatement partStatement1 = this.bladeConn
					.prepareStatement(sql1);
			ResultSet rs1 = partStatement1.executeQuery();
			if (GenerateExtract.debug)
				GenerateExtract.log("genOraPartitions=" + sql1);
			int i = 0;
			if (rs1.next()) {
				String column_name = rs1.getString(1);
				String sql2 = "SELECT TC.DATA_TYPE FROM   ALL_TAB_COLUMNS TC WHERE  TC.OWNER = '"
						+ GenerateExtract.removeQuote(schemaName)
						+ "' "
						+ "AND    TC.TABLE_NAME = '"
						+ GenerateExtract.removeQuote(tabName)
						+ "' "
						+ "AND    TC.COLUMN_NAME = '"
						+ GenerateExtract.removeQuote(column_name) + "' ";

				PreparedStatement partStatement2 = this.bladeConn
						.prepareStatement(sql2);
				ResultSet rs2 = partStatement2.executeQuery();
				String data_type = "";
				if (GenerateExtract.debug)
					GenerateExtract.log("genOraPartitions=" + sql2);
				if (rs2.next()) {
					data_type = rs2.getString(1);
				}
				if (rs2 != null)
					rs2.close();
				if (partStatement2 != null)
					partStatement2.close();
				if ((data_type.equals("DATE"))
						|| (data_type.equals("VARCHAR2"))
						|| (data_type.equals("NUMBER"))) {
					String sql3 = "SELECT T.PARTITION_NAME, T.HIGH_VALUE, T.HIGH_VALUE_LENGTH, TABLESPACE_NAME FROM   ALL_TAB_PARTITIONS T WHERE  T.TABLE_OWNER = '"
							+ GenerateExtract.removeQuote(schemaName)
							+ "' "
							+ "AND    T.TABLE_NAME = '"
							+ GenerateExtract.removeQuote(tabName)
							+ "' "
							+ "ORDER BY T.PARTITION_POSITION";

					PreparedStatement partStatement3 = this.bladeConn
							.prepareStatement(sql3);
					ResultSet rs3 = partStatement3.executeQuery();
					if (GenerateExtract.debug)
						GenerateExtract.log("genOraPartitions=" + sql3);
					while (rs3.next()) {
						String part_name = rs3.getString(1);
						String part_value = rs3.getString(2);
						if (data_type.equals("DATE"))
							part_data = "'" + formatDate(part_value) + "'";
						else if (data_type.equals("VARCHAR2"))
							part_data = part_value;
						else if (data_type.equals("NUMBER"))
							part_data = part_value;
						if (i > 0) {
							buffer.append(",");
						} else {
							if (partType.equalsIgnoreCase("RANGE")) {
								buffer.append("PARTITION BY RANGE ("
										+ partColNames + ")"
										+ GenerateExtract.linesep);
								buffer.append("(" + GenerateExtract.linesep);
								buffer.append("PARTITION " + part_name
										+ " STARTING ("
										+ getInitialValue(colNums, "MINVALUE")
										+ ") INCLUSIVE ENDING (" + part_data
										+ ") EXCLUSIVE"
										+ GenerateExtract.linesep);
							} else if (partType.equalsIgnoreCase("HASH")) {
								buffer.append("DISTRIBUTE BY HASH ("
										+ partColNames + ")"
										+ GenerateExtract.linesep);
							}
							i++;
							continue;
						}
						if (partType.equalsIgnoreCase("RANGE")) {
							if (part_value.toUpperCase().startsWith("MAXVALUE")) {
								buffer.append("PARTITION " + part_name
										+ " ENDING ("
										+ getInitialValue(colNums, "MAXVALUE")
										+ ") INCLUSIVE"
										+ GenerateExtract.linesep);
							} else {
								buffer.append("PARTITION " + part_name
										+ " ENDING (" + part_data
										+ ") EXCLUSIVE"
										+ GenerateExtract.linesep);
							}
						}
						i++;
					}
					if (rs3 != null)
						rs3.close();
					if (partStatement3 != null)
						partStatement3.close();
				}
			}
			if (partType.equalsIgnoreCase("RANGE"))
				buffer.append(")" + GenerateExtract.linesep);
			if (rs1 != null)
				rs1.close();
			if (partStatement1 != null)
				partStatement1.close();
			if (i == 0) {
				return "";
			}

			buffer.append("-- Please check the converted DDL for partitions"
					+ GenerateExtract.linesep);
			return buffer.toString();
		}

		private String genzDB2Partitions(String schemaName, String tabName,
				String partKey) throws SQLException {
			StringBuffer buffer = new StringBuffer();
			String sql = "SELECT P.PARTITION, P.LIMITKEY FROM SYSIBM.SYSTABLEPART P, SYSIBM.SYSTABLES T WHERE P.TSNAME = T.TSNAME AND T.CREATOR = '"
					+ GenerateExtract.removeQuote(schemaName)
					+ "' "
					+ "AND T.NAME = '"
					+ GenerateExtract.removeQuote(tabName)
					+ "' " + "ORDER BY P.PARTITION";

			PreparedStatement partStatement = this.bladeConn
					.prepareStatement(sql);
			ResultSet rs2 = partStatement.executeQuery();
			buffer.append("PARTITION BY(" + partKey + ")"
					+ GenerateExtract.linesep);
			buffer.append("(" + GenerateExtract.linesep);
			int i = 0;
			if (GenerateExtract.debug)
				GenerateExtract.log("genzDB2Partitions=" + sql);
			while (rs2.next()) {
				if (i > 0)
					buffer.append(",");
				buffer.append("PART " + rs2.getString(1) + " STARTING '"
						+ rs2.getString(2) + "'" + GenerateExtract.linesep);
				i++;
			}
			buffer.append(")" + GenerateExtract.linesep);
			if (rs2 != null)
				rs2.close();
			if (partStatement != null)
				partStatement.close();
			if (i == 0) {
				return "";
			}

			buffer.append("-- Please check the converted DDL for partitions"
					+ GenerateExtract.linesep);
			return buffer.toString();
		}

		private void genDB2TableScript(int[] dataLen,
				ResultSetMetaData metadata, int id, long numRows)
				throws Exception {
			int autoIncrCount = 0;

			String isGlobal = "";
			String commitToken = "";
			StringBuffer buffer = new StringBuffer();
			StringBuffer seqBuffer = new StringBuffer();
			GenerateExtract.DataMap map = new GenerateExtract.DataMap();

			int colCount = metadata.getColumnCount();

			GenerateExtract.db2DropWriter.write("DROP TABLE "
					+ GenerateExtract.schemaName[id].toUpperCase() + "."
					+ GenerateExtract.tableName[id].toUpperCase() + ";"
					+ GenerateExtract.linesep);
			buffer.setLength(0);
			if ((GenerateExtract.db2_compatibility)
					&& (GenerateExtract.dbSourceName.equalsIgnoreCase("oracle"))) {
				isGlobal = isOracleTabTemporary(GenerateExtract.srcSchName[id]
						.toUpperCase(), GenerateExtract.srcTableName[id]
						.toUpperCase());
				commitToken = OracleTempOnCommitIndicator(
						GenerateExtract.srcSchName[id].toUpperCase(),
						GenerateExtract.srcTableName[id].toUpperCase());
			}
			buffer.append("--#SET :TABLE:"
					+ GenerateExtract
							.removeQuote(GenerateExtract.schemaName[id]
									.toUpperCase())
					+ ":"
					+ GenerateExtract.removeQuote(GenerateExtract.tableName[id]
							.toUpperCase()) + GenerateExtract.linesep);
			buffer.append("CREATE " + isGlobal + " TABLE "
					+ GenerateExtract.schemaName[id].toUpperCase() + "."
					+ GenerateExtract.tableName[id].toUpperCase()
					+ GenerateExtract.linesep);
			buffer.append("(" + GenerateExtract.linesep);
			for (int colIndex = 1; colIndex <= colCount; colIndex++) {
				int ij = colIndex - 1;
				String colType;
				if (GenerateExtract.dbSourceName.equalsIgnoreCase("oracle"))
					colType = getOracleDataType(GenerateExtract.srcSchName[id]
							.toUpperCase(), GenerateExtract.srcTableName[id]
							.toUpperCase(), metadata.getColumnName(colIndex));
				else
					colType = metadata.getColumnTypeName(colIndex)
							.toUpperCase();
				String colName;
				if (GenerateExtract.retainColName) {
					colName = GenerateExtract.getTruncName(id, "COL", metadata
							.getColumnName(colIndex), 128);
				} else {
					colName = GenerateExtract.getTruncName(id, "COL", metadata
							.getColumnName(colIndex), 30);
				}
				map.sourceDataType = colType;
				getDB2Type(map, id);
				if (GenerateExtract.debug)
					GenerateExtract
							.log("data type mapping: DB2=" + map.db2DataType
									+ " source=" + map.sourceDataType);
				String nullType = metadata.isNullable(colIndex) == 0 ? " NOT NULL"
						: "";
				int precision;
				try {
					precision = metadata.getPrecision(colIndex);
				} catch (Exception e) {
					precision = 2147483647;
					GenerateExtract
							.log("Error: Precision found more than 2GB limiting it to 2GB");
				}
				int colDisplayWidth = metadata.getColumnDisplaySize(colIndex);
				if ((precision == 0) && (colDisplayWidth > 0))
					precision = colDisplayWidth;
				int scale = metadata.getScale(colIndex);
				boolean isAutoIncr = metadata.isAutoIncrement(colIndex);
				String identity = "";
				if (isAutoIncr) {
					autoIncrCount++;
					getIdentityAttributes(GenerateExtract.srcTableName[id]
							.toUpperCase(), metadata.getColumnName(colIndex));
					this.last_value = (this.last_value == -1L ? (this.last_value = numRows + 1L)
							: this.last_value + this.increment_value);
					if (autoIncrCount == 1) {
						identity = " GENERATED BY DEFAULT AS IDENTITY (START WITH "
								+ this.last_value
								+ ", INCREMENT BY "
								+ this.increment_value + ", CACHE 20)";
					} else {
						identity = "";
						String tmpStr = GenerateExtract
								.removeQuote(GenerateExtract.tableName[id]
										.toUpperCase());
						tmpStr = tmpStr.replace("-", "_");
						String seqName = "SEQ" + (autoIncrCount - 1) + "_"
								+ tmpStr;
						String trigName = "TRIG" + (autoIncrCount - 1) + "_"
								+ tmpStr;
						trigName = GenerateExtract.retainColName ? GenerateExtract
								.getTruncName(0, "", trigName, 128)
								: GenerateExtract.getTruncName(0, "", trigName,
										18);
						trigName = GenerateExtract.schemaName[id].toUpperCase()
								+ ".\"" + trigName + "\"";
						seqName = GenerateExtract.retainColName ? GenerateExtract
								.getTruncName(0, "", seqName, 128)
								: GenerateExtract.getTruncName(0, "", seqName,
										18);
						seqName = GenerateExtract.schemaName[id].toUpperCase()
								+ ".\"" + seqName + "\"";
						GenerateExtract.db2DropWriter.write("DROP SEQUENCE "
								+ seqName + ";" + GenerateExtract.linesep);
						GenerateExtract.db2DropWriter.write("DROP TRIGGER "
								+ trigName + ";" + GenerateExtract.linesep);
						seqBuffer.append(GenerateExtract.linesep);
						seqBuffer.append("CREATE SEQUENCE " + seqName
								+ GenerateExtract.linesep);
						seqBuffer.append("START WITH " + this.last_value
								+ "  INCREMENT BY " + this.increment_value
								+ " CACHE 20;" + GenerateExtract.linesep);
						seqBuffer.append(GenerateExtract.linesep);
						seqBuffer.append("CREATE TRIGGER " + trigName
								+ GenerateExtract.linesep);
						seqBuffer.append("NO CASCADE BEFORE INSERT ON "
								+ GenerateExtract.schemaName[id].toUpperCase()
								+ "."
								+ GenerateExtract.tableName[id].toUpperCase()
								+ GenerateExtract.linesep);
						seqBuffer
								.append("REFERENCING NEW AS NEW FOR EACH ROW MODE DB2SQL"
										+ GenerateExtract.linesep);
						seqBuffer.append("SET NEW.\"" + colName
								+ "\" = NEXT VALUE FOR " + seqName + ";"
								+ GenerateExtract.linesep);
						seqBuffer.append(GenerateExtract.linesep);
					}
				}
				if (map.varlength) {
					if (map.useActualData) {
						int len = dataLen[ij];
						if (len == 0) {
							if ((colType.equalsIgnoreCase("text"))
									|| (colType.equalsIgnoreCase("bytea"))) {
								if (precision <= 0)
									len = 16777216;
								else
									len = precision;
							} else
								len = 255;
						} else {
							len = (int) (Math.ceil(len / 10.0D) * 10.0D);
						}
						if (len > 32000) {
							if (map.db2DataType.startsWith("BLOB"))
								map.db2DataType = ("BLOB(" + len + ")");
							else
								map.db2DataType = ("CLOB(" + len + ")");
						} else {
							if ((colType.equalsIgnoreCase("numeric"))
									|| (colType.equalsIgnoreCase("decimal"))) {
								if ((precision <= 0) || (precision > 31))
									precision = 31;
							}
							map.db2DataType = (map.db2DataType
									+ "("
									+ ((precision == 0)
											|| (precision == 2147483647) ? 255
											: precision) + "," + scale + ")");
						}

					} else if (((map.db2DataType.equalsIgnoreCase("CHAR")) || (map.db2DataType
							.equalsIgnoreCase("CHAR FOR BIT DATA")))
							&& (precision > 254)) {
						map.db2DataType = ("VARCHAR(" + precision + ")");
					} else if ((map.db2DataType.equalsIgnoreCase("BINARY"))
							&& (precision > 254)) {
						map.db2DataType += "(254)";
					} else if ((map.db2DataType.equalsIgnoreCase("GRAPHIC"))
							&& (precision > 127)) {
						map.db2DataType += "(127)";
					} else if ((map.db2DataType.equalsIgnoreCase("VARCHAR"))
							|| (map.db2DataType.equalsIgnoreCase("VARGRAPHIC"))
							|| (map.db2DataType.equalsIgnoreCase("DBCLOB"))
							|| (map.db2DataType.equalsIgnoreCase("CLOB"))) {
						if (GenerateExtract.customMapping) {
							if (GenerateExtract.dbSourceName
									.equalsIgnoreCase("oracle")) {
								int columnSize = precision / 3;
								if (precision == 0) {
									map.db2DataType = (GenerateExtract.graphic ? "VARGRAPHIC(4000)"
											: "VARCHAR(4000)");
								} else if (precision == 1333) {
									map.db2DataType = (GenerateExtract.graphic ? "VARGRAPHIC(4000)"
											: "VARCHAR(1333)");
								} else if ((precision > 0)
										&& (precision < 1333)) {
									map.db2DataType = ("VARCHAR(" + precision + ")");
								} else if ((precision == -1)
										|| (columnSize > 536870912))
									map.db2DataType = (GenerateExtract.graphic ? "DBCLOB(536870912)"
											: "CLOB(536870912)");
								else
									map.db2DataType = ("CLOB(" + precision + ")");
							} else {
								if (precision == 0) {
									map.db2DataType = (GenerateExtract.graphic ? "VARGRAPHIC(4000)"
											: "VARCHAR(4000)");
								} else if ((precision > 0)
										&& (precision < 32673)) {
									map.db2DataType = ("VARCHAR(" + precision + ")");
								} else if ((precision == -1)
										|| (precision > 536870912))
									map.db2DataType = (GenerateExtract.graphic ? "DBCLOB(536870912)"
											: "CLOB(1073741824)");
								else {
									map.db2DataType = ("CLOB(" + precision + ")");
								}
							}

						} else if (precision == -1) {
							map.db2DataType = (GenerateExtract.graphic ? "DBCLOB"
									: "CLOB");
						} else if (precision < 4096) {
							map.db2DataType = ("VARCHAR(" + precision + ")");
						} else {
							map.db2DataType = ("CLOB(" + precision + ")");
						}

					} else if ((map.db2DataType.equalsIgnoreCase("BLOB"))
							|| (map.db2DataType
									.equalsIgnoreCase("VARCHAR FOR BIT DATA"))) {
						if (GenerateExtract.customMapping) {
							if ((precision == -1) || (precision == 0)
									|| (precision > 32672)) {
								map.db2DataType = "BLOB(1073741824)";
								precision = 1073741824;
							} else if ((precision > 0) && (precision < 32673)) {
								map.db2DataType = ("VARCHAR(" + precision + ") FOR BIT DATA");
							}
							if (colType.equalsIgnoreCase("RAW")) {
								map.db2DataType = ("CHAR(" + precision + ") FOR BIT DATA");
							}

						} else if (precision == 0) {
							map.db2DataType = "BLOB(1073741824)";
						} else if ((precision > 0) && (precision < 32673)) {
							map.db2DataType = ("VARCHAR(" + precision + ") FOR BIT DATA");
							if ((GenerateExtract.db2_compatibility)
									&& (GenerateExtract.dbSourceName
											.equalsIgnoreCase("oracle"))) {
								map.db2DataType = map.db2DataType.replaceFirst(
										"VARCHAR", "VARCHAR2");
							}
						} else if (precision > 32672) {
							map.db2DataType = ("BLOB(" + precision + ")");
						}

					} else if (map.db2DataType
							.equalsIgnoreCase("CHAR FOR BIT DATA")) {
						map.db2DataType = ("CHAR(" + precision + ") FOR BIT DATA");
					} else if (map.db2DataType.equalsIgnoreCase("IMAGE")) {
						if (precision == 0) {
							map.db2DataType = "BLOB(1073741824)";
						} else {
							map.db2DataType = (map.db2DataType + "("
									+ precision + ")");
						}
					} else if (map.db2DataType.equalsIgnoreCase("VARBINARY")) {
						if (precision == 0) {
							map.db2DataType = "BLOB(1073741824)";
						} else if (precision > 32672) {
							map.db2DataType = ("BLOB(" + precision + ")");
						} else {
							map.db2DataType = ("VARBINARY(" + precision + ")");
						}

					} else if (GenerateExtract.dbSourceName
							.equalsIgnoreCase("oracle")) {
						if ((map.db2DataType.equalsIgnoreCase("NUMERIC"))
								|| (map.db2DataType.equalsIgnoreCase("NUMBER"))) {
							if ((scale < 0) && (scale != -127)) {
								precision -= scale;
								scale = 0;
							} else if (precision < scale) {
								precision = scale;
							}
							if ((scale == -127) || (scale == 0)) {
								if (GenerateExtract.customMapping) {
									if (precision == 0)
										map.db2DataType = "DOUBLE";
									else if (precision == 1)
										map.db2DataType = "SMALLINT";
									else if (precision == 3)
										map.db2DataType = "DECIMAL(3)";
									else if (precision == 5)
										map.db2DataType = "SMALLINT";
									else if (precision == 10)
										map.db2DataType = "DECIMAL(10)";
									else if ((precision > 1)
											&& (precision < 11))
										map.db2DataType = "INTEGER";
									else if ((precision > 10)
											&& (precision < 21))
										map.db2DataType = "BIGINT";
									else if (precision > 20) {
										map.db2DataType = "DOUBLE";
									}

								} else if (GenerateExtract.db2_compatibility) {
									if (GenerateExtract.oracleNumberMapping) {
										if (precision == 0)
											map.db2DataType = "DOUBLE";
										else if ((precision > 0)
												&& (precision < 5))
											map.db2DataType = "SMALLINT";
										else if ((precision > 4)
												&& (precision < 10))
											map.db2DataType = "INTEGER";
										else if ((precision > 9)
												&& (precision < 19))
											map.db2DataType = "BIGINT";
										else if ((precision > 18)
												&& (precision < 32))
											map.db2DataType = "FLOAT";
										else if (precision > 31) {
											if (GenerateExtract.oracleNumb31Mapping) {
												if ((GenerateExtract.releaseLevel != -1.0F)
														&& (GenerateExtract.releaseLevel >= 9.5F))
													map.db2DataType = "DECFLOAT(34)";
												else
													map.db2DataType = "DOUBLE";
											} else {
												map.db2DataType = "DOUBLE";
											}
										}
									} else if ((GenerateExtract.roundDown_31)
											&& (precision > 31)) {
										if (GenerateExtract.oracleNumb31Mapping) {
											if ((GenerateExtract.releaseLevel != -1.0F)
													&& (GenerateExtract.releaseLevel >= 9.5F))
												map.db2DataType = "DECFLOAT(34)";
											else
												map.db2DataType = "DOUBLE";
										} else
											map.db2DataType += "(31)";
									} else if (precision > 31) {
										if (GenerateExtract.oracleNumb31Mapping) {
											if ((GenerateExtract.releaseLevel != -1.0F)
													&& (GenerateExtract.releaseLevel >= 9.5F))
												map.db2DataType = "DECFLOAT(34)";
											else
												map.db2DataType = "DOUBLE";
										} else
											map.db2DataType = "DOUBLE";
									} else {
										map.db2DataType = (map.db2DataType
												+ "("
												+ (precision == 0 ? 31
														: precision) + ")");
									}

								} else if (precision == 0)
									map.db2DataType = "DOUBLE";
								else if ((precision > 0) && (precision < 5))
									map.db2DataType = "SMALLINT";
								else if ((precision > 4) && (precision < 10))
									map.db2DataType = "INTEGER";
								else if ((precision > 9) && (precision < 19))
									map.db2DataType = "BIGINT";
								else if ((precision > 18) && (precision < 32))
									map.db2DataType = "FLOAT";
								else if (precision > 31) {
									if (GenerateExtract.oracleNumb31Mapping) {
										if ((GenerateExtract.releaseLevel != -1.0F)
												&& (GenerateExtract.releaseLevel >= 9.7F))
											map.db2DataType = "DECFLOAT(34)";
										else
											map.db2DataType = "DOUBLE";
									} else {
										map.db2DataType = "DOUBLE";
									}
								}

							} else if ((GenerateExtract.roundDown_31)
									&& (precision > 31))
								map.db2DataType = (map.db2DataType + "(31,"
										+ scale + ")");
							else if (precision > 31)
								map.db2DataType = "DOUBLE";
							else {
								map.db2DataType = (map.db2DataType + "("
										+ (precision == 0 ? 31 : precision)
										+ "," + scale + ")");
							}
						} else {
							map.db2DataType = (map.db2DataType + "("
									+ (precision == 0 ? 255 : precision) + ","
									+ scale + ")");
						}

					} else if (map.db2DataType.equalsIgnoreCase("NUMERIC")) {
						if ((GenerateExtract.roundDown_31) && (precision > 31)) {
							if (scale == 0)
								map.db2DataType += "(31)";
							else
								map.db2DataType = (map.db2DataType + "(31,"
										+ scale + ")");
						} else if (precision > 31)
							map.db2DataType = "DOUBLE";
						else
							map.db2DataType = (map.db2DataType + "("
									+ (precision == 0 ? 31 : precision) + ","
									+ scale + ")");
					} else {
						map.db2DataType = (map.db2DataType + "("
								+ (precision == 0 ? 255 : precision) + ","
								+ scale + ")");
					}

				}

				if (((map.db2DataType.startsWith("BLOB")) || (map.db2DataType
						.startsWith("CLOB")))
						&& (precision > 1073741824)) {
					map.db2DataType += (GenerateExtract.dbTargetName
							.equalsIgnoreCase("zdb2") ? "" : " NOT LOGGED");
				}
				genDB2DefaultValues(GenerateExtract.schemaName[id],
						GenerateExtract.srcSchName[id],
						GenerateExtract.srcTableName[id], colName, nullType,
						map.db2DataType);
				buffer.append("\"" + colName + "\"" + " " + map.db2DataType
						+ " " + nullType + " " + identity);
				if (colIndex != colCount) {
					buffer.append("," + GenerateExtract.linesep);
				} else
					buffer.append(GenerateExtract.linesep);
			}
			buffer.append(")" + GenerateExtract.linesep);
			if ((GenerateExtract.dbSourceName.equalsIgnoreCase("zdb2"))
					|| (GenerateExtract.dbSourceName.equalsIgnoreCase("db2"))) {
				String part = "";
				if ((part != null) && (part.length() != 0))
					buffer.append(part + GenerateExtract.linesep);
			} else if (GenerateExtract.dbSourceName.equalsIgnoreCase("oracle")) {
				if (!GenerateExtract.useBestPracticeTSNames) {
					String tsName = getOraTableSpaces(
							GenerateExtract.srcSchName[id],
							GenerateExtract.srcTableName[id]);
					if ((tsName != null) && (tsName.length() != 0)) {
						buffer.append(tsName + GenerateExtract.linesep);
					}
				}
				String part = genOraPartitions(GenerateExtract.srcSchName[id],
						GenerateExtract.srcTableName[id]);
				if ((part != null) && (part.length() != 0)) {
					buffer.append(part + GenerateExtract.linesep);
				}
			}
			if (GenerateExtract.compressTable) {
				if (isGlobal.equals(""))
					buffer.append("COMPRESS YES" + GenerateExtract.linesep);
				else
					buffer.append(commitToken + GenerateExtract.linesep);
			}
			if (GenerateExtract.dbTargetName.equals("zdb2"))
				buffer.append("CCSID UNICODE" + GenerateExtract.linesep);
			buffer.append(";" + GenerateExtract.linesep);
			buffer.append(GenerateExtract.linesep);
			if (GenerateExtract.db2_compatibility) {
				GenerateExtract.db2TablesWriter.write(buffer.toString());
			} else {
				try {
					String dml = ModifyTableAll(buffer.toString(),
							GenerateExtract.schemaName[id],
							GenerateExtract.tableName[id]);
					GenerateExtract.db2TablesWriter.write(dml);
				} catch (Exception e) {
					e.printStackTrace();
					GenerateExtract.db2TablesWriter.write(buffer.toString());
				}
			}
			GenerateExtract.db2TablesWriter.write(seqBuffer.toString());
			GenerateExtract.db2TablesWriter.write(getTableComments(
					GenerateExtract.schemaName[id],
					GenerateExtract.tableName[id]));
		}

		private long countRows(int id) throws SQLException {
			PreparedStatement countStatement = this.bladeConn
					.prepareStatement(GenerateExtract.countSQL[id]);
			ResultSet rs = countStatement.executeQuery();
			if (rs.next()) {
				long rows = Long.parseLong(rs.getString(1));
				if (rs != null)
					rs.close();
				if (countStatement != null)
					countStatement.close();
				return rows;
			}
			if (rs != null)
				rs.close();
			if (countStatement != null)
				countStatement.close();
			return 0L;
		}

		private String getInputFileName(int id) throws Exception {
			String inputFile = "";
			String fil;
			if (GenerateExtract.multiTables[id] == 0) {
				fil = IBMExtractUtilities
						.FixSpecialChars(GenerateExtract.schemaName[id]
								.toLowerCase()
								+ "_"
								+ GenerateExtract.tableName[id].toLowerCase());
			} else {
				fil = IBMExtractUtilities.FixSpecialChars(new StringBuilder()
						.append(GenerateExtract.schemaName[id].toLowerCase())
						.append("_").append(
								GenerateExtract.tableName[id].toLowerCase())
						.toString())
						+ GenerateExtract.multiTables[id];
			}
			fil = fil.replace("\"", "");
			File f = new File(GenerateExtract.OUTPUT_DIR + "data"
					+ GenerateExtract.filesep + fil + ".txt");
			inputFile = "\"" + f.getCanonicalPath() + "\""
					+ GenerateExtract.linesep;
			return inputFile;
		}

		private String getAllInputFileNames(int id) throws Exception {
			String inputFile = "";
			for (int i = id; i < GenerateExtract.totalTables; i++) {
				if (i == id)
					inputFile = getInputFileName(i);
				if (i == GenerateExtract.totalTables - 1)
					break;
				if (GenerateExtract.multiTables[(i + 1)] == 0) {
					break;
				}
				inputFile = inputFile + "," + getInputFileName(i + 1);
			}

			return inputFile;
		}

		private void genDB2LoadScript(int[] dataLen,
				GenerateExtract.BlobVal bval, ResultSetMetaData metadata, int id)
				throws Exception {
			String tmpRemote = GenerateExtract.remoteLoad ? " CLIENT " : "";
			StringBuffer buffer = new StringBuffer();
			String lobsinfile = "";
			String fil = IBMExtractUtilities
					.FixSpecialChars(GenerateExtract.schemaName[id]
							.toLowerCase()
							+ "_" + GenerateExtract.tableName[id].toLowerCase());
			fil = fil.replace("\"", "");
			File f = new File(GenerateExtract.OUTPUT_DIR + "data"
					+ GenerateExtract.filesep + fil + ".txt");
			f = new File(GenerateExtract.OUTPUT_DIR + "data"
					+ GenerateExtract.filesep + fil + "2.txt");
			f = new File(GenerateExtract.OUTPUT_DIR + "dump"
					+ GenerateExtract.filesep + fil + ".txt");
			String dumpFile = f.getCanonicalPath();
			f = new File(GenerateExtract.OUTPUT_DIR + "msg"
					+ GenerateExtract.filesep + fil + ".txt");
			String msgFile = f.getCanonicalPath();
			f = new File(GenerateExtract.OUTPUT_DIR + "data"
					+ GenerateExtract.filesep + fil);
			String xmlDir = f.getCanonicalPath();
			String codePage = "";

			if (GenerateExtract.encoding.equalsIgnoreCase("utf-8")) {
				codePage = " CODEPAGE=1208 ";
			}
			int colCount = metadata.getColumnCount();
			buffer.setLength(0);

			buffer.append("--#SET :LOAD:"
					+ GenerateExtract
							.removeQuote(GenerateExtract.schemaName[id]
									.toUpperCase())
					+ ":"
					+ GenerateExtract.removeQuote(GenerateExtract.tableName[id]
							.toUpperCase()) + GenerateExtract.linesep);
			buffer.append("LOAD " + tmpRemote + " FROM "
					+ GenerateExtract.linesep + getAllInputFileNames(id));
			buffer.append("OF DEL " + GenerateExtract.linesep);
			if ((bval.isBlob) || (bval.isClob)) {
				lobsinfile = "LOBSINFILE";
				buffer.append("LOBS FROM \"" + xmlDir + GenerateExtract.filesep
						+ "\"" + GenerateExtract.linesep);
			}
			if (bval.isXml) {
				buffer.append("XML FROM \"" + xmlDir + GenerateExtract.filesep
						+ "\"" + GenerateExtract.linesep);
			}
			if (GenerateExtract.remoteLoad) {
				buffer
						.append("-- For Remote LOAD, you have to copy or mount LOBS FROM or XML FROM directory to the remote DB2 server otherwise LOAD will fail."
								+ GenerateExtract.linesep);
			}

			buffer
					.append("MODIFIED BY " + lobsinfile + " " + codePage
							+ " COLDEL" + GenerateExtract.colsep
							+ (bval.isXml ? " " : " ANYORDER ")
							+ " USEDEFAULTS CHARDEL\"\" DELPRIORITYCHAR "
							+ (GenerateExtract.debug ? "" : " NOROWWARNINGS ")
							+ (bval.isXml ? " XMLCHAR " : "")
							+ GenerateExtract.linesep);
			if (GenerateExtract.remoteLoad) {
				buffer
						.append("-- For Remote LOAD, the DUMPFILE directory should exist on the remote DB2 server otherwise LOAD will fail."
								+ GenerateExtract.linesep);
				buffer
						.append("-- Uncomment following line if DUMPFILE dir exists on the remote DB2 server."
								+ GenerateExtract.linesep);
				buffer.append("-- DUMPFILE=\"" + dumpFile + "\""
						+ GenerateExtract.linesep);
			}
			if ((!bval.isXml) && (!bval.isBlob) && (!bval.isClob)) {
				if (GenerateExtract.dumpfileb) {
					buffer.append("DUMPFILE=\"" + dumpFile + "\""
							+ GenerateExtract.linesep);
				} else {
					buffer.append("--DUMPFILE=\"" + dumpFile + "\""
							+ GenerateExtract.linesep);
				}
			}
			buffer.append("METHOD P (");
			for (int i = 1; i <= colCount; i++) {
				buffer.append(i);
				if (i == colCount)
					continue;
				buffer.append(",");
			}

			buffer.append(")" + GenerateExtract.linesep);
			if (!GenerateExtract.limitLoadRows.equalsIgnoreCase("ALL")) {
				try {
					int x = Integer.parseInt(GenerateExtract.limitLoadRows);
					if (x >= 0)
						buffer
								.append("ROWCOUNT " + x
										+ GenerateExtract.linesep);
				} catch (Exception v) {
				}
			}
			buffer.append("MESSAGES \"" + msgFile + "\""
					+ GenerateExtract.linesep);
			buffer.append((GenerateExtract.loadReplace ? "REPLACE" : "INSERT")
					+ " INTO " + GenerateExtract.schemaName[id].toUpperCase()
					+ "." + GenerateExtract.tableName[id].toUpperCase()
					+ GenerateExtract.linesep);
			buffer.append("(" + GenerateExtract.linesep);
			for (int i = 1; i <= colCount; i++) {
				String tmp;
				if (GenerateExtract.retainColName) {
					tmp = GenerateExtract.getTruncName(0, "", metadata
							.getColumnName(i), 128);
				} else {
					tmp = GenerateExtract.getTruncName(0, "", metadata
							.getColumnName(i), 30);
				}
				buffer.append("\"" + tmp + "\"");
				if (i != colCount) {
					buffer.append("," + GenerateExtract.linesep);
				} else {
					buffer.append(GenerateExtract.linesep);
				}

			}

			buffer.append(")" + GenerateExtract.linesep);
			if (!bval.isXml) {
				if ((GenerateExtract.loadstats)
						&& (GenerateExtract.loadReplace)) {
					buffer
							.append("STATISTICS YES WITH DISTRIBUTION AND DETAILED INDEXES ALL"
									+ GenerateExtract.linesep);
				} else {
					buffer
							.append("--STATISTICS YES WITH DISTRIBUTION AND DETAILED INDEXES ALL"
									+ GenerateExtract.linesep);
				}
			}
			if (!bval.isXml) {
				buffer.append(" NONRECOVERABLE " + GenerateExtract.linesep);
				buffer.append("INDEXING MODE AUTOSELECT"
						+ GenerateExtract.linesep);
			}
			buffer.append(";" + GenerateExtract.linesep);
			buffer.append(GenerateExtract.linesep);
			GenerateExtract.db2LoadWriter.write(buffer.toString());
			buffer.setLength(0);
			buffer.append("RUNSTATS ON TABLE "
					+ GenerateExtract.schemaName[id].toUpperCase() + "."
					+ GenerateExtract.tableName[id].toUpperCase()
					+ GenerateExtract.linesep);
			buffer.append("ON ALL COLUMNS WITH DISTRIBUTION"
					+ GenerateExtract.linesep);
			buffer.append("ON ALL COLUMNS AND DETAILED INDEXES ALL"
					+ GenerateExtract.linesep);
			buffer.append("ALLOW WRITE ACCESS ;" + GenerateExtract.linesep);
			buffer.append(GenerateExtract.linesep);
			buffer.append(GenerateExtract.linesep);
			GenerateExtract.db2RunstatWriter.write(buffer.toString());
			GenerateExtract.db2TabCountWriter.write(getDB2Count(id));
			GenerateExtract.db2LoadTerminateWriter
					.write(getDB2LoadTerminate(id));
			GenerateExtract.db2CheckPendingWriter.write("SET INTEGRITY FOR "
					+ GenerateExtract.schemaName[id].toUpperCase() + "."
					+ GenerateExtract.tableName[id].toUpperCase()
					+ " IMMEDIATE CHECKED;" + GenerateExtract.linesep);
			GenerateExtract.db2TabStatusWriter
					.write("select substr(rtrim(tabschema)||'.'||rtrim(tabname),1,50) TABLE_NAME, substr(const_checked,1,1) FK_CHECKED, substr(const_checked,2,1) CC_CHECKED, status from syscat.tables where tabschema = '"
							+ GenerateExtract
									.removeQuote(GenerateExtract.schemaName[id]
											.toUpperCase())
							+ "' and tabname = '"
							+ GenerateExtract
									.removeQuote(GenerateExtract.tableName[id]
											.toUpperCase())
							+ "';"
							+ GenerateExtract.linesep);
		}

		private StringBuffer getzDB2Load(int id, ResultSetMetaData metadata)
				throws Exception {
			String nocopypend = GenerateExtract.znocopypend ? "NOCOPYPEND" : "";
			StringBuffer buffer = new StringBuffer();
			GenerateExtract.DataMap map = new GenerateExtract.DataMap();

			int colCount = metadata.getColumnCount();

			buffer.setLength(0);
			buffer.append("CALL SYSPROC.DSNUTILS('"
					+ GenerateExtract.getHexCode("L") + "','NO',"
					+ GenerateExtract.linesep);
			buffer.append("'LOAD DATA REPLACE LOG NO " + nocopypend
					+ " FORMAT DELIMITED " + GenerateExtract.linesep);
			buffer.append("COLDEL X'"
					+ GenerateExtract.getHexCode(GenerateExtract.colsep)
					+ "' CHARDEL X'" + GenerateExtract.getHexCode("\"")
					+ "' DECPT X'" + GenerateExtract.getHexCode(".") + "' "
					+ GenerateExtract.linesep);
			buffer.append("ENFORCE NO " + GenerateExtract.linesep);
			buffer.append("UNICODE CCSID(1208,1208,0) "
					+ GenerateExtract.linesep);
			buffer.append("INTO TABLE "
					+ GenerateExtract.schemaName[id].toUpperCase() + "."
					+ GenerateExtract.tableName[id].toUpperCase()
					+ GenerateExtract.linesep);
			buffer.append("(" + GenerateExtract.linesep);
			for (int colIndex = 1; colIndex <= colCount; colIndex++) {
				int precision;
				try {
					precision = metadata.getPrecision(colIndex);
				} catch (Exception e) {
					precision = 2147483647;
					GenerateExtract
							.log("Error: Precision found more than 2GB limiting it to 2GB");
				}

				String colType = metadata.getColumnTypeName(colIndex)
						.toUpperCase();
				String colName = GenerateExtract.retainColName ? GenerateExtract
						.getTruncName(0, "", metadata.getColumnName(colIndex),
								128)
						: GenerateExtract.getTruncName(0, "", metadata
								.getColumnName(colIndex), 30);

				map.sourceDataType = colType;
				getDB2Type(map, id);
				String modifier;
				if (map.db2DataType.startsWith("CHAR")) {
					modifier = "CHAR";
				} else {
					if (map.db2DataType.startsWith("VARCHAR")) {
						modifier = "VARCHAR";
					} else {
						if (map.db2DataType.startsWith("BINARY")) {
							modifier = "BINARY";
						} else {
							if (map.db2DataType.startsWith("VARBINARY")) {
								if (precision > 32000)
									modifier = "VARCHAR BLOBF";
								else
									modifier = "VARBINARY";
							} else {
								if (map.db2DataType.startsWith("XML")) {
									modifier = "VARCHAR BLOBF";
								} else {
									if (map.db2DataType.startsWith("GRAPHIC")) {
										modifier = "GRAPHIC";
									} else {
										if (map.db2DataType
												.startsWith("VARGRAPHIC")) {
											modifier = "VARGRAPHIC";
										} else {
											if (map.db2DataType
													.startsWith("DBCLOB")) {
												modifier = "VARCHAR DBCLOBF";
											} else {
												if (map.db2DataType
														.startsWith("CLOB")) {
													modifier = GenerateExtract.mssqltexttoclob ? "VARCHAR CLOBF"
															: "VARCHAR";
												} else {
													if (map.db2DataType
															.startsWith("BLOB")) {
														modifier = "VARCHAR BLOBF";
													} else {
														if (map.db2DataType
																.startsWith("INT")) {
															modifier = "INTEGER EXTERNAL";
														} else {
															if (map.db2DataType
																	.startsWith("DOUBLE")) {
																modifier = "DOUBLE EXTERNAL";
															} else {
																if (map.db2DataType
																		.startsWith("FLOAT")) {
																	modifier = "FLOAT EXTERNAL";
																} else {
																	if (map.db2DataType
																			.startsWith("NUMERIC")) {
																		modifier = "DECIMAL EXTERNAL";
																	} else {
																		if (map.db2DataType
																				.equals("DATE")) {
																			modifier = "DATE EXTERNAL";
																		} else {
																			if (map.db2DataType
																					.equals("TIME")) {
																				modifier = "TIME EXTERNAL";
																			} else {
																				if (map.db2DataType
																						.equals("TIMESTAMP")) {
																					modifier = "TIMESTAMP EXTERNAL";
																				} else {
																					if (map.db2DataType
																							.startsWith("DECFLOAT"))
																						modifier = "DECFLOAT EXTERNAL";
																					else
																						modifier = map.db2DataType;
																				}
																			}
																		}
																	}
																}
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
				buffer.append("\"" + colName + "\" " + modifier);
				buffer.append(colIndex != colCount ? ","
						+ GenerateExtract.linesep : GenerateExtract.linesep);
			}

			buffer.append(")" + GenerateExtract.linesep);
			buffer
					.append("IDENTITYOVERRIDE', ?, 'LOAD', '<INPUTDATASETNAME>','',0,'<DISCARDDATASETNAME>','SYSDA',10, "
							+ GenerateExtract.linesep);
			buffer
					.append("'','',0,'','',0,'','',0,'','',0,'','',0,'<SYSUT1>','SYSDA',10,'<SORTOUT>','SYSDA',10,'','',0, "
							+ GenerateExtract.linesep);
			buffer.append("'<ERRORDATASETNAME>','SYSDA',10,'','',0)"
					+ GenerateExtract.linesep);
			buffer.append(";" + GenerateExtract.linesep);
			buffer.append(GenerateExtract.linesep);
			return buffer;
		}

		private StringBuffer getzDB2Check(int id) {
			StringBuffer buffer = new StringBuffer();
			buffer
					.append("SELECT 'CALL SYSPROC.DSNUTILS(''"
							+ GenerateExtract.removeQuote("C")
							+ "'',''NO'',''CHECK DATA TABLESPACE \"'||DBNAME||'\".\"'||TSNAME||'\" "
							+ GenerateExtract.linesep);
			buffer
					.append("SHRLEVEL CHANGE'',?, ''CHECK DATA'', '''','''',0,'''','''',0, "
							+ GenerateExtract.linesep);
			buffer
					.append("'''','''',0,'''','''',0,'''','''',0,'''','''',0,'''','''',0,'''','''',0,'''','''',0,'''','''',0, "
							+ GenerateExtract.linesep);
			buffer.append("''<ERRORDATASETNAME>'',''SYSDA'',10,'''','''',0);'"
					+ GenerateExtract.linesep);
			buffer.append("FROM SYSIBM.SYSTABLES WHERE CREATOR = '"
					+ GenerateExtract
							.removeQuote(GenerateExtract.schemaName[id]
									.toUpperCase()) + "' "
					+ GenerateExtract.linesep);
			buffer.append("AND NAME = '"
					+ GenerateExtract.removeQuote(GenerateExtract.tableName[id]
							.toUpperCase()) + "'" + GenerateExtract.linesep);
			buffer.append("AND CHECKFLAG = 'C';" + GenerateExtract.linesep);
			return buffer;
		}

		private String getzDB2RUNS(int id) {
			StringBuffer buffer = new StringBuffer();
			buffer
					.append("SELECT 'CALL SYSPROC.DSNUTILS(''"
							+ GenerateExtract.removeQuote("S")
							+ "'',''NO'',''RUNSTATS TABLESPACE \"'||DBNAME||'\".\"'||TSNAME||'\" "
							+ GenerateExtract.linesep);
			buffer.append("TABLE("
					+ GenerateExtract.schemaName[id].toUpperCase() + "."
					+ GenerateExtract.tableName[id].toUpperCase()
					+ ") COLUMN(ALL) INDEX(ALL) SHRLEVEL CHANGE'',"
					+ GenerateExtract.linesep);
			buffer
					.append("?, ''RUNSTATS TABLESPACE'', '''','''',0,'''','''',0, "
							+ GenerateExtract.linesep);
			buffer
					.append("'''','''',0,'''','''',0,'''','''',0,'''','''',0,'''','''',0,'''','''',0,'''','''',0,'''','''',0, "
							+ GenerateExtract.linesep);
			buffer.append("'''','''',0,'''','''',0);'"
					+ GenerateExtract.linesep);
			buffer.append("FROM SYSIBM.SYSTABLES WHERE CREATOR = '"
					+ GenerateExtract
							.removeQuote(GenerateExtract.schemaName[id]
									.toUpperCase()) + "' "
					+ GenerateExtract.linesep);
			buffer.append("AND NAME = '"
					+ GenerateExtract.removeQuote(GenerateExtract.tableName[id]
							.toUpperCase()) + "';" + GenerateExtract.linesep);
			buffer.append(GenerateExtract.linesep);
			return buffer.toString();
		}

		private String getzDB2TABS(int id) {
			StringBuffer buffer = new StringBuffer();
			buffer.append("SELECT SUBSTR(A.NAME,1,30) TABLE_NAME, "
					+ GenerateExtract.linesep);
			buffer.append("CASE A.STATUS " + GenerateExtract.linesep);
			buffer.append("  WHEN ' ' THEN 'COMPLETE' "
					+ GenerateExtract.linesep);
			buffer.append("  WHEN 'X' THEN 'COMPLETE' "
					+ GenerateExtract.linesep);
			buffer.append("  WHEN 'I' THEN 'SEE TABLE_STATUS2' "
					+ GenerateExtract.linesep);
			buffer.append("  WHEN 'R' THEN 'REGEN ERROR' "
					+ GenerateExtract.linesep);
			buffer.append("  ELSE 'UNKNOWN' " + GenerateExtract.linesep);
			buffer.append("END AS TABLE_STATUS1, " + GenerateExtract.linesep);
			buffer.append("CASE SUBSTR(A.TABLESTATUS,1,2) "
					+ GenerateExtract.linesep);
			buffer.append("  WHEN 'L' THEN 'AUX TABLE' "
					+ GenerateExtract.linesep);
			buffer.append("  WHEN 'P' THEN 'PRIMARY INDEX' "
					+ GenerateExtract.linesep);
			buffer.append("  WHEN 'R' THEN 'ROW ID INDEX' "
					+ GenerateExtract.linesep);
			buffer.append("  WHEN 'U' THEN 'UNIQUE KEY INDEX' "
					+ GenerateExtract.linesep);
			buffer.append("  WHEN 'V' THEN 'VIEW REGEN ERROR' "
					+ GenerateExtract.linesep);
			buffer.append("  WHEN ' ' THEN 'COMPLETE' "
					+ GenerateExtract.linesep);
			buffer.append("  ELSE 'UNKNOWN' " + GenerateExtract.linesep);
			buffer.append("END AS TABLE_STATUS2, " + GenerateExtract.linesep);
			buffer.append("CASE A.CHECKFLAG " + GenerateExtract.linesep);
			buffer.append("  WHEN 'C' THEN 'RI/CHECK ERROR' "
					+ GenerateExtract.linesep);
			buffer.append("  WHEN ' ' THEN 'CONSISTENT' "
					+ GenerateExtract.linesep);
			buffer.append("  ELSE 'UNKNOWN' " + GenerateExtract.linesep);
			buffer.append("END AS CHECKFLAG, " + GenerateExtract.linesep);
			buffer.append("CASE B.STATUS " + GenerateExtract.linesep);
			buffer.append("  WHEN 'A' THEN 'COMPLETE' "
					+ GenerateExtract.linesep);
			buffer.append("  WHEN 'C' THEN 'PI MISSING' "
					+ GenerateExtract.linesep);
			buffer.append("  WHEN 'P' THEN 'CHECK PEND' "
					+ GenerateExtract.linesep);
			buffer.append("  WHEN 'S' THEN 'CHECK PEND' "
					+ GenerateExtract.linesep);
			buffer.append("  WHEN 'T' THEN 'NO TABLES' "
					+ GenerateExtract.linesep);
			buffer.append("  ELSE 'UNKNOWN' " + GenerateExtract.linesep);
			buffer.append("END AS TS_STATUS " + GenerateExtract.linesep);
			buffer.append("FROM SYSIBM.SYSTABLES A, SYSIBM.SYSTABLESPACE B "
					+ GenerateExtract.linesep);
			buffer.append("WHERE A.CREATOR = '"
					+ GenerateExtract
							.removeQuote(GenerateExtract.schemaName[id]
									.toUpperCase()) + "' "
					+ GenerateExtract.linesep);
			buffer.append("AND A.NAME = '"
					+ GenerateExtract.removeQuote(GenerateExtract.tableName[id]
							.toUpperCase()) + "' " + GenerateExtract.linesep);
			buffer.append("AND B.NAME = A.TSNAME " + GenerateExtract.linesep);
			buffer.append("AND B.DBNAME = A.DBNAME;" + GenerateExtract.linesep);
			return buffer.toString();
		}

		private String getTableComments(String schemaName, String tableName) {
			PreparedStatement prepStatement = null;
			ResultSet rs1 = null;
			String sql = "";
			String sql2 = "";
			String type = "TABLE";
			String dstSchema = GenerateExtract.getSrctoDstSchema(schemaName);
			String comment = "";
			String columnName = "";
			StringBuffer buffer = new StringBuffer();

			if (GenerateExtract.dbSourceName.equalsIgnoreCase("oracle")) {
				sql = "SELECT COMMENTS FROM DBA_TAB_COMMENTS WHERE OWNER = '"
						+ GenerateExtract.removeQuote(schemaName)
						+ "' AND TABLE_TYPE = '" + type
						+ "' AND TABLE_NAME = '"
						+ GenerateExtract.removeQuote(tableName)
						+ "' AND COMMENTS IS NOT NULL";

				sql2 = "SELECT COLUMN_NAME, COMMENTS FROM DBA_COL_COMMENTS WHERE OWNER = '"
						+ GenerateExtract.removeQuote(schemaName)
						+ "' AND TABLE_NAME = '"
						+ GenerateExtract.removeQuote(tableName)
						+ "' AND COMMENTS IS NOT NULL";
			} else if (GenerateExtract.dbSourceName.equalsIgnoreCase("idb2")) {
				sql = "SELECT TABLE_TEXT FROM SYSIBM.SQLTABLES WHERE TABLE_SCHEM = '"
						+ GenerateExtract.removeQuote(schemaName)
						+ "' AND TABLE_TYPE = '"
						+ type
						+ "' AND TABLE_NAME = '"
						+ GenerateExtract.removeQuote(tableName)
						+ "' AND TABLE_TEXT IS NOT NULL";

				sql2 = "SELECT COLUMN_NAME, COLUMN_TEXT FROM SYSIBM.SQLCOLUMNS WHERE TABLE_SCHEM = '"
						+ GenerateExtract.removeQuote(schemaName)
						+ "' AND TABLE_NAME = '"
						+ GenerateExtract.removeQuote(tableName)
						+ "' AND COLUMN_TEXT IS NOT NULL";
			}

			if (sql.equals("")) {
				return "";
			}
			try {
				prepStatement = this.bladeConn.prepareStatement(sql);
				rs1 = prepStatement.executeQuery();
				if (rs1.next()) {
					comment = rs1.getString(1);
					if ((comment != null) && (comment.length() > 0)) {
						if (comment.length() > 255)
							comment = comment.substring(1, 255);
						comment = comment.replace("'", "''");
						buffer.append("COMMENT ON " + type + " " + dstSchema
								+ "." + tableName + " IS '" + comment + "'"
								+ GenerateExtract.linesep + ";"
								+ GenerateExtract.linesep);
					}
				}
				if (rs1 != null)
					rs1.close();
				if (prepStatement != null) {
					prepStatement.close();
				}
				prepStatement = this.bladeConn.prepareStatement(sql2);
				rs1 = prepStatement.executeQuery();
				while (rs1.next()) {
					columnName = rs1.getString(1);
					comment = rs1.getString(2);
					if ((comment == null) || (comment.length() <= 0))
						continue;
					comment = comment.replace("'", "''");
					if (comment.length() > 255)
						comment = comment.substring(1, 255);
					comment = comment.replace("'", "''");
					buffer.append("COMMENT ON COLUMN " + dstSchema + "."
							+ tableName + ".\"" + columnName + "\" IS '"
							+ comment + "'" + GenerateExtract.linesep + ";"
							+ GenerateExtract.linesep);
				}

				if (rs1 != null)
					rs1.close();
				if (prepStatement != null)
					prepStatement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			return buffer.toString();
		}

		private String getDB2LoadTerminate(int id) throws Exception {
			StringBuffer buffer = new StringBuffer();
			if (GenerateExtract.osType.equalsIgnoreCase("win"))
				buffer.append("LOAD FROM NUL: OF DEL TERMINATE INTO "
						+ GenerateExtract.schemaName[id].toUpperCase() + "."
						+ GenerateExtract.tableName[id].toUpperCase() + ";"
						+ GenerateExtract.linesep);
			else
				buffer.append("LOAD FROM /dev/null OF DEL TERMINATE INTO "
						+ GenerateExtract.schemaName[id].toUpperCase() + "."
						+ GenerateExtract.tableName[id].toUpperCase() + ";"
						+ GenerateExtract.linesep);
			return buffer.toString();
		}

		private String getDB2Count(int id) throws Exception {
			StringBuffer buffer = new StringBuffer();
			buffer
					.append("select count_big(*) "
							+ GenerateExtract
									.putQuote(GenerateExtract
											.getTruncName(
													0,
													"",
													new StringBuilder()
															.append(
																	GenerateExtract
																			.removeQuote(GenerateExtract.schemaName[id]
																					.toUpperCase()))
															.append(".")
															.append(
																	GenerateExtract
																			.removeQuote(GenerateExtract.tableName[id]
																					.toUpperCase()))
															.toString(), 30))
							+ " FROM "
							+ GenerateExtract.schemaName[id].toUpperCase()
							+ "." + GenerateExtract.tableName[id].toUpperCase()
							+ ";" + GenerateExtract.linesep);

			return buffer.toString();
		}

		private void genzDB2LoadScript(int id, ResultSetMetaData metadata)
				throws Exception {
			if (GenerateExtract.zOSDataSets[id] != null) {
				String[] dsNames = GenerateExtract.zOSDataSets[id].split(",");
				StringBuffer buffer = getzDB2Load(id, metadata);
				for (int i = 0; i < dsNames.length; i++) {
					String inputName = dsNames[i].replace("'", "");
					String discName = inputName + ".DISC";
					String errName = inputName + ".LERR";
					String sysut1 = inputName + ".UT1";
					String sortout = inputName + ".OUT";
					String script = buffer.toString();
					script = script.replaceFirst("<INPUTDATASETNAME>",
							inputName);
					script = script.replaceFirst("<DISCARDDATASETNAME>",
							discName);
					script = script.replaceFirst("<ERRORDATASETNAME>", errName);
					script = script.replaceFirst("<SYSUT1>", sysut1);
					script = script.replaceFirst("<SORTOUT>", sortout);
					if (i > 0) {
						script = script.replaceFirst("DATA REPLACE",
								"DATA RESUME YES");
					}
					GenerateExtract.db2LoadWriter.write(script);
				}
				String script = getzDB2RUNS(id);
				GenerateExtract.db2RunstatWriter.write(script);
				script = getDB2Count(id);
				GenerateExtract.db2TabCountWriter.write(script);
				buffer = getzDB2Check(id);
				script = buffer.toString();
				String errName = dsNames[0].replace("'", "") + ".CERR";
				script = script.replaceFirst("<ERRORDATASETNAME>", errName);
				GenerateExtract.db2CheckPendingWriter.write(script);
				script = getzDB2TABS(id);
				GenerateExtract.db2TabStatusWriter.write(script);
			}
		}

		private String lobColName(GenerateExtract.BlobVal bval, int length,
				int id, int colIndex, String fileName) {
			String lobColData = "";
			if (GenerateExtract.lobsToFiles) {
				if (length == 0)
					lobColData = "";
				else
					lobColData = fileName;
			} else {
				if (length == 0)
					return lobColData;
				if (bval.isXML[(colIndex - 1)] != false) {
					if (GenerateExtract.multiTables[id] == 0) {
						lobColData = "\"<XDS FIL='X" + bval.xmlFileCounter
								+ ".lob' OFF='" + bval.xmlOffset + "' LEN='"
								+ length + "'/>\"";
					} else {
						lobColData = "\"<XDS FIL='X" + bval.xmlFileCounter
								+ ".lob" + GenerateExtract.multiTables[id]
								+ "' OFF='" + bval.xmlOffset + "' LEN='"
								+ length + "'/>\"";
					}
					bval.xmlOffset += length;
				}
				if (!bval.isBLOB[(colIndex - 1)]) {
					if (GenerateExtract.multiTables[id] == 0) {
						lobColData = "B" + bval.blobFileCounter + ".lob."
								+ bval.blobOffset + "." + length + "/";
					} else {
						lobColData = "B" + bval.blobFileCounter + ".lob"
								+ GenerateExtract.multiTables[id] + "."
								+ bval.blobOffset + "." + length + "/";
					}
					bval.blobOffset += length;
				}
				if (bval.isCLOB[(colIndex - 1)] != false) {
					if (GenerateExtract.multiTables[id] == 0) {
						lobColData = "C" + bval.clobFileCounter + ".lob."
								+ bval.clobOffset + "." + length + "/";
					} else {
						lobColData = "C" + bval.clobFileCounter + ".lob"
								+ GenerateExtract.multiTables[id] + "."
								+ bval.clobOffset + "." + length + "/";
					}
					bval.clobOffset += length;
				}
			}
			return lobColData;
		}

		private String getLobFileName(GenerateExtract.BlobVal bval, int id,
				int ColIndex, String initValue) throws IOException {
			String fileName = GenerateExtract.schemaName[id].toLowerCase()
					+ "_" + GenerateExtract.tableName[id].toLowerCase();
			fileName = fileName.replace("\"", "");
			fileName = GenerateExtract.OUTPUT_DIR + "data"
					+ GenerateExtract.filesep + fileName;
			File lobDir = new File(fileName);
			if (!lobDir.exists())
				lobDir.mkdirs();
			if (GenerateExtract.lobsToFiles) {
				fileName = fileName + GenerateExtract.filesep + getLobSeq("L")
						+ ".lob";
				File tmpfile = new File(fileName);
				fileName = tmpfile.getCanonicalPath();
				this.blobWriter = new BufferedOutputStream(
						new FileOutputStream(fileName));
			} else if (initValue != null) {
				if (initValue.equalsIgnoreCase("blob"))
					fileName = fileName + "/B0.lob";
				else if (initValue.equalsIgnoreCase("clob"))
					fileName = fileName + "/C0.lob";
				else if (initValue.equalsIgnoreCase("xml"))
					fileName = fileName + "/X0.lob";
			} else {
				if (bval.isBLOB[(ColIndex - 1)] != false) {
					if (this.filesizelimit < bval.blobOffset) {
						bval.blobFileCounter += 1;
						bval.blobOffset = 0;
						if (this.blobWriter != null) {
							this.blobWriter.close();
						}
						if (GenerateExtract.multiTables[id] == 0) {
							fileName = fileName + "/B" + bval.blobFileCounter
									+ ".lob";
						} else {
							fileName = fileName + "/B" + bval.blobFileCounter
									+ ".lob" + GenerateExtract.multiTables[id];
						}
						this.blobWriter = new BufferedOutputStream(
								new FileOutputStream(fileName));
					} else if (GenerateExtract.multiTables[id] == 0) {
						fileName = fileName + "/B" + bval.blobFileCounter
								+ ".lob";
					} else {
						fileName = fileName + "/B" + bval.blobFileCounter
								+ ".lob" + GenerateExtract.multiTables[id];
					}
				}

				if (bval.isCLOB[(ColIndex - 1)] != false) {
					if (this.filesizelimit < bval.clobOffset) {
						bval.clobFileCounter += 1;
						bval.clobOffset = 0;
						if (this.clobWriter != null) {
							this.clobWriter.close();
						}
						if (GenerateExtract.multiTables[id] == 0) {
							fileName = fileName + "/C" + bval.clobFileCounter
									+ ".lob";
						} else {
							fileName = fileName + "/C" + bval.clobFileCounter
									+ ".lob" + GenerateExtract.multiTables[id];
						}
						this.clobWriter = new BufferedOutputStream(
								new FileOutputStream(fileName));
					} else if (GenerateExtract.multiTables[id] == 0) {
						fileName = fileName + "/C" + bval.clobFileCounter
								+ ".lob";
					} else {
						fileName = fileName + "/C" + bval.clobFileCounter
								+ ".lob" + GenerateExtract.multiTables[id];
					}
				}

				if (bval.isXML[(ColIndex - 1)] != false) {
					if (this.filesizelimit < bval.xmlOffset) {
						bval.xmlFileCounter += 1;
						bval.xmlOffset = 0;
						if (this.xmlWriter != null) {
							this.xmlWriter.close();
						}
						if (GenerateExtract.multiTables[id] == 0) {
							fileName = fileName + "/X" + bval.xmlFileCounter
									+ ".lob";
						} else {
							fileName = fileName + "/X" + bval.xmlFileCounter
									+ ".lob" + GenerateExtract.multiTables[id];
						}
						this.xmlWriter = new BufferedOutputStream(
								new FileOutputStream(fileName));
					} else if (GenerateExtract.multiTables[id] == 0) {
						fileName = fileName + "/X" + bval.xmlFileCounter
								+ ".lob";
					} else {
						fileName = fileName + "/X" + bval.xmlFileCounter
								+ ".lob" + GenerateExtract.multiTables[id];
					}
				}

			}

			return fileName;
		}

		private void checkLOB(GenerateExtract.BlobVal bval, ResultSetMetaData md)
				throws SQLException {
			bval.isBLOB = new boolean[md.getColumnCount()];
			bval.isCLOB = new boolean[md.getColumnCount()];
			bval.isXML = new boolean[md.getColumnCount()];
			for (int i = 1; i <= md.getColumnCount(); i++) {
				int ij = i - 1;
				String tmp = md.getColumnTypeName(i).toUpperCase();
				int precision;
				try {
					precision = md.getPrecision(i);
				} catch (Exception e) {
					precision = 2147483647;
					GenerateExtract
							.log("Error: Precision found more than 2GB limiting it to 2GB");
				}
				bval.isCLOB[ij] = false;
				bval.isBLOB[ij] = false;
				bval.isXML[ij] = false;
				if ((GenerateExtract.dbSourceName.equalsIgnoreCase("oracle"))
						|| (GenerateExtract.dbSourceName
								.equalsIgnoreCase("db2"))) {
					bval.isCLOB[ij] = ((tmp.equalsIgnoreCase("CLOB"))
							|| (tmp.equalsIgnoreCase("LONG")) ? true : false);
					bval.isBLOB[ij] = (tmp.equalsIgnoreCase("BLOB") ? true
							: false);
					bval.isXML[ij] = ((tmp.equalsIgnoreCase("SYS.XMLTYPE"))
							|| (tmp.equalsIgnoreCase("XMLTYPE"))
							|| (tmp.equalsIgnoreCase("XML")) ? true : false);
				} else if (GenerateExtract.dbSourceName
						.equalsIgnoreCase("postgres")) {
					bval.isBLOB[ij] = ((tmp.equalsIgnoreCase("BYTEA"))
							|| (tmp.equalsIgnoreCase("TEXT")) ? true : false);
					bval.isCLOB[ij] = (tmp.equalsIgnoreCase("TEXT") ? true
							: false);
				} else if (GenerateExtract.dbSourceName
						.equalsIgnoreCase("sybase")) {
					bval.isBLOB[ij] = (tmp.equalsIgnoreCase("IMAGE") ? true
							: false);
					bval.isCLOB[ij] = ((tmp.equalsIgnoreCase("TEXT"))
							&& (GenerateExtract.mssqltexttoclob) ? true : false);
				} else if (GenerateExtract.dbSourceName
						.equalsIgnoreCase("mssql")) {
					bval.isXML[ij] = (tmp.equalsIgnoreCase("XML") ? true
							: false);
					if ((tmp.equalsIgnoreCase("IMAGE"))
							|| (tmp.equalsIgnoreCase("VARBINARY"))) {
						if (tmp.equalsIgnoreCase("VARBINARY")) {
							if (precision > 320000) {
								bval.isBLOB[ij] = true;
							}
						} else
							bval.isBLOB[ij] = true;
					}
					if ((!tmp.equalsIgnoreCase("TEXT"))
							&& (!tmp.equalsIgnoreCase("NTEXT")))
						continue;
					bval.isCLOB[ij] = (GenerateExtract.mssqltexttoclob ? true
							: false);
				} else if (GenerateExtract.dbSourceName
						.equalsIgnoreCase("mysql")) {
					if ((tmp.equalsIgnoreCase("BLOB"))
							|| (tmp.equalsIgnoreCase("MEDIUMBLOB"))
							|| (tmp.equalsIgnoreCase("LONGBLOB"))) {
						bval.isBLOB[ij] = true;
					} else {
						if ((!tmp.equalsIgnoreCase("VARCHAR"))
								|| (precision <= 32000))
							continue;
						bval.isCLOB[ij] = true;
					}
				} else if (GenerateExtract.dbSourceName
						.equalsIgnoreCase("hxtt")) {
					bval.isBLOB[ij] = (tmp.equalsIgnoreCase("OLE") ? true
							: false);
				} else if (GenerateExtract.dbSourceName
						.equalsIgnoreCase("access")) {
					bval.isBLOB[ij] = (tmp.equalsIgnoreCase("LONGBINARY"));
				} else {
					if (!GenerateExtract.dbSourceName
							.equalsIgnoreCase("domino"))
						continue;
					bval.isBLOB[ij] = ((tmp.equalsIgnoreCase("RICH TEXT")) && (precision > 15000));
				}
			}
			bval.isXml = (bval.isClob = bval.isBlob = false);
			for (int i = 1; i <= md.getColumnCount(); i++) {
				int ij = i - 1;
				if (bval.isBLOB[ij] != false) {
					bval.isBlob = true;
				}
				if (bval.isCLOB[ij] != false) {
					bval.isClob = true;
				}
				if (bval.isXML[ij] == false)
					continue;
				bval.isXml = true;
			}
		}

		private String pad(Object str, int padlen, String pad) {
			String padding = new String();
			int len = Math.abs(padlen) - str.toString().length();
			if (len < 1)
				return str.toString();
			for (int i = 0; i < len; i++) {
				padding = padding + pad;
			}
			return str + padding;
		}

		private String massageTS(String str) {
			int idx;
			if ((idx = str.indexOf('-')) < 5) {
				return pad(str, str.length() + 4 - idx, "0");
			}
			return str;
		}

		private byte[] getQuoteFixedinBytes(byte[] input, int len) {
			if (len == 0)
				return null;
			byte[] bytesWithQuote = new byte[len * 2 + 2];
			bytesWithQuote[0] = 34;
			int k = 1;
			for (int i = 0; i < len; i++) {
				bytesWithQuote[(k++)] = input[i];
				if (input[i] != 34)
					continue;
				bytesWithQuote[(k++)] = 34;
			}

			bytesWithQuote[(k++)] = 34;
			byte[] finalBytes = new byte[k];
			System.arraycopy(bytesWithQuote, 0, finalBytes, 0, k);
			return finalBytes;
		}

		private byte[] getBinaryString(InputStream data) throws Exception {
			byte[] tmpBytes = null;

			if (data != null) {
				int len = data.read(this.binaryData);

				if (len > 0) {
					tmpBytes = getQuoteFixedinBytes(this.binaryData, len);
				}
			} else {
				tmpBytes = null;
			}
			return tmpBytes;
		}

		private byte[] getColumnValue(int[] dataLen,
				GenerateExtract.BlobVal bval, int id, long numRow,
				ResultSet rs, ResultSetMetaData metaData, int colIndex)
				throws Exception {
			byte[] tmpBytes = null;
			String colType = "";
			String tmpStr = "";

			Blob blob = null;
			int ij = colIndex - 1;
			int blobLength = 0;
			byte[] lob = null;
			String lobFileName;
			try {
				colType = metaData.getColumnTypeName(colIndex).toUpperCase();
				String javaType = metaData.getColumnClassName(colIndex);

				if ((!GenerateExtract.dbSourceName.equalsIgnoreCase("oracle"))
						|| (!colType.equalsIgnoreCase("LONG"))) {
					tmpStr = rs.getString(colIndex);
				}
				if (tmpStr != null)
					dataLen[ij] = Math.max(tmpStr.length(), dataLen[ij]);
				if (GenerateExtract.dbSourceName.equalsIgnoreCase("oracle")) {
					if (colType.startsWith("TIMESTAMP")) {
						Timestamp tst = rs.getTimestamp(colIndex);
						if (tst != null) {
							tmpStr = tst.toString();
							if (tmpStr != null) {
								tmpStr = massageTS(tmpStr);
							}
						} else {
							tmpStr = null;
						}
					} else if (colType.equalsIgnoreCase("LONG")) {
						GenerateExtract.DataMap map = new GenerateExtract.DataMap();
						map.sourceDataType = colType;
						getDB2Type(map, id);
						if (map.db2DataType.startsWith("CLOB")) {
							tmpStr = "";
							bval.isCLOB[(colIndex - 1)] = true;
						} else {
							Reader input = rs.getCharacterStream(colIndex);
							char[] buffer = new char[1024000];
							int charRead = 0;
							StringBuffer dataBuffer = new StringBuffer();
							while ((charRead = input.read(buffer)) != -1) {
								tmpStr = "";
								blobLength += charRead;
								if (charRead == 1024000) {
									tmpStr = new String(buffer);
								} else
									tmpStr = new String(buffer, 0, charRead);

								dataBuffer.append(tmpStr);
							}
						}
					} else if (colType.equalsIgnoreCase("DATE")) {
						tmpStr = rs.getString(colIndex);
						if (tmpStr != null) {
							tmpStr = massageTS(tmpStr);
						}

					} else if ((colType.equals("BLOB"))
							|| (colType.equals("CLOB"))
							|| (colType.equals("SYS.XMLTYPE"))
							|| (colType.equalsIgnoreCase("XMLTYPE"))) {
						tmpStr = "";
					} else if (colType.equalsIgnoreCase("RAW")) {
						InputStream data = rs.getBinaryStream(colIndex);
						tmpBytes = getBinaryString(data);
						if (tmpBytes == null)
							tmpStr = null;
					} else {
						tmpStr = rs.getString(colIndex);
					}

				} else if (GenerateExtract.dbSourceName
						.equalsIgnoreCase("postgres")) {
					if (colType.equalsIgnoreCase("bool")) {
						tmpStr = rs.getString(colIndex);
						if (tmpStr != null) {
							if ((tmpStr.equalsIgnoreCase("t"))
									|| (tmpStr.equalsIgnoreCase("true"))
									|| (tmpStr.equalsIgnoreCase("yes"))
									|| (tmpStr.equalsIgnoreCase("1"))
									|| (tmpStr.equalsIgnoreCase("y"))) {
								tmpStr = "1";
							} else {
								tmpStr = "0";
							}
						}
					} else {
						tmpStr = rs.getString(colIndex);
					}
				} else if ((GenerateExtract.dbSourceName
						.equalsIgnoreCase("sybase"))
						|| (GenerateExtract.dbSourceName
								.equalsIgnoreCase("mssql"))) {
					if ((colType.equalsIgnoreCase("BINARY"))
							|| (colType.equalsIgnoreCase("VARBINARY"))) {
						InputStream data = rs.getBinaryStream(colIndex);
						tmpBytes = getBinaryString(data);
						if (tmpBytes == null)
							tmpStr = null;
					} else if (colType.equalsIgnoreCase("UNIQUEIDENTIFIER")) {
						tmpBytes = rs.getBytes(colIndex);
						if (tmpBytes != null) {
							int len = tmpBytes.length;
							if (len > 0) {
								tmpBytes = getQuoteFixedinBytes(tmpBytes, len);
							}
						}
					} else {
						tmpStr = rs.getString(colIndex);
					}
				} else if (GenerateExtract.dbSourceName
						.equalsIgnoreCase("mysql")) {
					if ((colType.equals("YEAR"))
							&& (javaType.equals("java.sql.Date"))) {
						java.sql.Date tmpDate = rs.getDate(colIndex);
						if (tmpDate != null) {
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
							tmpStr = sdf.format(tmpDate);
						} else {
							tmpStr = null;
						}
					} else {
						tmpStr = rs.getString(colIndex);
					}
				} else if (GenerateExtract.dbSourceName
						.equalsIgnoreCase("domino")) {
					Object field = null;
					try {
						field = rs.getObject(colIndex);
					} catch (SQLException qex) {
						if (qex.getErrorCode() == 23316) {
							field = rs.getString(colIndex);
						} else {
							field = "(null)";
						}
					}
					tmpStr = field == null ? null : field.toString();
				} else if ((!GenerateExtract.dbSourceName
						.equalsIgnoreCase("access"))
						|| (!colType.equals("LONGBINARY"))) {
					if (javaType.equalsIgnoreCase("byte[]")) {
						InputStream data = rs.getBinaryStream(colIndex);
						tmpBytes = getBinaryString(data);
						if (tmpBytes == null)
							tmpStr = null;
					} else {
						tmpStr = rs.getString(colIndex);
					}
				}

				if (tmpStr == null)
					return null;
				if ((bval.isCLOB[ij] == false) && (bval.isBLOB[ij] == false)
						&& (bval.isXML[ij] == false)) {
					if (javaType != null) {
						if (javaType.equalsIgnoreCase("java.lang.String")) {
							if (tmpStr != null) {
								tmpStr = tmpStr.replace("\"", "\"\"");
								if (GenerateExtract.trimTrailingSpaces)
									tmpStr = tmpStr.replaceAll("\\s+$", "");
								tmpStr = "\"" + tmpStr + "\"";
							}
						} else if ((colType.equalsIgnoreCase("TIMESTAMPTZ"))
								|| (colType.equalsIgnoreCase("TIMESTAMPLTZ"))) {
							if (tmpStr != null) {
								tmpStr = convertTSTZ(tmpStr);
							}
						} else if ((javaType.equalsIgnoreCase("java.sql.Time"))
								&& (colType.equalsIgnoreCase("TIMETZ"))) {
							if (tmpStr != null) {
								tmpStr = convertTimeTZ(tmpStr);
							}
						}
					}
					if (tmpBytes == null) {
						return tmpStr.getBytes(GenerateExtract.encoding);
					}
					return tmpBytes;
				}

				lobFileName = getLobFileName(bval, id, colIndex, null);
				if ((GenerateExtract.dbSourceName.equalsIgnoreCase("oracle"))
						|| (GenerateExtract.dbSourceName
								.equalsIgnoreCase("zdb2"))
						|| (GenerateExtract.dbSourceName
								.equalsIgnoreCase("db2"))) {
					Object obj = rs.getObject(colIndex);
					if (obj != null) {
						if (colType.equalsIgnoreCase("BLOB")) {
							blob = (Blob) obj;
							InputStream input = blob.getBinaryStream();
							byte[] buffer = new byte[1024000];
							int bytesRead = 0;
							while ((bytesRead = input.read(buffer)) != -1) {
								blobLength += bytesRead;
								this.blobWriter.write(buffer, 0, bytesRead);
							}
						} else if ((colType.equalsIgnoreCase("CLOB"))
								|| (colType.equalsIgnoreCase("XML"))) {
							String buf = null;
							Reader input = rs.getCharacterStream(colIndex);
							char[] buffer = new char[1024000];
							int charRead = 0;
							while ((charRead = input.read(buffer)) != -1) {
								buf = "";
								if (charRead == 1024000) {
									buf = new String(buffer);
								} else
									buf = new String(buffer, 0, charRead);

								lob = buf.getBytes(GenerateExtract.encoding);
								blobLength += lob.length;
								if (colType.equalsIgnoreCase("XML")) {
									this.xmlWriter.write(lob);
									continue;
								}
								this.clobWriter.write(lob);
							}
						} else if (colType.equalsIgnoreCase("LONG")) {
							String buf = obj.toString();
							lob = buf.getBytes(GenerateExtract.encoding);
							blobLength += lob.length;
							this.clobWriter.write(lob);
						} else if ((colType.equalsIgnoreCase("SYS.XMLTYPE"))
								|| (colType.equalsIgnoreCase("XMLTYPE"))) {
							bval.isXml = true;
							String buf = null;
							try {
								// XMLType xmlObj = (XMLType) obj;
								// buf = xmlObj.getStringVal();
								// lob = buf.getBytes(GenerateExtract.encoding);
								// blobLength += lob.length;
								// this.xmlWriter.write(lob);
							} catch (Exception e) {
								e.printStackTrace();
							} catch (NoClassDefFoundError nc) {
								buf = "<ErrorNotice>Serious Error: Table "
										+ GenerateExtract.srcTableName[id]
										+ " has XML data and this could not be unloaded since "
										+ " you forgot to include xdb.jar and xmlparserv2.jar in the classpath.</ErrorNotice>";

								lob = buf.getBytes(GenerateExtract.encoding);
								blobLength += lob.length;
								this.xmlWriter.write(lob);
							}
						}
					}
				} else if ((GenerateExtract.dbSourceName
						.equalsIgnoreCase("sybase"))
						|| (GenerateExtract.dbSourceName
								.equalsIgnoreCase("mssql"))) {
					Object obj = rs.getObject(colIndex);
					if (obj != null) {
						if ((colType.equalsIgnoreCase("IMAGE"))
								|| (colType.equalsIgnoreCase("VARBINARY"))) {
							InputStream input = rs.getBinaryStream(colIndex);
							byte[] buffer = new byte[1024000];
							int bytesRead = 0;
							while ((bytesRead = input.read(buffer)) != -1) {
								blobLength += bytesRead;
								this.blobWriter.write(buffer, 0, bytesRead);
							}
						} else {
							if (colType.equalsIgnoreCase("XML"))
								bval.isXml = true;
							Reader input = rs.getCharacterStream(colIndex);
							char[] buffer = new char[1024000];
							int charRead = 0;
							while ((charRead = input.read(buffer)) != -1) {
								String buf = "";
								if (charRead == 1024000) {
									buf = new String(buffer);
								} else
									buf = new String(buffer, 0, charRead);

								lob = buf.getBytes(GenerateExtract.encoding);
								blobLength += lob.length;
								if (colType.equalsIgnoreCase("XML"))
									this.xmlWriter.write(lob);
								else
									this.clobWriter.write(lob);
							}
						}
					}
				} else if ((GenerateExtract.dbSourceName
						.equalsIgnoreCase("postgres"))
						|| (GenerateExtract.dbSourceName
								.equalsIgnoreCase("mysql"))
						|| (GenerateExtract.dbSourceName
								.equalsIgnoreCase("hxtt"))
						|| (GenerateExtract.dbSourceName
								.equalsIgnoreCase("access"))) {
					if (colType.endsWith("BLOB")) {
						InputStream input = rs.getBinaryStream(colIndex);
						byte[] buffer = new byte[1024000];
						int bytesRead = 0;
						while ((bytesRead = input.read(buffer)) != -1) {
							blobLength += bytesRead;
							this.blobWriter.write(buffer, 0, bytesRead);
						}
					} else {
						Reader input = rs.getCharacterStream(colIndex);
						char[] buffer = new char[1024000];
						int charRead = 0;
						while ((charRead = input.read(buffer)) != -1) {
							String buf = "";
							if (charRead == 1024000) {
								buf = new String(buffer);
							} else
								buf = new String(buffer, 0, charRead);

							lob = buf.getBytes(GenerateExtract.encoding);
							blobLength += lob.length;
							this.clobWriter.write(lob);
						}
					}
				} else if (GenerateExtract.dbSourceName
						.equalsIgnoreCase("domino")) {
					String buffer;
					try {
						buffer = rs.getString(colIndex);
					} catch (SQLException qex) {
						if (qex.getErrorCode() == 23316) {
							buffer = rs.getString(colIndex);
						} else {
							buffer = "(null)";
						}
					}
					if (buffer != null) {
						blobLength = buffer.length();
						this.blobWriter.write(buffer.getBytes(), 0, blobLength);
					}
				}
				if (GenerateExtract.lobsToFiles)
					this.blobWriter.close();
			} catch (SQLException e) {
				String err = e.getMessage();
				GenerateExtract.log("Row[" + numRow + "] Col[" + colIndex
						+ "] Error:" + err);
				if ((GenerateExtract.dbSourceName.equalsIgnoreCase("oracle"))
						&& (err.startsWith("Stream has already been closed")))
					;
				GenerateExtract
						.log("The above is a known Oracle JDBC Driver problem. Get the latest JDBC driver and hope that this problem goes away.");
				return "SkipThisRow".getBytes(GenerateExtract.encoding);
			}
			return lobColName(bval, blobLength, id, colIndex, lobFileName)
					.getBytes(GenerateExtract.encoding);
		}

		private String convertTSTZ(String ts) {
			String newts = "";
			String tz = "";
			String micro = "";

			Timestamp tst = null;
			try {
				newts = ts;
				int pos = ts.length() - 3;
				if ((ts.charAt(pos) == '-') || (ts.charAt(pos) == '+')) {
					newts = ts.substring(0, pos);
					tz = ts.substring(pos);
					if (tz.length() == 3)
						tz = tz + "00";
				} else {
					pos = ts.length();
				}
				int pos2 = newts.lastIndexOf('.');
				if (pos2 > 0) {
					micro = newts.substring(pos2);
					newts = newts.substring(0, pos2);
				}
				newts = newts + tz;
				SimpleDateFormat sdf;
				if (tz.equals("")) {
					sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
				} else {
					sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ",
							Locale.US);
				}
				java.util.Date date = sdf.parse(newts);
				tst = new Timestamp(date.getTime());
				newts = tst.toString();
				pos2 = newts.lastIndexOf('.');
				if (pos2 > 0) {
					newts = newts.substring(0, pos2);
				}
				newts = newts + micro;
			} catch (ParseException e) {
				newts = "";
				e.printStackTrace();
			}
			return newts;
		}

		private String convertTimeTZ(String ts) {
			String newts = "";
			String tz = "";
			String micro = "";

			Timestamp tst = null;
			try {
				newts = ts;
				int pos = ts.length() - 3;
				if ((ts.charAt(pos) == '-') || (ts.charAt(pos) == '+')) {
					newts = ts.substring(0, pos);
					tz = ts.substring(pos);
					if (tz.length() == 3)
						tz = tz + "00";
				} else {
					pos = ts.length();
				}
				int pos2 = newts.lastIndexOf('.');
				if (pos2 > 0) {
					micro = newts.substring(pos2);
					newts = newts.substring(0, pos2);
				}
				newts = newts + tz;
				SimpleDateFormat sdf;
				if (tz.equals("")) {
					sdf = new SimpleDateFormat("HH:mm:ss", Locale.US);
				} else {
					sdf = new SimpleDateFormat("HH:mm:ssZ", Locale.US);
				}
				java.util.Date date = sdf.parse(newts);
				tst = new Timestamp(date.getTime());
				newts = tst.toString();
				pos2 = newts.lastIndexOf('.');
				if (pos2 > 0) {
					newts = newts.substring(0, pos2);
				}
				newts = newts.substring(11) + micro;
			} catch (ParseException e) {
				newts = "";
				e.printStackTrace();
			}
			return newts;
		}

		private String getPDSName(String dsName, String dsPrefix) {
			if (dsName.equals("")) {
				dsName = ZFile.getFullyQualifiedDSN(dsPrefix);

				String pdsName = "" + GenerateExtract.suffix;
				int i = pdsName.length();
				while (i < 7) {
					pdsName = "0" + pdsName;
					i++;
				}
				return "'" + dsName + pdsName + "'";
			}
			return dsName;
		}

		private String allocTBLPDS(int id, String tblDSN, int spaceMult) {
			tblDSN = getPDSName(tblDSN, "TBLDATA."
					+ GenerateExtract.zdb2tableseries);
			try {
				if (ZFile.dsExists(tblDSN)) {
					try {
						ZFile.remove("//" + tblDSN);
					} catch (Exception e) {
						GenerateExtract.log("Close the dataset " + tblDSN
								+ " and try again.");
						System.exit(-1);
					}
				}

				if (!ZFile.dsExists(tblDSN)) {
					if (GenerateExtract.debug)
						GenerateExtract
								.log("Calling ZFile.bpxwdyn(ALLOC FI("
										+ this.dataDD
										+ ") DA("
										+ tblDSN
										+ ") LRECL(32756) DSORG(PS) RECFM(V,B) MAXVOL(60) "
										+ " REUSE NEW CATALOG MSG(WTP) BLKSIZE(32760) "
										+ (GenerateExtract.storclas
												.equalsIgnoreCase("none") ? ""
												: new StringBuilder()
														.append(" STORCLAS (")
														.append(
																GenerateExtract.storclas)
														.append(")").toString())
										+ " CYL SPACE("
										+ (int) (spaceMult * GenerateExtract.overAlloc)
										+ ","
										+ (int) (GenerateExtract.secondary * GenerateExtract.overAlloc)
										+ ")");

					ZFile
							.bpxwdyn("ALLOC FI("
									+ this.dataDD
									+ ") DA("
									+ tblDSN
									+ ") LRECL(32756) DSORG(PS) RECFM(V,B) MAXVOL(60) "
									+ " REUSE NEW CATALOG MSG(WTP) BLKSIZE(32760) "
									+ (GenerateExtract.storclas
											.equalsIgnoreCase("none") ? ""
											: new StringBuilder().append(
													" STORCLAS (").append(
													GenerateExtract.storclas)
													.append(")").toString())
									+ " CYL SPACE("
									+ (int) (spaceMult * GenerateExtract.overAlloc)
									+ ","
									+ (int) (GenerateExtract.secondary * GenerateExtract.overAlloc)
									+ ")");
				}

			} catch (Exception e) {
				if (GenerateExtract.debug)
					e.printStackTrace();
				spaceMult = Math.max((int) (0.95D * spaceMult), 1);
				GenerateExtract.log("Alloc failed (" + id
						+ "). Trying with new value  " + spaceMult);
				if (spaceMult > 10) {
					allocTBLPDS(id, tblDSN, spaceMult);
				} else {
					GenerateExtract.log("We are out of space. Aborting ... ");
					System.exit(-1);
				}
			}
			return tblDSN;
		}

		private String openTBLPSDataset(int id, int spaceMult) {
			String schema = GenerateExtract
					.removeQuote(GenerateExtract.schemaName[id].toLowerCase());
			String table = GenerateExtract
					.removeQuote(GenerateExtract.tableName[id].toLowerCase());
			String dataPS = allocTBLPDS(id, "", spaceMult);
			try {
				GenerateExtract.zfp[id] = new ZFile("//DD:" + this.dataDD,
						"wb,type=record,noseek");
				GenerateExtract.log(dataPS + " opened (" + id + ") space="
						+ spaceMult);
			} catch (Exception e) {
				GenerateExtract.log("Error occured opening PS Dataset (" + id
						+ ")= " + dataPS + " for Schema = " + schema
						+ " Table = " + table);
				e.printStackTrace();
				System.exit(-1);
			}
			return dataPS;
		}

		private void writeDataFile(int id, long recordCount, byte[] bytes)
				throws Exception {
			int rowSize = 0;

			if (GenerateExtract.dbTargetName.equals("db2luw")) {
				GenerateExtract.fp[id].write(bytes);
			} else {
				if (recordCount == 0L) {
					return;
				}
				rowSize = Math.max(rowSize, bytes.length);
				try {
					if (GenerateExtract.zfp[id] == null) {
						double mult = Math.floor(32756 / (rowSize + 4)) * 15.0D;
						this.spaceMult = (int) Math.ceil(recordCount / mult);
						this.spaceMult = Math
								.max(
										Math
												.min(
														this.spaceMult,
														(int) (4367.0D / GenerateExtract.overAlloc)),
										1);
						String dsName = openTBLPSDataset(id, this.spaceMult);
						GenerateExtract.log("Allocating new PS dataset "
								+ dsName + " table is "
								+ GenerateExtract.schemaName[id] + "."
								+ GenerateExtract.tableName[id]);
						this.rowNum = 0L;
						GenerateExtract.zOSDataSets[id] = dsName;
					}
					long maxRows = 32756 / (rowSize + 4);
					maxRows = (long) Math.floor(maxRows);
					maxRows = 15
							* (this.spaceMult + 15 * GenerateExtract.secondary)
							* maxRows;
					if (this.rowNum > maxRows) {
						GenerateExtract.zfp[id].close();
						String dsName = openTBLPSDataset(id, this.spaceMult);
						GenerateExtract.log("Rows = " + this.rowNum
								+ " Allocating a new PS dataset " + dsName
								+ " table is " + GenerateExtract.schemaName[id]
								+ "." + GenerateExtract.tableName[id]);
						this.rowNum = 0L;
						// GenerateExtract.zOSDataSets[id] = (GenerateExtract
						// .access$7500()[id]
						// + "," + dsName);
					}

					GenerateExtract.zfp[id].write(bytes);
					if (!GenerateExtract.debug)
						;
				} catch (Exception e) {
					if (GenerateExtract.debug)
						e.printStackTrace();
					GenerateExtract.zfp[id].close();
					String dsName = openTBLPSDataset(id, this.spaceMult);
					GenerateExtract.log(" rowSize=" + rowSize
							+ " We should never be in this exception. Rows = "
							+ this.rowNum + " Allocating a new PS dataset "
							+ dsName + " table is "
							+ GenerateExtract.schemaName[id] + "."
							+ GenerateExtract.tableName[id]);
					this.rowNum = 0L;
					// GenerateExtract.zOSDataSets[id] = (GenerateExtract.
					// .zOSDataSets()[id]
					// + "," + dsName);
					GenerateExtract.zfp[id].write(bytes);
				}
				this.rowNum += 1L;
			}
		}

		private long dumpData(int id) {
			long tableRows = 0L;

			GenerateExtract.BlobVal bval = new GenerateExtract.BlobVal();

			Statement statement = null;
			PreparedStatement queryStatement = null;
			ResultSet Reader = null;

			String sql = GenerateExtract.query[id];
			boolean skip = false;
			int colCount = 0;
			int numUniq = 1;

			long numRows = 0L;
			try {
				if (GenerateExtract.compressTable) {
					if ((GenerateExtract.releaseLevel == -1.0F)
							|| (GenerateExtract.releaseLevel < 9.0F)) {
						GenerateExtract
								.log("compressTable=true can not be used since database release = "
										+ GenerateExtract.releaseLevel);
					}
				}
				if (GenerateExtract.compressIndex) {
					if ((GenerateExtract.releaseLevel == -1.0F)
							|| (GenerateExtract.releaseLevel < 9.7F)) {
						GenerateExtract
								.log("compressIndex=true can not be used since database release = "
										+ GenerateExtract.releaseLevel);
					}
				}
				if (GenerateExtract.dbTargetName.equals("zdb2")) {
					if (GenerateExtract.dataUnload) {
						tableRows = countRows(id);
					}
				}
				ResultSetMetaData rsmtadta;
				if ((GenerateExtract.dbSourceName.equalsIgnoreCase("mysql"))
						|| (GenerateExtract.dbSourceName
								.equalsIgnoreCase("sybase"))
						|| (GenerateExtract.dbSourceName
								.equalsIgnoreCase("oracle"))) {
					statement = this.bladeConn.createStatement(1003, 1007);

					if (GenerateExtract.fetchSize == 0) {
						if (GenerateExtract.dbSourceName
								.equalsIgnoreCase("mysql")) {
							String newSQL = sql + " LIMIT 1";
							Reader = statement.executeQuery(newSQL);
							rsmtadta = Reader.getMetaData();
							checkLOB(bval, rsmtadta);
							colCount = rsmtadta.getColumnCount();
							Reader.close();
							statement.close();
							statement = this.bladeConn.createStatement(1003,
									1007);

							statement.setFetchSize(-2147483648);
							Reader = statement.executeQuery(sql);
						} else {
							Reader = statement.executeQuery(sql);
							rsmtadta = Reader.getMetaData();
							checkLOB(bval, rsmtadta);
							colCount = rsmtadta.getColumnCount();
						}
					} else {
						statement.setFetchSize(GenerateExtract.fetchSize);
						Reader = statement.executeQuery(sql);
						rsmtadta = Reader.getMetaData();
						checkLOB(bval, rsmtadta);
						colCount = rsmtadta.getColumnCount();
					}
				} else {
					queryStatement = this.bladeConn.prepareStatement(sql);
					queryStatement.setFetchSize(GenerateExtract.fetchSize);
					if (GenerateExtract.dataUnload)
						Reader = queryStatement.executeQuery();
					rsmtadta = queryStatement.getMetaData();
					checkLOB(bval, rsmtadta);
					colCount = rsmtadta.getColumnCount();
				}
				long last = System.currentTimeMillis();
				int[] dataLen = new int[colCount];
				this.blobWriter = (this.clobWriter = this.xmlWriter = null);
				if (GenerateExtract.dataUnload) {
					if (!GenerateExtract.lobsToFiles) {
						if (bval.isBlob)
							this.blobWriter = new BufferedOutputStream(
									new FileOutputStream(getLobFileName(bval,
											id, 0, "BLOB")));
						if (bval.isClob)
							this.clobWriter = new BufferedOutputStream(
									new FileOutputStream(getLobFileName(bval,
											id, 0, "CLOB")));
						if (bval.isXml)
							this.xmlWriter = new BufferedOutputStream(
									new FileOutputStream(getLobFileName(bval,
											id, 0, "XML")));
					}
					while (Reader.next()) {
						this.byteOutputStream.reset();
						skip = false;
						for (int i = 1; i <= colCount; i++) {
							byte[] tmp = getColumnValue(dataLen, bval, id,
									numRows, Reader, rsmtadta, i);
							if ((tmp != null)
									&& (tmp.toString().equals("SkipThisRow"))) {
								skip = true;
							} else {
								if (tmp != null) {
									this.byteOutputStream.write(tmp);
								}
								if (i == colCount)
									continue;
								this.byteOutputStream
										.write(GenerateExtract.colsep
												.getBytes(GenerateExtract.encoding));
							}
						}
						if (GenerateExtract.dbTargetName.equals("db2luw")) {
							this.byteOutputStream.write(GenerateExtract.linesep
									.getBytes(GenerateExtract.encoding));
						}
						numRows += 1L;
						if (!skip) {
							writeDataFile(id, tableRows, this.byteOutputStream
									.toByteArray());
						}
						if (numRows % 10000L == 0L) {
							long now = System.currentTimeMillis();
							DecimalFormat myFormatter = new DecimalFormat(
									"###.###");
							GenerateExtract
									.log(GenerateExtract
											.removeQuote(GenerateExtract.srcTableName[id])
											+ " 10000 rows unloaded in "
											+ myFormatter
													.format((now - last) / 1000.0D)
											+ " sec");
							last = now;
						}
						if (GenerateExtract.limitExtractRows
								.equalsIgnoreCase("ALL"))
							continue;
						try {
							int x = Integer
									.parseInt(GenerateExtract.limitExtractRows);
							if ((x >= 0) && (numRows >= x)) {
								break;
							}
						} catch (Exception s) {
						}
					}
					if (GenerateExtract.dbTargetName.equals("zdb2")) {
						if (GenerateExtract.zfp[id] != null)
							GenerateExtract.zfp[id].close();
					}
					if (!GenerateExtract.lobsToFiles) {
						if (this.blobWriter != null)
							this.blobWriter.close();
						if (this.clobWriter != null)
							this.clobWriter.close();
						if (this.xmlWriter != null)
							this.xmlWriter.close();
					}
				}
				if (GenerateExtract.dbSourceName.equalsIgnoreCase("mysql")) {
					if (Reader != null)
						Reader.close();
					if (queryStatement != null)
						queryStatement.close();
					if (statement != null)
						statement.close();
				}
				this.bladeConn.commit();
				if (!GenerateExtract.dbSourceName.equalsIgnoreCase("mysql")) {
					this.bladeConn.setAutoCommit(true);
				}
				if (GenerateExtract.ddlGen) {
					if (GenerateExtract.multiTables[id] == 0) {
						if (!GenerateExtract.dbSourceName
								.equalsIgnoreCase("domino")) {
							genDB2CheckConstraints(id);
							genDB2Fkeys(id, rsmtadta);
							genDB2TableKeys(id);
							checkUniqueConstraint(id);
						}
						genDB2TableScript(dataLen, rsmtadta, id, numRows);
					}
				}
				if (GenerateExtract.dataUnload) {
					if (GenerateExtract.multiTables[id] == 0) {
						if (GenerateExtract.dbTargetName.equals("db2luw"))
							genDB2LoadScript(dataLen, bval, rsmtadta, id);
						else
							genzDB2LoadScript(id, rsmtadta);
					}
				}
				if (!GenerateExtract.dbSourceName.equalsIgnoreCase("domino"))
					this.bladeConn.setAutoCommit(false);
				numUniq++;
			} catch (SQLException qex) {
				GenerateExtract.log("SQL=" + sql);
				GenerateExtract.log("exception unloading data Code="
						+ qex.getErrorCode() + " Message=" + qex.getMessage());
				qex.printStackTrace();
				try {
					this.bladeConn.rollback();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			} catch (Exception e) {
				GenerateExtract.log("SQL=" + sql);
				GenerateExtract.log("exception unloading data Message="
						+ e.getMessage());
				e.printStackTrace();
				try {
					this.bladeConn.rollback();
				} catch (SQLException qex) {
					qex.printStackTrace();
				}
			} finally {
				try {
					if (Reader != null)
						Reader.close();
					if (queryStatement != null)
						queryStatement.close();
					if (statement != null)
						statement.close();
				} catch (Exception e) {
					GenerateExtract.log("close() error " + e.getMessage());
				}
			}
			return numRows;
		}
	}

	public class DataMap {
		public boolean varlength = false;
		public boolean useActualData = false;
		public String db2DataType = "";
		public String sourceDataType = "";
		public int defaultlength;

		public DataMap() {
		}
	}

	public class BlobVal {
		public boolean[] isBLOB;
		public boolean[] isCLOB;
		public boolean[] isXML;
		boolean isBlob;
		boolean isClob;
		boolean isXml;
		public int blobOffset;
		public int xmlOffset;
		public int clobOffset;
		public int blobFileCounter;
		public int clobFileCounter;
		public int xmlFileCounter;

		public BlobVal() {
			this.blobOffset = 0;
			this.clobOffset = 0;
			this.xmlOffset = 0;
			this.blobFileCounter = 0;
			this.clobFileCounter = 0;
			this.xmlFileCounter = 0;
			this.isBlob = false;
			this.isXml = false;
			this.isClob = false;
		}
	}
}