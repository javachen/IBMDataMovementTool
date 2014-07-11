package ibm;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.JarURLConnection;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.text.StringCharacterIterator;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.jdbc.driver.OracleConnection;

import com.ibm.db2.jcc.DB2Diagnosable;
import com.ibm.db2.jcc.DB2Sqlca;

public class IBMExtractUtilities {
	private static final SimpleDateFormat timestampFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH.mm.ss.SSS");
	private static String URL_PROP_FILE = "url.properties";
	private static String DRIVER_PROP_FILE = "driver.properties";
	private static String SCHEMA_EXCLUDE_FILE = "SchemaExcludeList.properties";
	private static String DEPLOY_FILES_FILE = "DeployFiles.properties";
	private static String DEPLOYED_OBJECT_FILE = "DeployedObjects.properties";
	private static String MAP_COMPATIBILITY_FILES = "compatibilityFiles.properties";
	private static Properties mapDeployFiles = null;
	private static Properties mapExcludeList = null;
	private static Properties deployedObjectsList = new Properties();
	private static Properties propURL = null;
	private static Properties propDrivers = null;
	private static Properties mapCompatibilityFiles = null;
	public static String linesep = System.getProperty("line.separator");
	private static final Class[] parameters = { URL.class };
	public static String osType = System.getProperty("os.name").toUpperCase()
			.startsWith("Z/OS") ? "z/OS" : System.getProperty("os.name")
			.toUpperCase().startsWith("WIN") ? "WIN" : "OTHER";
	public static String filesep = System.getProperty("file.separator");
	public static String SQLCode = "";
	public static String Message = "";
	public static String DB2Path = "";
	public static String InstanceName = "";
	public static String Varchar2_Compat = "";
	public static String Date_Compat = "";
	public static String Number_Compat = "";
	public static String Decflt_rounding = "";
	public static String FailedLine = "";
	public static String ReleaseLevel = "";
	public static boolean DataExtracted = false;
	public static boolean DeployCompleted = false;
	public static boolean ScriptExecutionCompleted = false;
	public static boolean DB2Compatibility = false;
	public static boolean db2ScriptCompleted = false;

	public static void log(String msg) {
		if (osType.equals("z/OS")) {
			System.out.println(timestampFormat.format(new Date()) + ":" + msg);
		} else
			System.out.println("[" + timestampFormat.format(new Date()) + "] "
					+ msg);
	}

	public static String removeQuote(String name) {
		if ((name == null) || (name.length() == 0))
			return name;
		int len = name.length();
		if ((name.charAt(0) == '"') && (len > 2)) {
			return name.substring(1, len - 1);
		}
		return name;
	}

	public static String putQuote(String name) {
		if ((name == null) || (name.length() == 0))
			return name;
		if (name.charAt(0) == '"')
			return name;
		return "\"" + name + "\"";
	}

	public static void copyFile(File in, File out) throws IOException {
		FileChannel inChannel = new FileInputStream(in).getChannel();
		FileChannel outChannel = new FileOutputStream(out).getChannel();
		try {
			inChannel.transferTo(0L, inChannel.size(), outChannel);
		} catch (IOException e) {
			throw e;
		} finally {
			if (inChannel != null)
				inChannel.close();
			if (outChannel != null)
				outChannel.close();
		}
	}

	public static String FileContents(String fp) throws IOException {
		BufferedReader in = new BufferedReader(new FileReader(fp));

		StringBuffer buffer = new StringBuffer();
		String line;
		while ((line = in.readLine()) != null) {
			if ((line.startsWith("DB21034E"))
					|| (line.startsWith("valid Command Line Processor command"))
					|| (line.equals(""))) {
				continue;
			}
			buffer.append(line + "\n");
		}

		in.close();
		return buffer.toString();
	}

	public static String getStringName(String name, String suffix) {
		if ((name == null) || (name.length() == 0))
			return name;
		if (name.charAt(name.length() - 1) == '"') {
			return name.substring(0, name.length() - 1) + suffix + "\"";
		}
		return name + suffix;
	}

	public static String escapeUnixChar(String strToEscape) {
		StringCharacterIterator charIter = new StringCharacterIterator(
				strToEscape);
		StringBuilder buf = new StringBuilder();
		char ch = charIter.current();
		while (ch != 65535) {
			if (ch == ';')
				buf.append("\\;");
			else if (ch == '&')
				buf.append("\\&");
			else if (ch == '(')
				buf.append("\\(");
			else if (ch == ')')
				buf.append("\\)");
			else if (ch == '|')
				buf.append("\\|");
			else if (ch == '*')
				buf.append("\\*");
			else if (ch == '?')
				buf.append("\\?");
			else if (ch == '[')
				buf.append("\\[");
			else if (ch == ']')
				buf.append("\\]");
			else if (ch == '~')
				buf.append("\\~");
			else if (ch == '{')
				buf.append("\\{");
			else if (ch == '}')
				buf.append("\\}");
			else if (ch == '>')
				buf.append("\\>");
			else if (ch == '<')
				buf.append("\\<");
			else if (ch == '^')
				buf.append("\\^");
			else if (ch == '"')
				buf.append("\\\"");
			else if (ch == '$')
				buf.append("\\$");
			else
				buf.append(ch);
			ch = charIter.next();
		}
		return buf.toString();
	}

	public static String FixSpecialChars(String name) {
		char[] specialChars = { '/', '\\', ':', '>', '<', '"', '*', '?', '|' };
		char[] replaceChars = { '_', '-', 'Z', 'G', 'L', 'D', 'S', 'Q', 'P' };
		for (int i = 0; i < specialChars.length; i++) {
			int pos = name.indexOf(specialChars[i]);
			if (pos > 0)
				name = name.replace(specialChars[i], replaceChars[i]);
		}
		return name;
	}

	private static String convertToHex(byte[] data) {
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < data.length; i++) {
			int halfbyte = data[i] >> 4 & 0xF;
			int two_halfs = 0;
			do {
				if ((0 <= halfbyte) && (halfbyte <= 9))
					buf.append((char) (48 + halfbyte));
				else
					buf.append((char) (97 + (halfbyte - 10)));
				halfbyte = data[i] & 0xF;
			} while (two_halfs++ < 1);
		}
		return buf.toString();
	}

	public static String MD5(String text) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] md5hash = new byte[32];
			md.update(text.getBytes("iso-8859-1"), 0, text.length());
			md5hash = md.digest();
			return convertToHex(md5hash);
		} catch (Exception e) {
		}
		return null;
	}

	public static String putQuote(String strToConvert, String sep) {
		String targetString = "";
		String[] tmp = strToConvert.split(sep);
		for (int i = 0; i < tmp.length; i++) {
			tmp[i] = (tmp[i].contains(" ") ? "\"" + tmp[i] + "\"" : tmp[i]);
			if (i > 0)
				targetString = targetString + sep;
			targetString = targetString + tmp[i];
		}
		return targetString;
	}

	public static void AddURL(URL u) throws IOException {
		URLClassLoader sysloader = (URLClassLoader) ClassLoader
				.getSystemClassLoader();
		Class sysclass = URLClassLoader.class;
		try {
			Method method = sysclass.getDeclaredMethod("addURL", parameters);
			method.setAccessible(true);
			method.invoke(sysloader, new Object[] { u });
		} catch (Throwable t) {
			t.printStackTrace();
			throw new IOException(
					"Error, could not add URL to system classloader");
		}
	}

	public static void AddFile(File f) throws IOException {
		AddURL(f.toURL());
	}

	public static boolean FileExists(String fileName) {
		File f = new File(fileName);
		return f.exists();
	}

	public static void getStringChunks(ResultSet rs, int colIndex,
			StringBuffer buf) {
		char[] buffer = new char[1024000];
		int charRead = 0;
		try {
			Reader input = rs.getCharacterStream(colIndex);
			while ((input != null) && ((charRead = input.read(buffer)) != -1)) {
				buf.append(buffer, 0, charRead);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static String getDriverName(String dbSourceName) {
		try {
			if (propDrivers == null) {
				propDrivers = new Properties();
				InputStream istream = ClassLoader
						.getSystemResourceAsStream(DRIVER_PROP_FILE);
				if (istream == null) {
					FileInputStream finStream = new FileInputStream(
							DRIVER_PROP_FILE);
					propDrivers.load(finStream);
					finStream.close();
				} else {
					propDrivers.load(istream);
					istream.close();
				}
				log("Configuration file loaded: '" + DRIVER_PROP_FILE + "'");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		String driverName = propDrivers.getProperty(dbSourceName.toLowerCase());
		return driverName;
	}

	public static String readJarFile(String aboutFile) {
		try {
			BufferedReader breader = null;
			File f = new File(aboutFile);
			StringBuffer buffer = new StringBuffer();
			if (!f.exists()) {
				InputStream istream = ClassLoader
						.getSystemResourceAsStream(aboutFile);
				breader = new BufferedReader(new InputStreamReader(istream));
			} else {
				breader = new BufferedReader(new FileReader(f));
			}
			String line;
			while ((line = breader.readLine()) != null) {
				buffer.append(line + linesep);
			}
			breader.close();
			return buffer.toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}

	public static String getURL(String dbSourceName, String server, int port,
			String dbName) {
		String url = "";
		try {
			if (propURL == null) {
				propURL = new Properties();
				InputStream istream = ClassLoader
						.getSystemResourceAsStream(URL_PROP_FILE);
				if (istream == null) {
					FileInputStream finStream = new FileInputStream(
							URL_PROP_FILE);
					propURL.load(finStream);
					finStream.close();
				} else {
					propURL.load(istream);
					istream.close();
				}
				log("Configuration file loaded: '" + URL_PROP_FILE + "'"
						+ linesep);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (dbSourceName.equalsIgnoreCase("oracle")) {
			url = (String) propURL.get(dbSourceName) + server + ":" + port
					+ ":" + dbName;
		} else if (dbSourceName.equalsIgnoreCase("mssql")) {
			url = (String) propURL.get(dbSourceName) + server + ":" + port
					+ ";database=" + dbName;
		} else if ((dbSourceName.equalsIgnoreCase("access"))
				|| (dbSourceName.equalsIgnoreCase("hxtt"))
				|| (dbSourceName.equalsIgnoreCase("domino"))) {
			url = (String) propURL.get(dbSourceName) + server;
		} else if (dbSourceName.equalsIgnoreCase("mysql")) {
			url = (String) propURL.get(dbSourceName) + server + ":" + port
					+ "/" + dbName + "?zeroDateTimeBehavior=round";
		} else if (dbSourceName.equalsIgnoreCase("zdb2")) {
			url = (String) propURL.get(dbSourceName)
					+ server
					+ ":"
					+ port
					+ "/"
					+ dbName
					+ ":retrieveMessagesFromServerOnGetMessage=true;emulateParameterMetaDataForZCalls=1;";
		} else if (dbSourceName.equalsIgnoreCase("idb2")) {
			url = (String) propURL.get(dbSourceName) + server + ":" + port
					+ "/" + dbName + ";date format=iso";
		} else if (dbSourceName.equalsIgnoreCase("sybase")) {
			url = (String) propURL.get(dbSourceName) + server + ":" + port
					+ "/" + dbName;
		} else {
			url = (String) propURL.get(dbSourceName) + server + ":" + port
					+ "/" + dbName;
		}
		return url;
	}

	public static String db2ScriptName(String outputDir, boolean ddlGen,
			boolean dataUnload) {
		String scriptName = "";
		String ext = "";
		if (osType.equals("WIN")) {
			ext = ".cmd";
		} else {
			ext = ".sh";
		}
		if ((ddlGen) && (dataUnload)) {
			scriptName = outputDir + filesep + "db2gen" + ext;
		} else if ((ddlGen) && (!dataUnload)) {
			scriptName = outputDir + filesep + "db2ddl" + ext;
		} else {
			scriptName = outputDir + filesep + "db2load" + ext;
		}
		return scriptName;
	}

	private static String getDB2PathName(Connection conn) {
		String result = "";
		try {
			String sql = "select reg_var_value from sysibmadm.reg_variables where reg_var_name = 'DB2PATH'";
			PreparedStatement partStatement = conn.prepareStatement(sql);
			ResultSet rs = partStatement.executeQuery();
			while (rs.next()) {
				result = rs.getString(1);
			}
			if (rs != null)
				rs.close();
			if (partStatement != null)
				partStatement.close();
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
			String userHome = System.getProperty("user.home");
			if (FileExists(userHome + "/sqllib")) {
				result = userHome + "/sqllib/java";
			} else
				result = "";
		}
		log("DB2 PATH is " + result);
		return result;
	}

	private static void getCompatibilityParams(Connection conn) {
		Varchar2_Compat = "";
		Date_Compat = "";
		Number_Compat = "";
		try {
			String sql = "select a.value, b.value, c.value, d.value from sysibmadm.dbcfg a, sysibmadm.dbcfg b, sysibmadm.dbcfg c, sysibmadm.dbcfg d where a.name = 'varchar2_compat' and b.name = 'date_compat' and c.name = 'number_compat'and d.name = 'decflt_rounding'";

			PreparedStatement partStatement = conn.prepareStatement(sql);
			ResultSet rs = partStatement.executeQuery();
			while (rs.next()) {
				Varchar2_Compat = rs.getString(1);
				Date_Compat = rs.getString(2);
				Number_Compat = rs.getString(3);
				Decflt_rounding = rs.getString(4);
			}
			if (rs != null)
				rs.close();
			if (partStatement != null)
				partStatement.close();
			conn.commit();
		} catch (Exception e) {
			Varchar2_Compat = "";
			Date_Compat = "";
			Number_Compat = "";
			Decflt_rounding = "";
			e.printStackTrace();
		}
		log("DB2 Compatibility params varchar2_compat=" + Varchar2_Compat
				+ " date_compat=" + Date_Compat + " number_compat="
				+ Number_Compat + " Decflt_rounding=" + Decflt_rounding);
	}

	private static String getDB2ReleaseLevel(Connection conn) {
		String result = "";
		try {
			String sql = "select prod_release from sysibmadm.env_prod_info fetch first row only";
			PreparedStatement partStatement = conn.prepareStatement(sql);
			ResultSet rs = partStatement.executeQuery();
			while (rs.next()) {
				result = rs.getString(1);
			}
			if (rs != null)
				rs.close();
			if (partStatement != null)
				partStatement.close();
			conn.commit();
		} catch (Exception e) {
			result = "";
		}
		log("DB2 Release Level is " + result);
		return result;
	}

	private static String getInstanceName(Connection conn) {
		String result = "";
		try {
			String sql = "select inst_name from sysibmadm.env_inst_info fetch first row only";
			PreparedStatement partStatement = conn.prepareStatement(sql);
			ResultSet rs = partStatement.executeQuery();
			while (rs.next()) {
				result = rs.getString(1);
			}
			if (rs != null)
				rs.close();
			if (partStatement != null)
				partStatement.close();
			conn.commit();
		} catch (Exception e) {
			result = "";
			e.printStackTrace();
		}
		log("DB2 instance name is " + result);
		return result;
	}

	public static void CloseConnection(Connection mainConn) {
		try {
			mainConn.commit();
			mainConn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static String removeLineChar(String input) {
		String patternStr = "(?m)$^|[\\r\\n|\\r|\\n]+";
		String replaceStr = " ";
		Pattern pattern = Pattern.compile(patternStr);
		Matcher matcher = pattern.matcher(input);
		replaceStr = matcher.replaceAll(" ");
		return replaceStr;
	}

	public static String getDBVersion(String dbSourceName, String server,
			int port, String dbName, String userid, String pwd) {
		StringBuffer sb = new StringBuffer();
		Connection mainConn = null;
		String driverName = getDriverName(dbSourceName);
		String sql = null;

		if (dbSourceName.equals("oracle")) {
			sql = "SELECT * FROM V$VERSION";
		} else if (dbSourceName.equals("db2")) {
			sql = "SELECT service_level||'(FP'||fixpack_num||')' FROM TABLE (sysproc.env_get_inst_info()) as x";
		} else if ((dbSourceName.equals("sybase"))
				|| (dbSourceName.equals("mssql"))) {
			sql = "SELECT @@VERSION";
		} else if ((dbSourceName.equals("mysql"))
				|| (dbSourceName.equals("postgres"))) {
			sql = "SELECT VERSION()";
		} else if (dbSourceName.equals("zdb2")) {
			sql = "";
		}

		if ((sql == null) || (sql.length() == 0)) {
			return "Unknown Version for " + dbSourceName;
		}
		try {
			try {
				Class.forName(driverName).newInstance();
			} catch (Exception e) {
				e.printStackTrace();
				Message = "Driver could not be loaded. See console's output.";
				return null;
			}
			String url = getURL(dbSourceName, server, port, dbName);
			if (dbSourceName.equalsIgnoreCase("domino")) {
				mainConn = DriverManager.getConnection(url);
			} else {
				mainConn = DriverManager.getConnection(url, userid, pwd);
				mainConn.setAutoCommit(true);
			}
			try {
				PreparedStatement stat = mainConn.prepareStatement(sql);
				ResultSet Reader = stat.executeQuery();
				while (Reader.next()) {
					sb.append(Reader.getString(1) + linesep);
				}
				if (Reader != null)
					Reader.close();
				if (stat != null)
					stat.close();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
			mainConn.close();
		} catch (SQLException e) {
			log("Error for : " + driverName + " for " + dbSourceName
					+ " Error Message :" + e.getMessage());
			System.exit(-1);
		}
		return sb.toString();
	}

	public static Connection OpenConnection(String dbSourceName, String server,
			int port, String dbName, String userid, String pwd) {
		Connection mainConn = null;
		Message = "";
		String driverName = getDriverName(dbSourceName);
		try {
			try {
				Class.forName(driverName).newInstance();
			} catch (Exception e) {
				e.printStackTrace();
				Message = "Driver could not be loaded. See console's output.";
				return null;
			}
			log("Driver " + driverName + " loaded");
			String url = getURL(dbSourceName, server, port, dbName);
			if (dbSourceName.equalsIgnoreCase("domino")) {
				mainConn = DriverManager.getConnection(url);
			} else {
				mainConn = DriverManager.getConnection(url, userid, pwd);
				mainConn.setAutoCommit(false);
				if (dbSourceName.equalsIgnoreCase("db2")) {
					DB2Path = getDB2PathName(mainConn);
					InstanceName = getInstanceName(mainConn);
					getCompatibilityParams(mainConn);
				}
			}
			Message = "Connection to " + dbSourceName + " succeeded.";
			log(Message);
		} catch (SQLException e) {
			if (e.getErrorCode() == -4499) {
				if (e.getMessage().contains(
						"An attempt was made to access a database"))
					Message = "Connection succeeded but database not found";
				else if (e.getMessage().contains("Error opening socket"))
					Message = "Error opening socket to DB2 Server. Check if DB2 instance is up. If yes, check check port number.";
				else
					Message = "Error connecting to DB2 server";
				log("Error Message : " + e.getMessage());
			} else if (e.getErrorCode() == -4214) {
				Message = "Connection succeeded but userid/password is incorrect.";
			} else {
				if (e.getMessage().contains("User ID or Password invalid"))
					Message = "Error connecting. User ID or Password invalid";
				else if (e
						.getMessage()
						.contains(
								"The Network Adapter could not establish the connection"))
					Message = "The Network Adapter could not establish the connection";
				else
					Message = dbSourceName
							+ " JDBC connection problem. Please see console's output.";
				log("Error connecting : " + driverName + " for " + dbSourceName
						+ " Error Message :" + e.getMessage());
			}
			log(Message);
			e.printStackTrace();
			return null;
		}
		return mainConn;
	}

	public static boolean isValidValue(String value, String validValues) {
		if (validValues == null)
			return true;
		String[] valids = validValues.split(",");
		for (int i = 0; i < valids.length; i++) {
			if (value.equalsIgnoreCase(valids[i]))
				return true;
		}
		return false;
	}

	public static boolean DeleteDir(File dir) {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				boolean success = DeleteDir(new File(dir, children[i]));
				if (!success) {
					return false;
				}
			}
		}
		return dir.delete();
	}

	public static String getSQLMessage(Connection conn, String code,
			String message) {
		StringBuffer sb = new StringBuffer();
		String sqlCode = "SQL" + Math.abs(Integer.valueOf(code).intValue());
		String sql = "VALUES (SYSPROC.SQLERRM('" + sqlCode + "','" + message
				+ "',';','en_US',1))";
		ResultSet rs = null;
		try {
			PreparedStatement partStatement = conn.prepareStatement(sql);
			rs = partStatement.executeQuery();
			while (rs.next()) {
				sb.append(rs.getString(1));
			}
			if (rs != null)
				rs.close();
			if (partStatement != null) {
				partStatement.close();
			}
			conn.commit();

			sb.append(linesep + linesep);
			sql = "VALUES (SYSPROC.SQLERRM('" + sqlCode + "','','','en_US',0))";
			partStatement = conn.prepareStatement(sql);
			rs = partStatement.executeQuery();
			while (rs.next()) {
				sb.append(rs.getString(1));
			}
			if (rs != null)
				rs.close();
			if (partStatement != null)
				partStatement.close();
		} catch (SQLException e) {
			log("SYSPROC.SQLERRM Error:" + e.getMessage());
			return message;
		}
		return sb.toString();
	}

	public static void DeployObject(Connection conn, String outputDirectory,
			String type, String schema, String objectName, String sql) {
		String sqlerrmc = "";
		int lineNumber = -1;
		if (conn == null) {
			Message = "Connection does not exist.";
			return;
		}
		Message = IBMExtractUtilities.SQLCode = IBMExtractUtilities.FailedLine = "";
		Statement statement = null;
		try {
			statement = conn.createStatement();
			try {
				statement.execute("SET SQLCOMPAT PLSQL");
			} catch (Exception ex) {
			}
			try {
				statement.execute("SET CURRENT SCHEMA = '" + schema + "'");
				statement
						.execute("SET CURRENT FUNCTION PATH = SYSIBM,SYSFUN,SYSPROC,SYSCAT,SYSIBMADM,"
								+ schema);
			} catch (Exception ex) {
			}
			statement.execute(sql);
			conn.commit();
			if (statement != null)
				statement.close();
		} catch (SQLException e) {
			if ((e instanceof DB2Diagnosable)) {
				DB2Sqlca sqlca = ((DB2Diagnosable) e).getSqlca();
				if (sqlca != null) {
					lineNumber = sqlca.getSqlErrd()[2];
					sqlerrmc = sqlca.getSqlErrmc();
				}
			}
			if ((e.getErrorCode() == -601) || (e.getErrorCode() == -624)) {
				SQLCode = "0";
			} else {
				SQLCode = "" + e.getErrorCode();
				FailedLine = "" + lineNumber;
				Message = getSQLMessage(conn, SQLCode, sqlerrmc);
			}
		}
		String key = type + ":" + schema + ":" + objectName;
		LoadDeployedObjects(outputDirectory);
		if ((SQLCode.equals("")) || (SQLCode.equals("0"))) {
			deployedObjectsList.setProperty(key, "1");
		} else {
			deployedObjectsList.setProperty(key, "0");
		}
		SaveDeployedObjects(outputDirectory);
	}

	public static String CheckOracleRequisites(Connection connection,
			String userID) {
		String sql = "";
		String role = "";
		String message = "";
		if ((userID != null) && (userID.length() != 0))
			userID = userID.toUpperCase();
		try {
			((OracleConnection) connection).setSessionTimeZone(TimeZone
					.getDefault().getID());
			try {
				sql = "SELECT GRANTED_ROLE FROM DBA_ROLE_PRIVS WHERE GRANTEE = '"
						+ userID + "' AND GRANTED_ROLE = 'DBA'";
				PreparedStatement partStatement = connection
						.prepareStatement(sql);
				ResultSet rs = partStatement.executeQuery();
				role = "";
				while (rs.next()) {
					if (role.equals("")) {
						role = rs.getString(1);
						continue;
					}
					role = ":" + role + rs.getString(1);
				}
				if (!role.startsWith("DBA")) {
					message = "DBA ROLE is not available to user '" + userID
							+ "'. Please consult your Oracle DBA.";
				}
				if (rs != null)
					rs.close();
				if (partStatement != null)
					partStatement.close();
				log(message);
				return message;
			} catch (SQLException e2) {
				if (e2.getErrorCode() == 942) {
					message = "DBA / SELECT_CATALOG_ROLE not available to user '"
							+ userID + "'. Please consult your Oracle DBA.";
					log(message);
					return message;
				}
			}
		} catch (SQLException e) {
			try {
				((OracleConnection) connection)
						.setSessionTimeZone(getTimeZoneOffset());
			} catch (Exception ed) {
				sql = "ALTER SESSION SET TIME_ZONE='" + getTimeZoneOffset()
						+ "'";
				log("Serious Error Ora-1804: Unable to set timezone for Oracle. Trying "
						+ sql);
				try {
					PreparedStatement statement = connection
							.prepareStatement(sql);
					int rc = statement.executeUpdate();
					if (statement != null)
						statement.close();
				} catch (SQLException e1) {
					log("Alternate method did not work. It looks that the timezone file on Oracle server are not compatible with current version of Oracle.");
					log("Try \"select count(*) from v$timezone_names\" at your SQL*Plus and if count is 0, contact Oracle support to get a fix.");
					log("You can also look doc ID: 414590.1 and 417893.1 for a solution in metalink.oracle.com");
					log("Continuing with the movement of data and you might notice Oracle -01866 error for the tables having TIMESTAMP column");
					message = "Serious Timezone problem in Oracle database. See console log.";
				}
			}
		}
		return message;
	}

	public static boolean TestConnection(boolean remote,
			boolean compatibilityMode, String dbSourceName, String server,
			int port, String dbName, String userid, String pwd) {
		Connection mainConn = null;
		Message = "";
		String driverName = getDriverName(dbSourceName);
		try {
			try {
				Class.forName(driverName).newInstance();
			} catch (Exception e) {
				e.printStackTrace();
				Message = "Driver could not be loaded. See console's output.";
				return false;
			}
			log("Driver " + driverName + " loaded");
			String url = getURL(dbSourceName, server, port, dbName);
			if (dbSourceName.equalsIgnoreCase("domino")) {
				mainConn = DriverManager.getConnection(url);
			} else {
				mainConn = DriverManager.getConnection(url, userid, pwd);
				mainConn.setAutoCommit(false);
				if (dbSourceName.equalsIgnoreCase("db2")) {
					DB2Path = getDB2PathName(mainConn);
					InstanceName = getInstanceName(mainConn);
					getCompatibilityParams(mainConn);
					ReleaseLevel = getDB2ReleaseLevel(mainConn);
					DB2Compatibility = isDB2CompatiblitySet(mainConn);
					mainConn.commit();
					mainConn.close();
					if (compatibilityMode) {
						if (DB2Compatibility) {
							Message = "Connection to "
									+ (remote ? "remote " : "local ")
									+ dbSourceName + " server succeeded.";
							return true;
						}

						Message = "DB2_COMPATIBILITY_VECTOR is not set on "
								+ (remote ? "remote " : "local ")
								+ "DB2 Server.";
						return false;
					}

					Message = "Connection to "
							+ (remote ? "remote " : "local ") + dbSourceName
							+ " server succeeded.";
					return true;
				}
				if (dbSourceName.equalsIgnoreCase("oracle")) {
					Message = CheckOracleRequisites(mainConn, userid);
					mainConn.commit();
					mainConn.close();

					return Message.equals("");
				}

			}

			Message = "Connection to " + (remote ? "remote " : "local ")
					+ dbSourceName + " succeeded.";
			log(Message);
			mainConn.commit();
			mainConn.close();
		} catch (SQLException e) {
			if (e.getErrorCode() == -4499) {
				if (e.getMessage().contains(
						"An attempt was made to access a database"))
					Message = "Connection succeeded but database not found";
				else if (e.getMessage().contains("Error opening socket"))
					Message = "Error opening socket to DB2 Server. Check if DB2 instance is up. If yes, check check port number.";
				else
					Message = "Error connecting to DB2 server";
				log("Error Message : " + e.getMessage());
			} else if (e.getErrorCode() == -4214) {
				Message = "Connection succeeded but userid/password is incorrect.";
			} else {
				if (e.getMessage().contains("User ID or Password invalid"))
					Message = "Error connecting. User ID or Password invalid";
				else if (e
						.getMessage()
						.contains(
								"The Network Adapter could not establish the connection"))
					Message = "The Network Adapter could not establish the connection";
				else
					Message = dbSourceName
							+ " JDBC connection problem. Please see console's output.";
				log("Error connecting : " + driverName + " for " + dbSourceName
						+ " Error Message :" + e.getMessage());
			}
			log(Message);
			e.printStackTrace();
			return false;
		}
		return true;
	}

	private static void LoadExcludeList() {
		try {
			if (mapExcludeList == null) {
				mapExcludeList = new Properties();
				InputStream istream = ClassLoader
						.getSystemResourceAsStream(SCHEMA_EXCLUDE_FILE);
				if (istream == null) {
					FileInputStream finStream = new FileInputStream(
							SCHEMA_EXCLUDE_FILE);
					mapExcludeList.load(finStream);
					finStream.close();
				} else {
					mapExcludeList.load(istream);
					istream.close();
				}
				log("Configuration file loaded: '" + SCHEMA_EXCLUDE_FILE + "'");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void LoadDeployedObjects(String outputDirectory) {
		String file = outputDirectory + "/savedobjects/" + DEPLOYED_OBJECT_FILE;
		if (FileExists(file)) {
			try {
				FileInputStream inputStream = new FileInputStream(file);
				deployedObjectsList.load(inputStream);
				inputStream.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static void SaveDeployedObjects(String outputDirectory) {
		try {
			if (!FileExists(outputDirectory + "/savedobjects")) {
				new File(outputDirectory + "/savedobjects").mkdir();
			}
			FileOutputStream ostream = new FileOutputStream(outputDirectory
					+ "/savedobjects/" + DEPLOYED_OBJECT_FILE);
			deployedObjectsList.store(ostream, "-- Deployed Objects in DB2");
			ostream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void LoadCompatibilityFiles() {
		try {
			if (mapCompatibilityFiles == null) {
				mapCompatibilityFiles = new Properties();
				InputStream istream = ClassLoader
						.getSystemResourceAsStream(MAP_COMPATIBILITY_FILES);
				if (istream == null) {
					FileInputStream finStream = new FileInputStream(
							MAP_COMPATIBILITY_FILES);
					mapCompatibilityFiles.load(finStream);
					finStream.close();
				} else {
					mapCompatibilityFiles.load(istream);
					istream.close();
				}
				log("Compatibility file loaded: '" + MAP_COMPATIBILITY_FILES
						+ "'");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void LoadDeployFiles() {
		try {
			if (mapDeployFiles == null) {
				mapDeployFiles = new Properties();
				InputStream istream = ClassLoader
						.getSystemResourceAsStream(DEPLOY_FILES_FILE);
				if (istream == null) {
					FileInputStream finStream = new FileInputStream(
							DEPLOY_FILES_FILE);
					mapDeployFiles.load(finStream);
					finStream.close();
				} else {
					mapDeployFiles.load(istream);
					istream.close();
				}
				log("Configuration file loaded: '" + DEPLOY_FILES_FILE + "'");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static String getCompatibilityFiles(String dbSourceName) {
		String fileList = "";
		LoadCompatibilityFiles();

		fileList = mapCompatibilityFiles
				.getProperty(dbSourceName.toLowerCase());
		return fileList;
	}

	public static String getDeployFiles(String dbSourceName) {
		String fileList = "";
		LoadDeployFiles();

		fileList = mapDeployFiles.getProperty(dbSourceName.toLowerCase());
		return fileList;
	}

	public static String getExcludeList(String dbSourceName) {
		String[] strArray = null;
		String schemaList = "";
		LoadExcludeList();

		schemaList = mapExcludeList.getProperty(dbSourceName.toLowerCase());
		if (schemaList != null)
			strArray = schemaList.split("~");
		schemaList = "";
		for (int i = 0; i < strArray.length; i++) {
			if (i > 0)
				schemaList = schemaList + ",";
			schemaList = schemaList + "'" + strArray[i] + "'";
		}
		return schemaList;
	}

	private static boolean Excluded(String dbSourceName, String schemaName) {
		String[] strArray = null;
		String schemaList = null;
		if ((schemaName == null) || (schemaName.equals(""))) {
			return false;
		}
		LoadExcludeList();

		schemaList = mapExcludeList.getProperty(dbSourceName.toLowerCase());
		if (schemaList != null)
			strArray = schemaList.split("~");
		for (int i = 0; i < strArray.length; i++) {
			if (strArray[i].equalsIgnoreCase(schemaName))
				return false;
		}
		return true;
	}

	public static String GetSchemaList(String dbSourceName, String server,
			int port, String dbName, String userid, String pwd) {
		Connection mainConn = null;

		String tableSchema = "";
		String schemaNames = "";
		String driverName = getDriverName(dbSourceName);

		Message = "";
		try {
			Class.forName(driverName).newInstance();
			log("Driver " + driverName + " loaded");
			String url = getURL(dbSourceName, server, port, dbName);
			if (dbSourceName.equalsIgnoreCase("domino")) {
				mainConn = DriverManager.getConnection(url);
			} else {
				mainConn = DriverManager.getConnection(url, userid, pwd);
				mainConn.setAutoCommit(false);
			}

			DatabaseMetaData dbMetaData = mainConn.getMetaData();
			ResultSet Reader = dbMetaData.getSchemas();
			int i = 0;
			while (Reader.next()) {
				tableSchema = Reader.getString(1);
				if (!Excluded(dbSourceName, tableSchema))
					continue;
				if (i > 0)
					schemaNames = schemaNames + ":";
				schemaNames = schemaNames + tableSchema;
				i++;
			}

			if (dbSourceName.equalsIgnoreCase("mysql"))
				schemaNames = "admin";
			mainConn.commit();
			Reader.close();
			mainConn.close();
		} catch (Exception e) {
			if ((dbSourceName.equals("access"))
					|| (dbSourceName.equals("domino"))) {
				log("Schema names are not supported in " + dbSourceName
						+ " database");
			} else {
				log("Error in loading the driver : " + driverName + " for "
						+ dbSourceName + " Error Message :" + e.getMessage());
				Message = dbSourceName
						+ " Error encountered. Please see console's output.";
				e.printStackTrace();
			}
		}
		return schemaNames;
	}

	private static boolean isOracleIOTOverFlow(Connection conn,
			String schemaName, String tableName) {
		boolean notOverFlow = true;
		String sql = "SELECT 1 FROM DBA_TABLES WHERE OWNER = '" + schemaName
				+ "' " + "AND TABLE_NAME = '" + tableName
				+ "' AND IOT_TYPE = 'IOT_OVERFLOW'";
		try {
			PreparedStatement statement = conn.prepareStatement(sql);
			ResultSet rs = statement.executeQuery();
			if (rs.next()) {
				notOverFlow = false;
			}
			if (rs != null)
				rs.close();
			if (statement != null)
				statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return notOverFlow;
	}

	public static void CreateTableScript(String dbSourceName,
			String db2SchemaName, String schemaList, String server, int port,
			String dbName, String userid, String pwd) {
		Connection mainConn = null;

		String tableName = "";
		String schemaName = "";
		String OUTPUT_DIR = null;
		String[] tableType = { "TABLE" };

		String driverName = getDriverName(dbSourceName);
		StringBuffer buffer = new StringBuffer();

		Message = "";
		try {
			OUTPUT_DIR = System.getProperty("OUTPUT_DIR");
			if ((OUTPUT_DIR == null) || (OUTPUT_DIR.equals(""))) {
				OUTPUT_DIR = ".";
			}
			File tmpfile = new File(OUTPUT_DIR);
			tmpfile.mkdirs();
			BufferedWriter inputFileWriter = new BufferedWriter(new FileWriter(
					OUTPUT_DIR + filesep + dbName + ".tables", false));
			Class.forName(driverName).newInstance();
			log("Driver " + driverName + " loaded");
			String url = getURL(dbSourceName, server, port, dbName);
			if (dbSourceName.equalsIgnoreCase("domino")) {
				mainConn = DriverManager.getConnection(url);
			} else {
				mainConn = DriverManager.getConnection(url, userid, pwd);
				mainConn.setAutoCommit(false);
			}
			ResultSet Reader;
			if ((dbSourceName.equalsIgnoreCase("access"))
					|| (dbSourceName.equalsIgnoreCase("domino"))) {
				DatabaseMetaData dbMetaData = mainConn.getMetaData();
				Reader = dbMetaData.getTables(null, null, null, null);
			} else {
				DatabaseMetaData dbMetaData = mainConn.getMetaData();
				Reader = dbMetaData.getTables(null, "%", "%", tableType);
			}

			buffer.setLength(0);
			if ((schemaList == null) || (schemaList.equals(""))
					|| (schemaList.equalsIgnoreCase("all"))) {
				schemaList = GetSchemaList(dbSourceName, server, port, dbName,
						userid, pwd);
			}
			while (Reader.next()) {
				tableName = Reader.getString(3);
				schemaName = Reader.getString(2);
				if (((dbSourceName.equalsIgnoreCase("access")) && (tableName
						.startsWith("MSys")))
						|| ((dbSourceName.equalsIgnoreCase("oracle")) && (tableName
								.startsWith("BIN$"))))
					continue;
				if ((schemaName == null) || (schemaName.equals(""))) {
					if ((db2SchemaName == null) || (db2SchemaName.equals("")))
						db2SchemaName = "ADMIN";
					if (dbSourceName.equalsIgnoreCase("mysql")) {
						buffer.append("\"" + db2SchemaName + "\".\""
								+ tableName + "\":SELECT * FROM " + tableName
								+ linesep);
						continue;
					}
					buffer.append("\"" + db2SchemaName + "\".\"" + tableName
							+ "\":SELECT * FROM \"" + tableName + "\""
							+ linesep);
					continue;
				}

				String[] strArray = schemaList.split(":");
				for (int i = 0; i < strArray.length; i++) {
					if (!strArray[i].equalsIgnoreCase(schemaName))
						continue;
					db2SchemaName = schemaName.replaceAll("\\\\", "\\\\\\\\");
					if (dbSourceName.equalsIgnoreCase("oracle")) {
						boolean notOverFlow = isOracleIOTOverFlow(mainConn,
								schemaName, tableName);
						if (notOverFlow)
							buffer.append("\"" + db2SchemaName + "\".\""
									+ tableName + "\":SELECT * FROM \""
									+ schemaName + "\".\"" + tableName + "\""
									+ linesep);
					} else {
						buffer.append("\"" + db2SchemaName + "\".\""
								+ tableName + "\":SELECT * FROM \""
								+ schemaName + "\".\"" + tableName + "\""
								+ linesep);
					}
				}
			}

			if ((buffer.length() == 0)
					&& (dbSourceName.equalsIgnoreCase("oracle")))
				buffer
						.append("\"" + userid.toUpperCase()
								+ "\".\"DUMMY_TABLE\":SELECT * FROM " + "\""
								+ userid.toUpperCase() + "\".\"DUMMY_TABLE\""
								+ linesep);
			log(buffer.toString());
			inputFileWriter.write(buffer.toString());
			inputFileWriter.close();
			mainConn.commit();
			Reader.close();
			mainConn.close();
		} catch (Exception e) {
			if ((dbSourceName.equals("access"))
					|| (dbSourceName.equals("domino"))) {
				log("Schema names are not supported in " + dbSourceName
						+ " database");
			} else {
				log("Error in loading the driver : " + driverName + " for "
						+ dbSourceName + " Error Message :" + e.getMessage());
				Message = dbSourceName
						+ " Error encountered. Please see console's output.";
				e.printStackTrace();
			}
		}
	}

	public static boolean isDB2CompatiblitySet(Connection conn) {
		String db2Compatibility = null;
		boolean found = false;

		if (conn == null) {
			found = true;
		} else {
			try {
				String sql = "SELECT REG_VAR_VALUE FROM SYSIBMADM.REG_VARIABLES WHERE REG_VAR_NAME = 'DB2_COMPATIBILITY_VECTOR' FETCH FIRST ROW ONLY";

				PreparedStatement partStatement = conn.prepareStatement(sql);
				ResultSet rs = partStatement.executeQuery();
				while (rs.next()) {
					db2Compatibility = rs.getString(1);
					found = true;
				}
				if (rs != null)
					rs.close();
				if (partStatement != null)
					partStatement.close();
				conn.commit();
			} catch (Exception e) {
				db2Compatibility = "";
				e.printStackTrace();
			}
			if (!found) {
				log("*** WARNING ***. The DB2_COMPATIBILITY_VECTOR is not set.");
				log("To set compatibility mode, discontinue this program and run the following commands");
				log("db2set DB2_COMPATIBILITY_VECTOR=FFF");
				log("db2stop force");
				log("db2start");
				Message = "DB2_COMPATIBILITY_VECTOR is not set for DB2.";
			}
			log("DB2 Compatibility Vector=" + db2Compatibility);
		}
		return found;
	}

	public static boolean isJDBCLicenseAdded(String vendor, String jarNames) {
		if ((vendor.equals("db2")) || (vendor.equals("zdb2"))) {
			if ((jarNames.contains("db2jcc_license_cu.jar"))
					|| (jarNames.contains("db2jcc4_license_cu.jar"))
					|| (jarNames.contains("db2jcc_license_cisuz.jar"))
					|| (jarNames.contains("db2jcc4_license_cisuz.jar"))) {
				return true;
			}

			Message = "db2jcc_license_cu.jar or db2jcc4_license_cu.jar or db2jcc_license_cisuz.jar or db2jcc4_license_cisuz.jar file not included.";
			return false;
		}

		return true;
	}

	public static boolean isDB2COMMSet() {
		String line = null;
		Process p = null;
		boolean found = false;
		BufferedReader stdInput = null;
		try {
			p = Runtime.getRuntime().exec("db2set -all");
			stdInput = new BufferedReader(new InputStreamReader(p
					.getInputStream()));
			while ((line = stdInput.readLine()) != null) {
				if (!line.contains("DB2COMM"))
					continue;
				found = true;
			}

			if (stdInput != null)
				stdInput.close();
		} catch (Exception e) {
			found = false;
		}
		if (!found) {
			log("*** WARNING ***. I did not detect DB2COMM set for TCPIP.");
			log("To set db2comm mode, discontinue this program and run the following commands");
			log("db2set DB2COMM=TCPIP");
			log("db2stop force");
			log("db2start");
			Message = "DB2COMM is not set for DB2.";
		}
		return found;
	}

	public static boolean isDB2Installed(boolean remote) {
		String line = null;
		Process p = null;
		boolean found = false;
		BufferedReader stdInput = null;

		if (remote)
			return true;
		try {
			if (osType.equalsIgnoreCase("win")) {
				p = Runtime.getRuntime().exec("db2cmd /c /i /w set ");
				stdInput = new BufferedReader(new InputStreamReader(p
						.getInputStream()));
				while ((line = stdInput.readLine()) != null) {
					if (!line.startsWith("DB2PATH"))
						continue;
					found = true;
				}

			}

			p = Runtime.getRuntime().exec("env");
			stdInput = new BufferedReader(new InputStreamReader(p
					.getInputStream()));
			while ((line = stdInput.readLine()) != null) {
				if (!line.startsWith("DB2INSTANCE"))
					continue;
				found = true;
			}

			if (stdInput != null)
				stdInput.close();
		} catch (Exception e) {
			found = false;
		}
		if (!found) {
			if (osType.equalsIgnoreCase("win")) {
				log("*** WARNING ***. I did not detect DB2 environment.");
			} else {
				log("*** WARNING ***. I did not detect DB2 environment.");
				log("Are you sure that you are calling db2profile from your shell profile?");
			}
			Message = "You are not running this application from within DB2 environment.";
		}
		return found;
	}

	public static String db2JavaPath() {
		String line = null;
		String db2Path = null;
		String javaHome = System.getProperty("java.home");
		String userHome = System.getProperty("user.dir");
		String arch = System.getProperty("sun.arch.data.model");
		String[] tok = null;
		Process p = null;
		BufferedReader stdInput = null;

		return (db2Path == null) || (db2Path.equalsIgnoreCase("not detected")) ? javaHome
				: db2Path;
	}

	public static String db2JDBCHome() {
		String line = null;
		String db2Path = null;
		String userHome = System.getProperty("user.dir");
		String[] tok = null;
		Process p = null;
		BufferedReader stdInput = null;

		if (osType.equalsIgnoreCase("win")) {
			try {
				p = Runtime.getRuntime().exec("db2cmd /c /i /w set ");
				stdInput = new BufferedReader(new InputStreamReader(p
						.getInputStream()));
				while ((line = stdInput.readLine()) != null) {
					if (!line.startsWith("DB2PATH"))
						continue;
					tok = line.split("=");
					db2Path = tok[1] + "\\java";
				}

				if (stdInput != null)
					stdInput.close();
			} catch (Exception e) {
				db2Path = "not detected";
			}
		} else if (osType.equalsIgnoreCase("z/OS")) {
			userHome = System.getProperty("user.dir");
			db2Path = "not detected";
		} else {
			userHome = System.getProperty("user.home");
			if (FileExists(userHome + "/sqllib")) {
				db2Path = userHome + "/sqllib/java";
			} else
				db2Path = "not detected";
		}
		return (db2Path == null) || (db2Path.equalsIgnoreCase("not detected")) ? userHome
				: db2Path;
	}

	private static String encry(int encKey, String toEnc) {
		int t = 0;
		String tog = "";
		if (encKey > 0) {
			while (t < toEnc.length()) {
				int a = toEnc.charAt(t);
				int c = a ^ encKey;
				char d = (char) c;
				tog = tog + d;
				t++;
			}
		}
		return tog;
	}

	private static String encryStr(String encKey, String toEnc) {
		int t = 0;
		int encKeyI = 0;
		while (t < encKey.length()) {
			encKeyI += encKey.charAt(t);
			t++;
		}
		return encry(encKeyI, toEnc);
	}

	private static String Str2Unicode(String str) {
		String result = "";
		char[] chars = str.toCharArray();
		for (int i = 0; i < chars.length; i++) {
			String hexa = Long.toHexString(chars[i]).toUpperCase();
			result = result
					+ new StringBuilder().append("0000").append(hexa)
							.toString().substring(hexa.length(),
									hexa.length() + 4);
		}
		return result;
	}

	private static String Unicode2Str(String s) {
		int i = 0;
		int len = s.length();

		StringBuffer sb = new StringBuffer(len);
		while (i < len) {
			String t = s.substring(i, i + 4);
			char c = (char) Integer.parseInt(t, 16);
			i += 4;
			sb.append(c);
		}
		return sb.toString();
	}

	public static String SaveObject(String sqlTerminator,
			String outputDirectory, String type, String schema,
			String objectName, String sqlSource) {
		new File(outputDirectory + "/savedobjects").mkdir();
		String fileName = outputDirectory + "/savedobjects/"
				+ schema.toLowerCase() + "_" + objectName.toLowerCase()
				+ ".sql";
		try {
			BufferedWriter db2SourceWriter = new BufferedWriter(new FileWriter(
					fileName, false));
			db2SourceWriter.write("--#SET TERMINATOR " + sqlTerminator
					+ linesep + linesep);
			db2SourceWriter.write("--#SET :" + type + ":" + schema + ":"
					+ objectName + "" + linesep);
			db2SourceWriter.write(sqlSource);
			db2SourceWriter.write(sqlTerminator + linesep + linesep);
			log("Object " + objectName + " saved in " + fileName);
			db2SourceWriter.close();
			return "Saved in " + fileName;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "Error saving source";
	}

	public static boolean isHexString(String str) {
		return str.matches("^[0-9A-Fa-f]+$");
	}

	public static String Encrypt(String str) {
		return Str2Unicode(encryStr("IBMExtract", str));
	}

	public static String Decrypt(String str) {
		return encryStr("IBMExtract", Unicode2Str(str));
	}

	public static boolean isIPLocal(String hostNameToCompare) {
		String remoteIPAddress = "";
		String localIPAddress = "";
		try {
			InetAddress inetAdd = InetAddress.getByName(hostNameToCompare);
			remoteIPAddress = inetAdd.getHostAddress();
		} catch (UnknownHostException e1) {
			remoteIPAddress = "";
			e1.printStackTrace();
		}
		try {
			Enumeration<NetworkInterface> nets = NetworkInterface
					.getNetworkInterfaces();
			for (NetworkInterface netint : Collections.list(nets)) {
				Enumeration<InetAddress> inetAddresses = netint
						.getInetAddresses();
				for (InetAddress inetAddress : Collections.list(inetAddresses)) {
					localIPAddress = inetAddress.getHostAddress();
					if (((localIPAddress != null) || (localIPAddress.length() > 0))
							&& ((remoteIPAddress != null) || (remoteIPAddress
									.length() > 0))
							&& (localIPAddress.equals(remoteIPAddress)))
						return true;
				}
			}
		} catch (SocketException e) {
			e.printStackTrace();
			return true;
		}
		return false;
	}

	public static String getTimeZoneOffset() {
		Calendar cal = Calendar.getInstance();
		TimeZone currentTimeZone = cal.getTimeZone();
		Calendar currentDt = new GregorianCalendar(currentTimeZone, Locale.US);
		int gmtOffset = currentTimeZone.getOffset(currentDt.get(0), currentDt
				.get(1), currentDt.get(2), currentDt.get(5), currentDt.get(7),
				currentDt.get(14));

		int hour = gmtOffset / 3600000;
		int min = Math.abs((gmtOffset - hour * 3600000) / 60000);
		return "" + hour + ":" + min;
	}

	public static void replaceStandardOutput(String logFileName) {
		try {
			PrintStream output = new PrintStream(new FileOutputStream(
					logFileName));
			PrintStream tee = new Tee(System.out, output);
			System.setOut(tee);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static void replaceStandardError(String logFileName) {
		try {
			PrintStream err = new PrintStream(new FileOutputStream(logFileName));
			PrintStream tee = new Tee(System.err, err);
			System.setErr(tee);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static String GetIDMTVersion() {
		boolean getUpdatedVersion = false;

		String currentVersion = IBMExtractUtilities.class.getPackage()
				.getImplementationVersion();
		String updatedVersion = "";
		String outcome = "";
		int readCount = 0;
		byte[] buffer = new byte[1024];

		int curMajor = currentVersion == null ? -1 : Integer.valueOf(
				currentVersion.substring(0, currentVersion.indexOf('.')))
				.intValue();
		int curMinor = currentVersion == null ? -1 : Integer.valueOf(
				currentVersion.substring(currentVersion.indexOf('.') + 1,
						currentVersion.indexOf('-'))).intValue();
		int curBuild = currentVersion == null ? -1 : Integer.valueOf(
				currentVersion.substring(currentVersion.indexOf('b') + 1))
				.intValue();
		try {
			URL url = new URL(
					"ftp://public.dhe.ibm.com/education/db2pot/demos/UploadedVersion.txt");
			URLConnection con = url.openConnection();
			InputStream is = con.getInputStream();
			BufferedInputStream bis = new BufferedInputStream(is);

			while ((readCount = bis.read(buffer)) > 0) {
				updatedVersion = new String(buffer, 0, readCount);
			}
			bis.close();
			is.close();

			int updMajor = updatedVersion == null ? -1 : Integer.valueOf(
					updatedVersion.substring(0, updatedVersion.indexOf('.')))
					.intValue();
			int updMinor = updatedVersion == null ? -1 : Integer.valueOf(
					updatedVersion.substring(updatedVersion.indexOf('.') + 1,
							updatedVersion.indexOf('-'))).intValue();
			int updBuild = updatedVersion == null ? -1 : Integer.valueOf(
					updatedVersion.substring(updatedVersion.indexOf('b') + 1))
					.intValue();

			log("Current Version         = " + currentVersion);
			log("New Available Version   = " + updatedVersion);
			if ((updMajor >= curMajor) && (updMinor >= curMinor)
					&& (updBuild >= curBuild)) {
				if ((updMajor != curMajor) || (updMinor != curMinor)
						|| (updBuild != curBuild))
					getUpdatedVersion = true;
			}
			if (getUpdatedVersion) {
				outcome = "Current version is "
						+ (currentVersion == null ? " not detected. "
								: currentVersion)
						+ "\n"
						+ "New Version "
						+ updatedVersion
						+ " is available for download from\n"
						+ "http://www.ibm.com/services/forms/preLogin.do?lang=en_US&source=idmt";
			} else {
				outcome = "Your version " + currentVersion + " is current";
			}
		} catch (Exception e) {
			outcome = "Sorry. The new version details could not be obtained.\nHowever, it is recommended to download latest version from this link.\nhttp://www.ibm.com/services/forms/preLogin.do?lang=en_US&source=idmt";
		}

		return outcome;
	}

	public static void GetLatestJar() {
		boolean getUpdatedVersion = false;
		int bytesRead = 0;

		String updatedVersion = "";

		String currentVersion = IBMExtractUtilities.class.getPackage()
				.getImplementationVersion();

		String address = "ftp://public.dhe.ibm.com/education/db2pot/demos/IBMDataMovementTool.jar";
		URL url = null;
		int curMajor = currentVersion == null ? -1 : Integer.valueOf(
				currentVersion.substring(0, currentVersion.indexOf('.')))
				.intValue();
		int curMinor = currentVersion == null ? -1 : Integer.valueOf(
				currentVersion.substring(currentVersion.indexOf('.') + 1,
						currentVersion.indexOf('-'))).intValue();
		int curBuild = currentVersion == null ? -1 : Integer.valueOf(
				currentVersion.substring(currentVersion.indexOf('b') + 1))
				.intValue();
		try {
			url = new URL("jar:" + address + "!/");
			JarURLConnection conn = (JarURLConnection) url.openConnection();
			JarFile jarFile = conn.getJarFile();
			Manifest mf = jarFile.getManifest();
			updatedVersion = mf.getMainAttributes().getValue(
					"Implementation-Version");
			int updMajor = updatedVersion == null ? -1 : Integer.valueOf(
					updatedVersion.substring(0, updatedVersion.indexOf('.')))
					.intValue();
			int updMinor = updatedVersion == null ? -1 : Integer.valueOf(
					updatedVersion.substring(updatedVersion.indexOf('.') + 1,
							updatedVersion.indexOf('-'))).intValue();
			int updBuild = updatedVersion == null ? -1 : Integer.valueOf(
					updatedVersion.substring(updatedVersion.indexOf('b') + 1))
					.intValue();
			log("Current Version         = " + currentVersion);
			log("Version at IBM FTP Site = " + updatedVersion);
			if ((updMajor >= curMajor) && (updMinor >= curMinor)
					&& (updBuild >= curBuild)) {
				if ((updMajor != curMajor) || (updMinor != curMinor)
						|| (updBuild != curBuild))
					getUpdatedVersion = true;
			}
			if (getUpdatedVersion) {
				url = new URL(address);
				String filename = url.getFile();
				filename = filename.substring(filename.lastIndexOf('/') + 1);
				String newfilename = filename + ".new";
				FileOutputStream out = new FileOutputStream(newfilename);
				URLConnection conn2 = url.openConnection();
				int contentLength = conn2.getContentLength();
				InputStream in = new BufferedInputStream(conn2.getInputStream());
				byte[] data = new byte[200000];
				int len;
				while ((len = in.read(data)) > 0) {
					bytesRead += len;
					out.write(data, 0, len);
				}
				in.close();
				out.flush();
				out.close();
				if (bytesRead != contentLength) {
					log("IBMDataMovementTool.jar download size did not match. Retry again ...");
				} else
					log("Download of IBMDataMovementTool.jar successful bytes="
							+ contentLength);
			} else {
				log("Your version " + currentVersion + " is current");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static String readLine(BufferedReader reader) throws IOException {
		int ch = -1;
		StringBuffer line = new StringBuffer();

		ch = reader.read();
		while ((ch != -1) && (ch != 13) && (ch != 10)) {
			line.append((char) ch);
			ch = reader.read();
		}

		if ((ch == -1) && (line.length() == 0)) {
			return null;
		}

		switch ((char) ch) {
		case '\r':
			reader.mark(2);
			switch (ch = reader.read()) {
			case 13:
				if ((char) (ch = reader.read()) == '\n')
					break;
				reader.reset();
				break;
			case 10:
				break;
			case -1:
				break;
			default:
				reader.reset();
			}
			break;
		case '\n':
		}

		return line.toString();
	}

	public static void putHelpInformation(BufferedWriter buffer, String fileName) {
		String versionInfo = IBMExtractUtilities.class.getPackage()
				.getImplementationVersion();

		String dropScript = "db2dropobjects.sh";

		if ((fileName == null) || (fileName.length() == 0)) {
			return;
		}
		int pos = fileName.lastIndexOf(".");
		String helpType;
		if (pos > 0)
			helpType = fileName.substring(0, pos);
		else {
			helpType = fileName;
		}
		if (osType.equalsIgnoreCase("win")) {
			dropScript = "db2dropobjects.cmd";
		}
		try {
			buffer.append("-- IBM Data Movement Tool Version " + versionInfo
					+ linesep);
			if (helpType.equalsIgnoreCase("db2check")) {
				buffer
						.append("-- This file contains check constraints extracted from source database."
								+ linesep);
				buffer
						.append("-- This script is part of the deployment script."
								+ linesep);
				buffer
						.append("-- The contents of the file will also be loaded in GUI interactive deployment."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2load")) {
				buffer
						.append("-- This file contains DB2 Load statements for loading data in DB2."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2runstats")) {
				buffer
						.append("-- This file contains DB2 RUNSTATS commands to generate table statistics."
								+ linesep);
				buffer
						.append("-- This script is not part of the deployment script so you will need to run this manually."
								+ linesep);
				buffer
						.append("-- To run this script, open DB2 command window and use following command."
								+ linesep);
				buffer.append("-- db2 -tvf db2runstats.sql" + linesep);
			} else if (helpType.equalsIgnoreCase("db2tabstatus")) {
				buffer
						.append("-- This script is part of deployment script and it is not necessary to run this individually."
								+ linesep);
				buffer
						.append("-- This file contains queries for each table to determine status of DB2 table after data load."
								+ linesep);
				buffer
						.append("-- You will see output for column FK_CHECKED CC_CHECKED STATUS"
								+ linesep);
				buffer.append("-- The value of Y Y and N indicate success."
						+ linesep);
			} else if (helpType.equalsIgnoreCase("db2tabcount")) {
				buffer
						.append("-- This script is part of deployment script and it is not necessary to run this individually."
								+ linesep);
				buffer
						.append("-- This file contains queries to do the row count for each table for which data was loaded."
								+ linesep);
				buffer
						.append("-- After movement of data, you should run rowcount script as that will tell you row count from source and target database."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2loadterminate")) {
				buffer
						.append("-- This script is not part of the deployment script and you will need to run it manually if required."
								+ linesep);
				buffer
						.append("-- Sometimes a DB2 table can remain in LOAD PENDING status due to some failures."
								+ linesep);
				buffer
						.append("-- For such cases, if you try to run db2load.sql, the LOAD may fail."
								+ linesep);
				buffer
						.append("-- In those situations, run this script to TERMINATE the pending LOAD."
								+ linesep);
				buffer
						.append("-- You can also run this script from GUI by choosing Execute DB2 Script"
								+ linesep);
				buffer
						.append("-- To run this script, open DB2 command window and use following command."
								+ linesep);
				buffer.append("-- db2 -tvf db2loadterminate.db2" + linesep);
			} else if (helpType.equalsIgnoreCase("db2checkpending")) {
				buffer
						.append("-- This script is not part of the deployment script and it might not be necessary to run this script."
								+ linesep);
				buffer
						.append("-- A better way to remove check pending status is done through db2checkRemoval.cmd script."
								+ linesep);
				buffer
						.append("-- db2checkRemoval.cmd or db2checkRemoval.sh is part of the deployment script."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2tables")) {
				buffer
						.append("-- This script contains DB2 DDL for tables and this is part of the deployment script."
								+ linesep);
				buffer
						.append("-- The contents of the file will also be loaded in GUI interactive deployment."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2fkeys")) {
				buffer
						.append("-- This script contains foreign keys and is part of the deployment script."
								+ linesep);
				buffer
						.append("-- The contents of the file will also be loaded in GUI interactive deployment."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2droptables")) {
				buffer
						.append("-- This script contains DROP TABLE statements and is part of the "
								+ dropScript + " script." + linesep);
			} else if (helpType.equalsIgnoreCase("db2cons")) {
				buffer
						.append("-- This script contains primary key constraints, indexes and unique indexes."
								+ linesep);
				buffer
						.append("-- This script is part of the deployment script."
								+ linesep);
				buffer
						.append("-- The contents of the file will also be loaded in GUI interactive deployment."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2dropfkeys")) {
				buffer
						.append("-- This script will drop FOREIGN KEY constraints and is part of the "
								+ dropScript + " script." + linesep);
			} else if (helpType.equalsIgnoreCase("db2uniq")) {
				buffer
						.append("-- This script is defunct now and can be discarded."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2sequences")) {
				buffer
						.append("-- This script contains DB2 sequences and is part of the deployment script."
								+ linesep);
				buffer
						.append("-- The contents of the file will also be loaded in GUI interactive deployment."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2dropsequences")) {
				buffer
						.append("-- This script contains DROP SEQUENCE statements and is part of the "
								+ dropScript + " script." + linesep);
			} else if (helpType.equalsIgnoreCase("db2dropobjects")) {
				buffer
						.append("-- This script contains DROP statements for PL/SQL objects and is part of the "
								+ dropScript + " script." + linesep);
			} else if (helpType.equalsIgnoreCase("db2roleprivs")) {
				buffer
						.append("-- This script contains privileges granted to a ROLE in DB2. This script is not part of the deployment script."
								+ linesep);
				buffer
						.append("-- The contents of the file will also be loaded in GUI interactive deployment."
								+ linesep);
				buffer.append("-- db2 -tvf db2roleprivs.db2" + linesep);
			} else if (helpType.equalsIgnoreCase("db2objprivs")) {
				buffer
						.append("-- This script contains object privileges granted to a user. This script is not part of the deployment script."
								+ linesep);
				buffer
						.append("-- You can also run this script from GUI by choosing Execute DB2 Script"
								+ linesep);
				buffer
						.append("-- To run this script, open DB2 command window and use following command."
								+ linesep);
				buffer.append("-- db2 -tvf db2objprivs.db2" + linesep);
			} else if (helpType.equalsIgnoreCase("db2synonyms")) {
				buffer
						.append("-- This script contains synonyms extracted and it is part of the deployment script."
								+ linesep);
				buffer
						.append("-- The contents of the file will also be loaded in GUI interactive deployment."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2dropsynonyms")) {
				buffer
						.append("-- This script contains DROP statements for synonyms and is part of the "
								+ dropScript + " script." + linesep);
			} else if (helpType.equalsIgnoreCase("db2mviews")) {
				buffer
						.append("-- This script contains equivalent of materialized views in DB2"
								+ linesep);
				buffer
						.append("-- This script is not part of the deployment script."
								+ linesep);
				buffer
						.append("-- However, contents of the file will be loaded in GUI interactive deployment."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2udf")) {
				buffer
						.append("-- This script contains DB2 UDFs generated by the tool if required. This script is part of the deployment script."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2tsbp")) {
				buffer
						.append("-- This script contains necessary and required DB2 table spaces and buffer pool using automatic storage."
								+ linesep);
				buffer
						.append("-- This script is part of the deployment script."
								+ linesep);
				buffer
						.append("-- If you have chosen option useBestPracticeTSNames=false, you will see source database"
								+ linesep);
				buffer.append("-- tablespaces name used in DB2 as it is."
						+ linesep);
				buffer
						.append("-- The contents of the file will also be loaded in GUI interactive deployment."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2droptsbp")) {
				buffer
						.append("-- This script contains DROP statements for table spaces and buffer pools and is part of the "
								+ dropScript + " script." + linesep);
			} else if (helpType.equalsIgnoreCase("db2default")) {
				buffer
						.append("-- This script contains ALTER TABLE to add default values to the columns. This script is part of the deployment script."
								+ linesep);
				buffer
						.append("-- The contents of the file will also be loaded in GUI interactive deployment."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2TruncName")) {
				buffer
						.append("-- This script contains a list of original table / column names and truncated name if they did happen."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2type_body")) {
				buffer
						.append("-- This script contains DB2 TYPE BODY statments and is not part of deployment script."
								+ linesep);
				buffer
						.append("-- However, contents of the file will be loaded in GUI interactive deployment."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2type")) {
				buffer
						.append("-- This script contains DB2 TYPE statments and is not part of deployment script."
								+ linesep);
				buffer
						.append("-- However, contents of the file will be loaded in GUI interactive deployment."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2trigger")) {
				buffer
						.append("-- This script contains TRIGGER statments and is not part of deployment script."
								+ linesep);
				buffer
						.append("-- However, contents of the file will be loaded in GUI interactive deployment."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2procedure")) {
				buffer
						.append("-- This script contains PROCEDURE statments and is not part of deployment script."
								+ linesep);
				buffer
						.append("-- However, contents of the file will be loaded in GUI interactive deployment."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2routine")) {
				buffer
						.append("-- This script contains DB2 Non-SQL PROCEDURE and FUNCTIONS statments and this is part of the deployment script."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2xmlschema")) {
				buffer
						.append("-- This script contains DB2 XSR definitions. This script is part of the deployment script."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2package_body")) {
				buffer
						.append("-- This script contains PACKAGE BODY statments and is not part of deployment script."
								+ linesep);
				buffer
						.append("-- However, contents of the file will be loaded in GUI interactive deployment."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2package")) {
				buffer
						.append("-- This script contains PACKAGE statments and is not part of deployment script."
								+ linesep);
				buffer
						.append("-- However, contents of the file will be loaded in GUI interactive deployment."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2function")) {
				buffer
						.append("-- This script contains FUNCTION statments and is not part of deployment script."
								+ linesep);
				buffer
						.append("-- However, contents of the file will be loaded in GUI interactive deployment."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2views")) {
				buffer
						.append("-- This script contains VIEWS statments and is not part of deployment script."
								+ linesep);
				buffer
						.append("-- However, contents of the file will be loaded in GUI interactive deployment."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2module")) {
				buffer
						.append("-- This script contains DB2 Module components and is part of the deployment script."
								+ linesep);
			} else if (helpType.equalsIgnoreCase("db2variable")) {
				buffer
						.append("-- This script contains DB2 Global Variables and is part of the deployment script."
								+ linesep);
			}
			buffer.append("-- " + linesep);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		GetLatestJar();
	}
}