package ibm;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

public class Count {

	public Count() {
		initDBSources();
	}

	private static void log(String msg) {
		System.out.println((new StringBuilder()).append("[").append(
				timestampFormat.format(new Date())).append("] ").append(msg)
				.toString());
	}

	private static String getURL(String vendor, String server, int port,
			String dbName) {
		String url = "";
		if (vendor.equalsIgnoreCase("oracle"))
			url = (new StringBuilder()).append((String) propURL.get(vendor))
					.append(server).append(":").append(port).append(":")
					.append(dbName).toString();
		else if (vendor.equalsIgnoreCase("mssql"))
			url = (new StringBuilder()).append((String) propURL.get(vendor))
					.append(server).append(":").append(port).append(
							";database=").append(dbName).toString();
		else if (vendor.equalsIgnoreCase("access")
				|| vendor.equalsIgnoreCase("hxtt"))
			url = (new StringBuilder()).append((String) propURL.get(vendor))
					.append(server).toString();
		else
			url = (new StringBuilder()).append((String) propURL.get(vendor))
					.append(server).append(":").append(port).append("/")
					.append(dbName).toString();
		return url;
	}

	private static void initDBSources() {
		ArrayList<String> al = new ArrayList<String>();
		String srcDriverName = "";
		String dstDriverName = "";
		if (!srcVendor.equalsIgnoreCase("postgres")
				&& !srcVendor.equalsIgnoreCase("oracle")
				&& !srcVendor.equalsIgnoreCase("db2")
				&& !srcVendor.equalsIgnoreCase("mysql")
				&& !srcVendor.equalsIgnoreCase("sybase")
				&& !srcVendor.equalsIgnoreCase("mssql")
				&& !srcVendor.equalsIgnoreCase("zdb2")
				&& !srcVendor.equalsIgnoreCase("access")
				&& !srcVendor.equalsIgnoreCase("hxtt")
				&& !srcVendor.equalsIgnoreCase("idb2")) {
			log("Invalid srcVendor supplied. Valid value can be one of these -> postgres oracle db2 mysql sybase mssql");
			System.exit(-1);
		}
		OUTPUT_DIR = System.getProperty("OUTPUT_DIR");
		if (OUTPUT_DIR == null)
			OUTPUT_DIR = ".";
		if (!OUTPUT_DIR.equalsIgnoreCase(".") && !OUTPUT_DIR.endsWith("\\")
				&& !OUTPUT_DIR.endsWith("/"))
			OUTPUT_DIR = (new StringBuilder()).append(OUTPUT_DIR).append("/")
					.toString();
		File tmpfile = new File(OUTPUT_DIR);
		tmpfile.mkdirs();
		try {
			log((new StringBuilder()).append("OUTPUT_DIR is : ").append(
					tmpfile.getCanonicalPath()).toString());
		} catch (IOException e1) {
			log(e1.getMessage());
		}
		propTables = new Properties();
		propDrivers = new Properties();
		propURL = new Properties();
		try {
			LineNumberReader in = new LineNumberReader(new FileReader(
					TABLES_PROP_FILE));
			do {
				String line;
				if ((line = in.readLine()) == null)
					break;
				if (!line.trim().equals("") && !line.startsWith("#"))
					al.add(line);
			} while (true);
		} catch (FileNotFoundException e) {
			log((new StringBuilder()).append("Configuration file '").append(
					TABLES_PROP_FILE).append("' not found. Existing ...")
					.toString());
			System.exit(-1);
		} catch (Exception ex) {
			log((new StringBuilder()).append(
					"Error reading configuration file '").append(
					TABLES_PROP_FILE).append("'").toString());
			System.exit(-1);
		}
		totalTables = al.size();
		log((new StringBuilder()).append("Configuration file loaded: '")
				.append(TABLES_PROP_FILE).append("'").toString());
		query = new String[totalTables];
		srcSchName = new String[totalTables];
		dstSchName = new String[totalTables];
		srcTableName = new String[totalTables];
		dstTableName = new String[totalTables];
		countDstSQL = new String[totalTables];
		countSrcSQL = new String[totalTables];
		log((new StringBuilder()).append("query size ").append(query.length)
				.append(" dstSchName size = ").append(dstSchName.length)
				.toString());
		int i;
		for (i = 0; i < totalTables; i++) {
			query[i] = new String();
			dstSchName[i] = new String();
			srcTableName[i] = new String();
			srcSchName[i] = new String();
			countDstSQL[i] = new String();
		}

		i = 0;
		for (Iterator<String> itr = al.iterator(); itr.hasNext(); i++) {
			String line = (String) itr.next();
			String values = line.substring(0, line.indexOf(":"));
			dstSchName[i] = values.substring(0, values.indexOf("."));
			dstSchName[i] = dstSchName[i].toUpperCase();
			dstTableName[i] = values.substring(values.indexOf(".") + 1);
			dstTableName[i] = dstTableName[i].toUpperCase();
			query[i] = line.substring(line.indexOf(":") + 1);
			String srcStr = query[i].toUpperCase();
			String qryStr = query[i].substring(srcStr.indexOf("FROM ") + 5);
			int fromPos = qryStr.indexOf(".");
			if (fromPos > 0) {
				srcSchName[i] = qryStr.substring(0, fromPos);
				srcTableName[i] = qryStr.substring(fromPos + 1);
				if (srcVendor.equalsIgnoreCase("mssql")
						|| srcVendor.equalsIgnoreCase("db2")
						|| srcVendor.equalsIgnoreCase("zdb2")
						|| srcVendor.equalsIgnoreCase("sybase"))
					countSrcSQL[i] = (new StringBuilder()).append(
							"SELECT COUNT_BIG(*) FROM ").append(srcSchName[i])
							.append(".").append(srcTableName[i]).toString();
				else
					countSrcSQL[i] = (new StringBuilder()).append(
							"SELECT COUNT(*) FROM ").append(srcSchName[i])
							.append(".").append(srcTableName[i]).toString();
				if (dstVendor.equalsIgnoreCase("mssql")
						|| dstVendor.equalsIgnoreCase("db2")
						|| dstVendor.equalsIgnoreCase("zdb2")
						|| dstVendor.equalsIgnoreCase("sybase"))
					countDstSQL[i] = (new StringBuilder()).append(
							"SELECT COUNT_BIG(*) FROM ").append(dstSchName[i])
							.append(".").append(dstTableName[i]).toString();
				else
					countDstSQL[i] = (new StringBuilder()).append(
							"SELECT COUNT(*) FROM ").append(dstSchName[i])
							.append(".").append(dstTableName[i]).toString();
				continue;
			}
			srcSchName[i] = srcVendor;
			srcTableName[i] = qryStr;
			if (srcVendor.equalsIgnoreCase("mssql")
					|| srcVendor.equalsIgnoreCase("db2")
					|| srcVendor.equalsIgnoreCase("zdb2")
					|| srcVendor.equalsIgnoreCase("sybase"))
				countSrcSQL[i] = (new StringBuilder()).append(
						"SELECT COUNT_BIG(*) FROM ").append(srcSchName[i])
						.append(".").append(srcTableName[i]).toString();
			else
				countSrcSQL[i] = (new StringBuilder()).append(
						"SELECT COUNT(*) FROM ").append(srcTableName[i])
						.toString();
			if (dstVendor.equalsIgnoreCase("mssql")
					|| dstVendor.equalsIgnoreCase("db2")
					|| dstVendor.equalsIgnoreCase("zdb2")
					|| dstVendor.equalsIgnoreCase("sybase"))
				countDstSQL[i] = (new StringBuilder()).append(
						"SELECT COUNT_BIG(*) FROM ").append(dstSchName[i])
						.append(".").append(dstTableName[i]).toString();
			else
				countDstSQL[i] = (new StringBuilder()).append(
						"SELECT COUNT(*) FROM ").append(dstTableName[i])
						.toString();
		}

		try {
			java.io.InputStream istream = ClassLoader
					.getSystemResourceAsStream(DRIVER_PROP_FILE);
			if (istream == null)
				propDrivers.load(new FileInputStream(DRIVER_PROP_FILE));
			else
				propDrivers.load(istream);
			log((new StringBuilder()).append("Configuration file loaded: '")
					.append(DRIVER_PROP_FILE).append("'").toString());
			istream = ClassLoader.getSystemResourceAsStream(URL_PROP_FILE);
			if (istream == null)
				propURL.load(new FileInputStream(URL_PROP_FILE));
			else
				propURL.load(istream);
			log((new StringBuilder()).append("Configuration file loaded: '")
					.append(URL_PROP_FILE).append("'").toString());
			countWriter = new BufferedWriter(new FileWriter(
					(new StringBuilder()).append(OUTPUT_DIR).append("/")
							.append(TABLES_PROP_FILE).append(".rowcount")
							.toString(), false));
		} catch (Exception e) {
			log((new StringBuilder()).append(
					"Error in loading the properties file Error Message :")
					.append(e.getMessage()).toString());
			System.exit(-1);
		}
		try {
			srcDriverName = propDrivers.getProperty(srcVendor);
			Class.forName(srcDriverName).newInstance();
			log((new StringBuilder()).append("Driver ").append(srcDriverName)
					.append(" loaded").toString());
			srcConn = DriverManager.getConnection(getURL(srcVendor, srcServer,
					srcPort, srcDBName), srcLogin);
			srcConn.setAutoCommit(autoCommit);
			if (!dstVendor.equals("")) {
				dstDriverName = propDrivers.getProperty(dstVendor);
				Class.forName(dstDriverName).newInstance();
				log((new StringBuilder()).append("Driver ").append(
						dstDriverName).append(" loaded").toString());
				dstConn = DriverManager.getConnection(getURL(dstVendor,
						dstServer, dstPort, dstDBName), dstLogin);
				dstConn.setAutoCommit(autoCommit);
			}
		} catch (Exception e) {
			log((new StringBuilder()).append("Error in loading the driver : ")
					.append(srcDriverName).append(" for ").append(srcVendor)
					.append("Error Message :").append(e.getMessage())
					.toString());
			System.exit(-1);
		}
	}

	private static String pad(Object str, int padlen, String pad) {
		String padding = new String();
		int len = Math.abs(padlen) - str.toString().length();
		if (len < 1)
			return str.toString();
		for (int i = 0; i < len; i++)
			padding = (new StringBuilder()).append(padding).append(pad)
					.toString();

		return padlen >= 0 ? (new StringBuilder()).append(str).append(padding)
				.toString() : (new StringBuilder()).append(padding).append(str)
				.toString();
	}

	private static void countRows() {
		ResultSet srcReader = null;
		ResultSet dstReader = null;
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);
		try {
			if (!dstVendor.equals(""))
				buffer.append((new StringBuilder()).append(
						pad(srcVendor, 85, " ")).append(" : ").append(
						pad(dstVendor, 85, " ")).append(linesep).toString());
			for (int i = 0; i < totalTables; i++) {
				PreparedStatement srcStatement = srcConn
						.prepareStatement(countSrcSQL[i]);
				srcReader = srcStatement.executeQuery();
				long srcNumber;
				for (srcNumber = 0L; srcReader.next(); srcNumber = srcReader
						.getLong(1))
					;
				String srcStr = (new StringBuilder()).append(srcSchName[i])
						.append(".").append(srcTableName[i]).toString();
				if (srcReader != null)
					srcReader.close();
				if (srcStatement != null)
					srcStatement.close();
				if (!dstVendor.equals("")) {
					String dstStr = (new StringBuilder()).append(dstSchName[i])
							.append(".").append(dstTableName[i]).toString();
					long dstNumber = -1L;
					try {
						PreparedStatement dstStatement = dstConn
								.prepareStatement(countDstSQL[i]);
						dstReader = dstStatement.executeQuery();
						for (dstNumber = 0L; dstReader.next(); dstNumber = dstReader
								.getLong(1))
							;
						if (dstReader != null)
							dstReader.close();
						if (dstStatement != null)
							dstStatement.close();
					} catch (Exception ex) {
						log(countDstSQL[i]);
						ex.printStackTrace();
					}
					buffer.append((new StringBuilder()).append(
							pad(srcStr, 60, " ")).append(" : ").append(
							pad(String.valueOf(srcNumber), 25, " ")).append(
							pad(dstStr, 60, " ")).append(" : ").append(
							dstNumber).append(linesep).toString());
					if (debug)
						log((new StringBuilder())
								.append(pad(srcStr, 60, " "))
								.append(" : ")
								.append(pad(String.valueOf(srcNumber), 25, " "))
								.append(" : ").append(dstNumber).toString());
					continue;
				}
				buffer.append((new StringBuilder())
						.append(pad(srcStr, 60, " ")).append(" : ").append(
								srcNumber).append(linesep).toString());
				if (debug)
					log((new StringBuilder()).append(pad(srcStr, 60, " "))
							.append(" : ").append(srcNumber).toString());
			}

			countWriter.write(buffer.toString());
			countWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String args[]) {
		if (args.length != 13 && args.length != 7) {
			System.out
					.println("usage: java ibm.Count table_prop_file srcVendor srcserver srcdbname srcportnum srcuid srcpwd dstVendor dstserver dstdbname dstportnum dstuid dstpwd");
			System.out
					.println("usage: java ibm.Count table_prop_file VendorName server dbname portnum uid pwd");
			System.exit(-1);
		}
		TABLES_PROP_FILE = args[0];
		srcVendor = args[1];
		srcServer = args[2];
		srcDBName = args[3];
		srcPort = Integer.parseInt(args[4]);
		String srcUid = args[5];
		String srcPwd = args[6];
		if (IBMExtractUtilities.isHexString(srcPwd))
			srcPwd = IBMExtractUtilities.Decrypt(srcPwd);
		log((new StringBuilder()).append("TABLES_PROP_FILE:").append(
				TABLES_PROP_FILE).toString());
		log((new StringBuilder()).append("DRIVER_PROP_FILE:").append(
				DRIVER_PROP_FILE).toString());
		log((new StringBuilder()).append("URL_PROP_FILE:")
				.append(URL_PROP_FILE).toString());
		log((new StringBuilder()).append("srcVendor:").append(srcVendor)
				.toString());
		log((new StringBuilder()).append("srcServer:").append(srcServer)
				.toString());
		log((new StringBuilder()).append("srcDBName:").append(srcDBName)
				.toString());
		log((new StringBuilder()).append("srcPort:").append(srcPort).toString());
		log((new StringBuilder()).append("srcUid:").append(srcUid).toString());
		if (args.length == 13) {
			dstVendor = args[7];
			dstServer = args[8];
			dstDBName = args[9];
			dstPort = Integer.parseInt(args[10]);
			String dstUid = args[11];
			String dstPwd = args[12];
			if (IBMExtractUtilities.isHexString(dstPwd))
				dstPwd = IBMExtractUtilities.Decrypt(dstPwd);
			log((new StringBuilder()).append("dstVendor:").append(dstVendor)
					.toString());
			log((new StringBuilder()).append("dstServer:").append(dstServer)
					.toString());
			log((new StringBuilder()).append("dstDBName:").append(dstDBName)
					.toString());
			log((new StringBuilder()).append("dstPort:").append(dstPort)
					.toString());
			log((new StringBuilder()).append("dstUid:").append(dstUid)
					.toString());
			dstLogin.setProperty("user", dstUid);
			dstLogin.setProperty("password", dstPwd);
		}
		srcLogin.setProperty("user", srcUid);
		srcLogin.setProperty("password", srcPwd);
		if (srcVendor.equalsIgnoreCase("mssql")) {
			srcLogin.setProperty("sendStringParametersAsUnicode", "true");
			srcLogin.setProperty("selectMethod", "cursor");
		}
		countRows();
	}

	private static final SimpleDateFormat timestampFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH.mm.ss.SSS");
	private static String linesep = System.getProperty("line.separator");
	private static String srcVendor;
	private static String dstVendor = "";
	private static String srcDBName;
	private static String dstDBName = "";
	private static String TABLES_PROP_FILE = "";
	private static String DRIVER_PROP_FILE = "driver.properties";
	private static String URL_PROP_FILE = "url.properties";
	private static String OUTPUT_DIR = null;
	private static Properties propTables;
	private static Properties propDrivers;
	private static Properties propURL;
	private static Properties srcLogin = new Properties();
	private static Properties dstLogin = new Properties();
	private static String srcServer = "";
	private static String dstServer = "";
	private static boolean autoCommit = false;
	private static int srcPort;
	private static int dstPort;
	private static int totalTables;
	private static BufferedWriter countWriter;
	private static boolean debug = true;
	private static String query[];
	private static String srcSchName[];
	private static String dstSchName[];
	private static String srcTableName[];
	private static String dstTableName[];
	private static String countDstSQL[];
	private static String countSrcSQL[];
	private static Connection srcConn;
	private static Connection dstConn;

}
