package ibm;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class GenerateMeet {
	private static String linesep = System.getProperty("line.separator");
	private static String filesep = System.getProperty("file.separator");
	private static String sqlTerminator = "/";
	private static boolean autoCommit = false;
	private static Connection mainConn;
	private static int port = 1521;
	private static int fetchSize = 1000;
	private static String dbSourceName = "oracle";
	private static String server = "localhost";
	private static String dbName = "XE";
	private static String OUTPUT_DIR = null;
	private static String schemaList = "";
	private static String uid = "";
	private static String pwd = "";
	private static String jdbcHome = ".";
	private static Properties login = new Properties();
	private static int majorSourceDBVersion;
	private static int minorSourceDBVersion;
	private static BufferedWriter db2MEETWriter;

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

	private void initDBSources() {
		OUTPUT_DIR = System.getProperty("OUTPUT_DIR");
		if (OUTPUT_DIR == null) {
			OUTPUT_DIR = ".";
		}
		File tmpfile = new File(OUTPUT_DIR);
		tmpfile.mkdirs();
		try {
			log("OUTPUT_DIR is : " + tmpfile.getCanonicalPath());
		} catch (IOException e1) {
			log(e1.getMessage());
		}

		String driverName = "";
		try {
			driverName = IBMExtractUtilities.getDriverName(dbSourceName);
			Class.forName(driverName).newInstance();
			log("Driver " + driverName + " loaded");
			String url = IBMExtractUtilities.getURL(dbSourceName, server, port,
					dbName);
			login.setProperty("user", uid);
			login.setProperty("password", pwd);
			mainConn = DriverManager.getConnection(url, login);
			mainConn.setAutoCommit(autoCommit);
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

	private void genMEETOutputForPublicSchema(String schemaList) {
		String sql = "";
		try {
			log("Starting generation of Meet output");

			sql = "SELECT TO_CHAR(DBMS_METADATA.GET_DDL('DB_LINK', object_name,'PUBLIC')) ddl_string FROM DBA_OBJECTS WHERE OWNER = 'PUBLIC' AND OBJECT_TYPE = 'DATABASE LINK'";

			db2MEETWriter.write(executeSQL(sql));
			log("Meet output for public db links done");

			sql = "SELECT DBMS_METADATA.GET_DDL('DIRECTORY', object_name, 'PUBLIC') ddl_string FROM DBA_OBJECTS WHERE OWNER = 'PUBLIC' AND OBJECT_TYPE = 'DIRECTORY'";

			db2MEETWriter.write(executeSQL(sql));
			log("Meet output for public directory done");

			sql = "SELECT DBMS_METADATA.GET_DDL('SYNONYM', SYNONYM_NAME, 'PUBLIC') ddl_string FROM DBA_SYNONYMS WHERE OWNER = 'PUBLIC' AND DB_LINK IS NULL AND TABLE_OWNER IN ("
					+ schemaList + ")";

			db2MEETWriter.write(executeSQL(sql));
			log("Meet output for public synonym done");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void getPLSQLSource(String schema, String type, String objectName,
			StringBuffer buffer) {
		ResultSet Reader = null;
		String dstSchema = schema;
		String plSQL = "select text from dba_source where owner = '"
				+ dstSchema + "' " + "and name = '" + objectName
				+ "' and type = '" + type + "' order by line asc";
		try {
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

	private void genMEETPLSQL(String schema) {
		String plSQL = "";
		String plsqlTemplate = "select dbms_metadata.get_ddl('&type&','&name&','&schema&') from dual";

		ResultSet Reader = null;
		ResultSet plsqlReader = null;
		StringBuffer chunks = new StringBuffer();

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
					String origType = Reader.getString(1);
					String name = Reader.getString(2);
					String type = origType.replace(" ", "_");
					String newType = type;
					if (type.equals("PACKAGE"))
						newType = "PACKAGE_SPEC";
					String ddlSQL = plsqlTemplate.replace("&schema&", schema);
					ddlSQL = ddlSQL.replace("&name&", name);
					ddlSQL = ddlSQL.replace("&type&", newType);
					try {
						chunks.setLength(0);
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
						chunks.append(sqlTerminator + linesep + linesep);
						try {
							db2MEETWriter.write(chunks.toString());
							db2MEETWriter.flush();
						} catch (IOException e) {
							log("Error writing to the meet file.");
							e.printStackTrace();
						}
					}
					if ((objCount > 0) && (objCount % 20 == 0))
						log(objCount
								+ " numbers of PL/SQL objects extracted for schema "
								+ schema);
					objCount++;
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

	private String getColumnList(String schema, String mviewName) {
		String sql = "";
		String columnList = "";

		ResultSet Reader = null;

		if (dbSourceName.equalsIgnoreCase("oracle")) {
			sql = "SELECT COLUMN_NAME FROM DBA_TAB_COLUMNS WHERE OWNER = '"
					+ schema + "' AND TABLE_NAME = '" + mviewName
					+ "' ORDER BY COLUMN_ID ASC";
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
					columnList = IBMExtractUtilities.putQuote(Reader
							.getString(1));
				} else {
					columnList = columnList + ","
							+ IBMExtractUtilities.putQuote(Reader.getString(1));
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

	private String getViewSource(String schema, String viewName) {
		ResultSet Reader = null;
		StringBuffer buffer = new StringBuffer();
		StringBuffer sb = new StringBuffer();
		String dstSchema = schema;
		String columnList = "";
		String plSQL = "";
		String headView = "";

		if (dbSourceName.equalsIgnoreCase("oracle")) {
			headView = "CREATE OR REPLACE VIEW "
					+ IBMExtractUtilities.putQuote(dstSchema) + "."
					+ IBMExtractUtilities.putQuote(viewName) + linesep;
			plSQL = "select text from dba_views where owner = '" + schema
					+ "' and view_name = '" + viewName + "'";
		}

		if (plSQL.equals("")) {
			return "";
		}
		try {
			columnList = "(" + getColumnList(schema, viewName) + ")" + linesep
					+ " AS " + linesep;
			PreparedStatement queryStatement = mainConn.prepareStatement(plSQL);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			int objCount = 0;
			while (Reader.next()) {
				try {
					sb.setLength(0);
					IBMExtractUtilities.getStringChunks(Reader, 1, sb);
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

	private String genMEETViews(String schema) {
		String viewSQL = "";
		String viewTemplate = "";

		ResultSet Reader = null;
		ResultSet ViewDDLReader = null;
		StringBuffer buffer = new StringBuffer();

		if (dbSourceName.equalsIgnoreCase("oracle")) {
			viewTemplate = "select dbms_metadata.get_ddl('VIEW','&viewName&','&schemaName&') from dual";
			viewSQL = "select view_name from dba_views where owner = '"
					+ schema + "' and view_name not like 'AQ$%'";
		}

		if (viewSQL.equals(""))
			return "";

		try {
			PreparedStatement queryStatement = mainConn
					.prepareStatement(viewSQL);
			queryStatement.setFetchSize(fetchSize);
			Reader = queryStatement.executeQuery();
			int objCount = 0;
			while (Reader.next()) {
				String viewName = Reader.getString(1);
				String ddlSQL = viewTemplate.replace("&schemaName&", schema);
				ddlSQL = ddlSQL.replace("&viewName&", viewName);
				PreparedStatement viewStatement = mainConn
						.prepareStatement(ddlSQL);
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
						buffer.append(viewDDL);
						buffer.append(linesep + sqlTerminator + linesep);
					}
					if ((objCount > 0) && (objCount % 20 == 0))
						log(objCount
								+ " numbers of views extracted for MEET schema "
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
				log(objCount
						+ " Total numbers of views extracted for MEET schema "
						+ schema);
			if (Reader != null)
				Reader.close();
			if (queryStatement != null)
				queryStatement.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return buffer.toString();
	}

	private void genMEETOutput(String schema) {
		String sql = "";
		try {
			sql = "SELECT     DBMS_METADATA.GET_DDL('TABLE', table_name,'"
					+ schema + "') ddl_string "
					+ "FROM DBA_TABLES WHERE OWNER = '" + schema + "' "
					+ "AND NVL(IOT_TYPE,'X') != 'IOT_OVERFLOW'";

			db2MEETWriter.write(executeSQL(sql));
			log("Meet output for tables done");

			sql = "SELECT DBMS_METADATA.GET_DDL('REF_CONSTRAINT', CONSTRAINT_NAME, '"
					+ schema
					+ "') ddl_string "
					+ "FROM DBA_CONSTRAINTS WHERE OWNER = '"
					+ schema
					+ "' "
					+ "AND CONSTRAINT_TYPE = 'R'";

			db2MEETWriter.write(executeSQL(sql));
			log("Meet output for RI done");

			sql = "SELECT TO_CHAR(DBMS_METADATA.GET_DDL('INDEX', object_name,'"
					+ schema + "')) ddl_string "
					+ "FROM DBA_OBJECTS WHERE OWNER = '" + schema + "' "
					+ "AND OBJECT_TYPE = 'INDEX'";

			db2MEETWriter.write(executeSQL(sql));
			log("Meet output for indexes done");

			sql = "SELECT TO_CHAR(DBMS_METADATA.GET_DDL('SEQUENCE', object_name,'"
					+ schema
					+ "')) ddl_string "
					+ "FROM DBA_OBJECTS WHERE OWNER = '"
					+ schema
					+ "' "
					+ "AND OBJECT_TYPE = 'SEQUENCE'";

			db2MEETWriter.write(executeSQL(sql));
			log("Meet output for sequences done");

			sql = "SELECT TO_CHAR(DBMS_METADATA.GET_DDL('DB_LINK', object_name,'"
					+ schema
					+ "')) ddl_string "
					+ "FROM DBA_OBJECTS WHERE OWNER = '"
					+ schema
					+ "' "
					+ "AND OBJECT_TYPE = 'DATABASE LINK'";

			db2MEETWriter.write(executeSQL(sql));
			log("Meet output for DB Links done");

			sql = "SELECT DBMS_METADATA.GET_DDL('DIRECTORY', object_name, '"
					+ schema + "') ddl_string "
					+ "FROM DBA_OBJECTS WHERE OWNER = '" + schema + "' "
					+ "AND OBJECT_TYPE = 'DIRECTORY'";

			db2MEETWriter.write(executeSQL(sql));
			log("Meet output for Directory done");

			sql = "SELECT DBMS_METADATA.GET_DDL('MATERIALIZED_VIEW', object_name, '"
					+ schema
					+ "') ddl_string "
					+ "FROM DBA_OBJECTS WHERE OWNER = '"
					+ schema
					+ "' "
					+ "AND OBJECT_TYPE = 'MATERIALIZED VIEW'";

			db2MEETWriter.write(executeSQL(sql));
			log("Meet output for Materialized Views done");

			sql = "SELECT DBMS_METADATA.GET_DDL('MATERIALIZED_VIEW_LOG', object_name, '"
					+ schema
					+ "') ddl_string "
					+ "FROM DBA_OBJECTS WHERE OWNER = '"
					+ schema
					+ "' "
					+ "AND OBJECT_TYPE = 'MATERIALIZED VIEW LOG'";

			db2MEETWriter.write(executeSQL(sql));
			log("Meet output for Materialized View Logs done");

			sql = "SELECT DBMS_METADATA.GET_DDL('JAVA_SOURCE', object_name, '"
					+ schema + "') ddl_string "
					+ "FROM DBA_OBJECTS WHERE OWNER = '" + schema + "' "
					+ "AND OBJECT_TYPE = 'JAVA SOURCE'";

			db2MEETWriter.write(executeSQL(sql));
			log("Meet output for Java Source done");

			sql = "SELECT DBMS_METADATA.GET_DDL('SYNONYM', object_name, '"
					+ schema + "') ddl_string "
					+ "FROM DBA_OBJECTS WHERE OWNER = '" + schema + "' "
					+ "AND OBJECT_TYPE = 'SYNONYM'";

			db2MEETWriter.write(executeSQL(sql));
			log("Meet output for Synonym done");

			db2MEETWriter.write(genMEETViews(schema));
			log("Meet output for Views done");

			genMEETPLSQL(schema);
			log("Meet output for PL/SQL Objects done. *** END *** ");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void generateMeetOutput() {
		try {
			db2MEETWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR
					+ filesep + "meet.sql", false));
			String[] schemas = schemaList.split(":");
			if (majorSourceDBVersion > 8) {
				try {
					String schemaList = "";
					for (int idx = 0; idx < schemas.length; idx++) {
						if (idx > 0)
							schemaList = schemaList + ",";
						schemaList = schemaList + "'"
								+ IBMExtractUtilities.removeQuote(schemas[idx])
								+ "'";
					}
					genMEETOutputForPublicSchema(schemaList);
					for (int idx = 0; idx < schemas.length; idx++) {
						genMEETOutput(IBMExtractUtilities
								.removeQuote(schemas[idx]));
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				db2MEETWriter
						.write("Oracle version < 8 is not yet supported to generate MEET output. Please contact vikram.khatri@us.ibm.com"
								+ linesep);
			}
			db2MEETWriter.close();
			log("Work completed");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void log(String msg) {
		IBMExtractUtilities.log(msg);
	}

	private boolean collectInput() {
		boolean ok = false;
		IBMExtractConfig cfg = new IBMExtractConfig();
		cfg.loadConfigFile();

		server = cfg.getStringConsoleInput(server,
				"Enter Oracle Host Name or IP Address");
		port = cfg.getIntConsoleInput(port, "Enter Oracle's port number");
		dbName = cfg.getStringConsoleInput(dbName,
				"Enter Oracle Service Name or Instance Name");
		uid = cfg
				.getStringConsoleInput(uid, "Enter User ID of Oracle database");
		pwd = cfg.getStringConsoleInput(pwd, "Enter Oracle database Passsword");
		if (!cfg.getJDBCValidation(1, jdbcHome, "oracle"))
			return false;
		System.out
				.println("Now we will try to connect to Oracle to extract schema names.");
		if (cfg.getYesNoQuitConsoleInput(1,
				"Do you want to continue (Yes) : 1 ",
				"Quit program                  : 2 ", "Enter a number") == 2) {
			return false;
		}
		if (IBMExtractUtilities.TestConnection(true, true, "oracle", server,
				port, dbName, uid, pwd)) {
			String srcSchName2 = IBMExtractUtilities.GetSchemaList("oracle",
					server, port, dbName, uid, pwd);
			if (IBMExtractUtilities.Message.equals("")) {
				System.out.println("Oracle's schema List extracted="
						+ srcSchName2);
				if (cfg.getYesNoQuitConsoleInput(1, "Do you want to use '"
						+ uid.toUpperCase() + "' as schema : 1 ",
						"Or the extracted List                   : 2 ",
						"Enter a number") == 1) {
					schemaList = uid.toUpperCase();
				} else
					schemaList = srcSchName2;
				ok = true;
			} else {
				System.out.println(IBMExtractUtilities.Message);
			}
		} else {
			System.out.println(IBMExtractUtilities.Message);
		}
		return ok;
	}

	public static void runMeet() {
		GenerateMeet meet = new GenerateMeet();
		if (meet.collectInput()) {
			meet.initDBSources();
			meet.generateMeetOutput();
		}
	}

	public static void main(String[] args) {
		if (args.length < 6) {
			System.out
					.println("usage: java -Xmx600m -DOUTPUT_DIR=. ibm.ExtractMeet server dbname portnum uid pwd schemaList");

			System.exit(-1);
		}
		server = args[0];
		dbName = args[1];
		port = Integer.parseInt(args[2]);
		uid = args[3];
		pwd = args[4];
		schemaList = args[5];
		if (IBMExtractUtilities.isHexString(pwd))
			pwd = IBMExtractUtilities.Decrypt(pwd);
		fetchSize = 1000;
		String version = GenerateMeet.class.getPackage()
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
		log("dbSourceName:" + dbSourceName);
		log("server:" + server);
		log("dbName:" + dbName);
		log("port:" + port);
		log("uid:" + uid);
		log("fetchSize:" + fetchSize);
		log("Timezone = " + System.getProperty("user.timezone") + " Offset="
				+ IBMExtractUtilities.getTimeZoneOffset());
		GenerateMeet meet = new GenerateMeet();
		meet.initDBSources();
		meet.generateMeetOutput();
	}
}