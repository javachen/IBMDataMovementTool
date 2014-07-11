package ibm;

import java.text.SimpleDateFormat;
import java.util.Date;

public class GenInput {
	private static final SimpleDateFormat timestampFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH.mm.ss.SSS");

	private static void log(String msg) {
		System.out.println("[" + timestampFormat.format(new Date()) + "] "
				+ msg);
	}

	public static void main(String[] args) {
		if (args.length < 8) {
			System.out
					.println("usage: java ibm.GenInput dbSourceName db2SchemaName srcSchemaName server dbname portnum uid pwd");
			System.out
					.println("srcSchemaName: Acts like a filter to extract source schemas. If 'ALL', extract all schemas from the source database");
			System.out
					.println("db2SchemaName: if srcSchemaName=ALL, db2SchemaName=ALL, Treat source schema names same as it is in DB2.");
			System.out
					.println("srcSchemaName: if srcSchemaName is not ALL and db2SchemaName=ALL, treat source schema name as it is in DB2.");
			System.out
					.println("srcSchemaName: if srcSchemaName is not ALL and db2SchemaName is not ALL, translate source schema name to name specified by db2SchemaName.");
			System.out
					.println("For MS Access, srcSchemaName is ignored since access does not have concept of schema but db2SchemaName is required for a schema name in DB2");
			System.exit(-1);
		}
		String dbSourceName = args[0];
		String db2SchemaName = args[1];
		String srcSchemaName = args[2];
		String server = args[3];
		String dbName = args[4];
		int port = Integer.parseInt(args[5]);
		String uid = args[6];
		String pwd = args[7];
		if (IBMExtractUtilities.isHexString(pwd))
			pwd = IBMExtractUtilities.Decrypt(pwd);
		if (srcSchemaName.equalsIgnoreCase("ALL"))
			db2SchemaName = "ALL";
		db2SchemaName = db2SchemaName.toUpperCase();
		if (dbSourceName.equalsIgnoreCase("domino"))
			dbName = server;
		log("dbSourceName:" + dbSourceName);
		log("db2SchemaName:" + db2SchemaName);
		log("srcSchemaName:" + srcSchemaName);
		log("server:" + server);
		log("dbName:" + dbName);
		log("port:" + port);
		log("uid:" + uid);

		IBMExtractUtilities.CreateTableScript(dbSourceName, db2SchemaName,
				srcSchemaName, server, port, dbName, uid, pwd);
	}
}