package ibm;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * 查看版本
 *
 * @author: <a href="mailTo:javachencto@163.com">JavaChen</a>
 * @date: 2011-3-25 下午04:10:55
 * @version: 1.0
 *
 */
public class Version {
	public static void shortVersion() {
		try {
			String version = Version.class.getPackage()
					.getImplementationVersion();
			BufferedWriter writer = new BufferedWriter(new FileWriter(
					"./UploadedVersion.txt", false));
			writer.write(version==null?"v1.0":version);
			writer.close();
		} catch (IOException e) {
		}
	}

	public static void main(String[] args) {
		System.out.println("IBMDataMovementTool Version "
				+ Version.class.getPackage().getImplementationVersion());
		System.out.println("Author: Vikram S Khatri vikram.khatri@us.ibm.com");
		System.out
				.println("This software is for data migration of following databases to DB2");
		System.out.println("Oracle        ==> DB2");
		System.out.println("SQL Server    ==> DB2");
		System.out.println("Sybase ASE    ==> DB2");
		System.out.println("MySQL         ==> DB2");
		System.out.println("PostgreSQL    ==> DB2");
		System.out.println("MS Access     ==> DB2");
		System.out.println("DB2           ==> DB2");

	}
}