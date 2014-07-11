//package ibm;
//
//import com.ibm.jzos.CatalogSearch;
//import com.ibm.jzos.CatalogSearch.Entry;
//import com.ibm.jzos.CatalogSearchField;
//import com.ibm.jzos.ZFile;
//import java.io.PrintStream;
//
//public class Jd {
//	private static void delDataset(String dsnName) throws Exception {
//		if (ZFile.dsExists("//'" + dsnName + "'")) {
//			ZFile.remove("//'" + dsnName + "'");
//		}
//	}
//
//	public static void main(String[] args) throws Exception {
//		if (args.length != 1) {
//			System.out.println("USAGE: ibm.Jd <filter_key>");
//			System.out.println("USAGE: ibm.Jd \"DNET770.TBLDATA.**\"");
//			System.out.println("USAGE: ibm.Jd \"DNET770.TBLDATA.**.CERR\"");
//			System.out.println("USAGE: ibm.Jd \"DNET770.TBLDATA.**.LERR\"");
//			System.out.println("USAGE: ibm.Jd \"DNET770.TBLDATA.**.DISC\"");
//			System.exit(1);
//		}
//		String filterKey = args[0].toUpperCase();
//
//		CatalogSearch catSearch = new CatalogSearch(filterKey);
//		catSearch.addFieldName("ENTNAME");
//		catSearch.search();
//
//		int datasetCount = 0;
//		while (catSearch.hasNext()) {
//			CatalogSearch.Entry entry = (CatalogSearch.Entry) catSearch.next();
//			if (entry.isDatasetEntry()) {
//				datasetCount++;
//				CatalogSearchField field = entry.getField("ENTNAME");
//				String dsn = field.getFString().trim();
//				delDataset(dsn);
//				System.out.println("DSN=" + dsn + " deleted");
//			}
//		}
//	}
//}