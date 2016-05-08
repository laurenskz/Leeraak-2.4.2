package nl.hanze.db.io;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;

import nl.hanze.db.def.TableDefinition;

public class TableDataIO_Unsorted extends TableDataIO {
	public TableDataIO_Unsorted(TableDefinition def) throws Exception {
		super(def);
	}

	/**** OPGAVE 3e ***/
	@Override
	public long add(String[] record) throws Exception {
		long s = System.currentTimeMillis();

		int pkpos = pkValLocation(record[def.getColPosition(def.getPK())]);
		if (pkpos != -1)
			throw new Exception("Primary key already in table");

		FileWriter fw = new FileWriter(BaseIO.getInitDir() + File.separator
				+ def.getTableName() + ".tbl", true);
		StringBuffer temp = new StringBuffer();
		Integer[] size = def.getSizes();
		if (size.length != record.length) {
			fw.close();
			throw new Exception("Malformed record");
		}

		for (int i = 0; i < record.length; i++) {
			temp.append(appendSpaces(record[i], size[i]));
			if (i != record.length - 1) {
				temp.append("#");
			}
		}
		temp.append("\r\n");

		fw.write(temp.toString());
		fw.close();
		long e = System.currentTimeMillis();
		return e - s;
	}

	/*** OPGAVE 3g ***/
	@Override
	public long delete(String colname, String value) throws Exception {
		long s = System.currentTimeMillis();

		long e = System.currentTimeMillis();
		return e - s;
	}

	/*** OPGAVE 3h ***/
	@Override
	public long update(String[] record) throws Exception {
		long s = System.currentTimeMillis();

		long e = System.currentTimeMillis();
		return e - s;
	}

	/*** OPGAVE 3i ***/
	@Override
	public long search(String colname, String value, ArrayList<String[]> result)
			throws Exception {
		long s = System.currentTimeMillis();

		long e = System.currentTimeMillis();
		return e - s;
	}

	/*** OPGAVE 3c ***/
	private int pkValLocation(String pkval) throws Exception {
		int col = def.getColPosition(def.getPK());
		boolean found = false;
		int i = 0;
		for (i = 0; i < numOfRecords() && !found; i++)
			found = recordAt(i)[col].equals(pkval);

		return found ? i - 1 : -1;
	}

}
