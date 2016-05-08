package nl.hanze.db.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import nl.hanze.db.def.TableDefinition;

/*
 * format of idx-record = #idx-record-nr#delete-flag#column-value#tbl-record-nr#null or nr-of-next-idx-record-in-list
 * 
 * delete-flag = 'D' or ' '; 'D' means this value is deleted; if an idx-record is deleted, it can later be reused
 * 
 * examples : 	"900000  # #10000     #50000   #null    " 
 * 				"900000  # #10000     #50000   #null    "
 * 				"900000  #D#10000     #50000   #null    " 
 * 				"900000  # #10000     #50000   #99999   " 
 * 
 * note on LF : 
 * on linux PrintWriter.println appends LF=0x0a="\n", on windows println appends LF=0x0d0a="\r\n" here always "\r\n" is used,
 * since it works on linux as well (same as in TableDataIO.java) so not using System.getProperty("line.separator")
 * 
 * record_size = 8+3+column-size+1+8+1+8+2 (fixed part = 31)
 */

@SuppressWarnings("unused")
public class TableDataIO_Indexed extends TableDataIO {
	// size must be < 10.000.000
	private final int hashsize = 60000;
	private final int record_size_fixed_part = 22;
	private RandomAccessFile indexFile;

	private class Positions {
		public int index;
		public int table;

		public Positions() {
			index = 0;
			table = 0;
		}
	}

	public TableDataIO_Indexed(TableDefinition def) throws Exception {
		super(def);
		indexFile = BaseIO.getFile(def.getTableName() + "_" + def.getPK()
				+ ".idx");
	}

	public int idxFixedRecordLength() {
		return this.record_size_fixed_part;
	}

	public int pkValLocation(String pkval) throws Exception {
		int col = def.getColPosition(def.getPK());
		boolean found = false;
		int i = 0;
		for (i = 0; i < numOfRecords() && !found; i++)
			found = recordAt(i)[col].equals(pkval);

		return found ? i - 1 : -1;
	}

	/*** OPGAVE 4e / implementeer add() ***/
	@Override
	public long add(String[] record) throws Exception {
		long s = System.currentTimeMillis();

		int pkpos = pkValLocation(record[def.getColPosition(def.getPK())]);
		if (pkpos != -1)
			throw new Exception("Primary key already in table");

		RandomAccessFile r = getRandomAccessFile();
		r.getChannel().position(r.getChannel().size());
		ByteBuffer contents = ByteBuffer.wrap(prepareRecord(record).getBytes());
		r.getChannel().write(contents);

		addIndexEntry(def.getColPosition(def.getPK()), record[def.getColPosition(def.getPK())]);

		long e = System.currentTimeMillis();
		return e - s;
	}

	/*** OPGAVE 4e / implementeer delete() ***/
	@Override
	public long delete(String colname, String value) throws Exception {
		long s = System.currentTimeMillis();

		// get the index record size
		Integer[] size = def.getSizes();
		int pkindex = def.getColPosition(def.getPK());
		int recordSize = idxFixedRecordLength() + size[pkindex];

		// get the hashcode of the value
		String s_value = value.trim();
		int indexPosition = hash(s_value);

		// go to the first position
		indexFile.seek(indexPosition * recordSize);
		RandomAccessFile table = getRandomAccessFile();

		String[] indexRecord;
		do {
			// read the info from the index
			String line = indexFile.readLine();
			indexRecord = line.split("#");

			// go to the location inside the table
			table.seek(recordLength() * Integer.parseInt(indexRecord[3]));

			// delete the line from the table and the index
			byte[] bytes = rawRecordAt((int) numOfRecords() - 1).getBytes();
			getRandomAccessFile().write(bytes);
			getRandomAccessFile().getChannel().truncate((numOfRecords()-1)*recordLength());
			deleteIndexEntry(colname, Integer.parseInt(indexRecord[1]));

		} while (indexRecord[4] != null);

		long e = System.currentTimeMillis();
		return e - s;
	}

	/*** OPGAVE 4e / implementeer update() ***/
	@Override
	public long update(String[] record) throws IOException, Exception { // TODO
		long s = System.currentTimeMillis();

		// get the index record size
		Integer[] size = def.getSizes();
		int pkindex = def.getColPosition(def.getPK());
		int recordSize = idxFixedRecordLength() + size[pkindex];

		// get the hashcode of the value
		String s_value = record[def.getColPosition(def.getPK())].trim();
		int indexPosition = hash(s_value);

		// go to the first position
		indexFile.seek(indexPosition * recordSize);
		RandomAccessFile table = getRandomAccessFile();

		// read the info from the index
		String line = indexFile.readLine();
		String[] indexRecord = line.split("#");

		// go to the record and write to it
		table.seek(recordLength() * Integer.parseInt(indexRecord[3]));
		table.write(prepareRecord(record).getBytes());

		long e = System.currentTimeMillis();
		return e - s;
	}

	/*** OPGAVE 4e / implementeer search ***/
	@Override
	public long search(String colname, String value, ArrayList<String[]> result)
			throws Exception {
		long s = System.currentTimeMillis();

		// get the index record size
		Integer[] size = def.getSizes();
		int pkindex = def.getColPosition(def.getPK());
		int indexRecordSize = idxFixedRecordLength() + size[pkindex];

		// get the hashcode of the value
		String s_value = value.trim();
		int indexPosition = hash(s_value);

		// go to the first position
		indexFile.seek(indexPosition * indexRecordSize);
		RandomAccessFile table = getRandomAccessFile();

		String[] tmp;
		do {
			// read the info from the index
			String line = indexFile.readLine();
			tmp = line.split("#");

			table.seek(recordLength() * Integer.parseInt(tmp[3]));

			// read the from the table and add them to the results ArrayList
			line = table.readLine();
			String[] parts = line.split("#");
			result.add(parts);

		} while (tmp[4] != null);

		long e = System.currentTimeMillis();
		return e - s;
	}

	/**
	 * Create an index file for the specified column.
	 * 
	 * @return the time it took to run this method.
	 * @throws Exception
	 */
	public long createIndexFile() throws Exception {
		long s = System.currentTimeMillis();
		// fill idx file with hashsize empty records/lines (empty means filled
		// with spaces)

		int pkindex = def.getColPosition(def.getPK());
		if (pkindex == -1) {
			throw new Exception("Primary key does not exist");
		}
		Integer[] size = def.getSizes();
		int recordSize = idxFixedRecordLength() + size[pkindex];

		StringBuffer temp = new StringBuffer();
		for (int i = 0; i < recordSize - 2; i++) {
			temp.append(" ");
		}

		indexFile.seek(0);
		for (int i = 0; i < this.hashsize; i++) {
			indexFile.writeBytes(temp + "\r\n");
		}

		int table_pos = 0;
		BufferedReader buf = new BufferedReader(getFileReader());
		String sLine = null;
		while ((sLine = buf.readLine()) != null) {
			// get column value (don't strip the spaces)
			String pkvalue = sLine.split("#")[pkindex];
			addIndexEntry(table_pos, pkvalue);
			table_pos++;
		}
		buf.close();

		long e = System.currentTimeMillis();
		return e - s;
	}

	/**
	 * Obtains the index entry of the specified column. returns the record
	 * number through hashing the column value method is based on a simple hash
	 * function from Robert Sedgwicks book "Algorithms in C" (URL:
	 * http://www.partow.net/programming/hashfunctions/index.html)
	 * 
	 * @param column
	 *            the column's name
	 * @param value
	 *            the value to obtain the index entry for
	 * @throws Exception
	 */
	private int hash(String column) throws Exception {
		int b = 378551;
		int a = 63689;
		long hash = 0;

		for (int i = 0; i < column.length(); i++) {
			hash = hash * a + column.charAt(i);
			a = a * b;
		}

		hash = hash % this.hashsize;
		hash = Math.abs(hash);

		return (int) hash;
	}

	/**
	 * Create an index entry in the index file of the specified column.
	 * 
	 * @param tablePosition
	 *            position or line nr in table *.tbl
	 * @param pkValue
	 *            the value of the primary key to create an index entry for
	 * @throws Exception
	 */

	/* TODO refactor! */
	protected void addIndexEntry(int tablePosition, String pkValue)
			throws Exception {
		checkForIndex();
		int pkindex = def.getColPosition(def.getPK());
		checkForPrimaryKey(pkindex);
		Integer[] size = def.getSizes();
		checkPKValueLength(pkValue);

		pkValue = appendSpaces(pkValue, TableDefinition.INTEGER_SIZE);

		// calculate index = hash value
		String s_value = pkValue.trim();
		int indexPosition = hash(s_value);
		int recordSize = idxFixedRecordLength() + size[pkindex];

		indexFile.seek(indexPosition * recordSize);

		String indexRecord = "";
		String line = indexFile.readLine();

		if (line.substring(0, 1).equals(" ")) {
			// empty record, reset file pointer and fill record
			indexFile.seek(indexPosition * recordSize);

			String indexPositionFormatted = String.format("%-5s",
					Integer.toString(indexPosition));
			String tablePositionFormatted = String.format("%-5s",
					Integer.toString(tablePosition));

			indexRecord = indexPositionFormatted + "# #" + pkValue + "#"
					+ tablePositionFormatted + "#" + "null " + "\r\n";
		} else {
			String[] parts = line.split("#");
			if (parts[1].equals("D")) {
				// Deleted record, reset file pointer, fill record but keep
				// previous link !
				indexFile.seek(indexPosition * recordSize);

				String indexPositionFormatted = String.format("%-5s",
						Integer.toString(indexPosition));
				String tablePositionFormatted = String.format("%-5s",
						Integer.toString(tablePosition));

				indexRecord = indexPositionFormatted + "# #" + pkValue + "#"
						+ tablePositionFormatted;
			} else {
				// Collision found ! a valid record is found, so add new record
				// at EOF
				// Calculate new record number
				int newIndexPosition = (int) (indexFile.length() / recordSize);

				String newIndexPositionFormatted = String.format("%-5s",
						Integer.toString(newIndexPosition));
				String tablePositionFormatted = String.format("%-5s",
						Integer.toString(tablePosition));

				// reset file pointer and update the current record
				indexFile.seek((indexPosition * recordSize)
						+ (recordSize - 2 - 5));
				indexFile
						.write(newIndexPositionFormatted.toString().getBytes());

				// move file pointer to EOF and append new record
				indexFile.seek(indexFile.length());
				indexRecord = newIndexPositionFormatted + "# #" + pkValue + "#"
						+ tablePositionFormatted + "#" + "null " + "\r\n";
			}
		}

		indexFile.writeBytes(indexRecord);
	}

	/**
	 * Search index file for a primary key value
	 * 
	 * @param pkvalue
	 *            the primary key value to search for
	 * @param position
	 *            [0] = index entry, [1] = table row
	 * @throws Exception
	 */

	/* TODO: refactor! */
	public boolean searchIndex(String pkvalue, Positions pos) throws Exception {
		boolean found = false;
		boolean end = false;

		checkForIndex();
		int pkindex = def.getColPosition(def.getPK());
		checkForPrimaryKey(pkindex);

		// calculate index = hash value
		String s_value = pkvalue.trim();
		int index = hash(s_value);

		Integer[] size = def.getSizes();
		int recordSize = idxFixedRecordLength() + size[pkindex];

		indexFile.seek(index * recordSize);
		String line = indexFile.readLine();

		if (line.substring(0, 1).equals(" ")) {
			// Empty record, end of search
			found = false;
			return found;
		}

		String[] parts = line.split("#");
		String s_part = parts[2].trim();
		if (s_part.equals(pkvalue) && !(parts[1].equals("D"))) {
			found = true;
			pos.index = Integer.parseInt(parts[0].trim());
			pos.table = Integer.parseInt(parts[3].trim());
		}
		while (!found && !end) {
			if (parts[4].substring(0, 4).equals("null")) {
				// end of linked list
				end = true;
				found = false;
			} else {
				index = Integer.parseInt(parts[4].trim());
				indexFile.seek(index * recordSize);
				line = indexFile.readLine();
				parts = line.split("#");
				if (parts[2].trim().equals(pkvalue) && !(parts[1].equals("D"))) {
					found = true;
					pos.index = Integer.parseInt(parts[0].trim());
					pos.table = Integer.parseInt(parts[3].trim());
				}
			}
		}
		return found;
	}

	/**
	 * Delete an index entry in the idx file of the specified column.
	 * 
	 * @param colname
	 *            the column's name
	 * @param idxPos
	 *            index entry (line nr) to be deleted
	 * @throws Exception
	 */

	/* TODO: refactor */
	protected long deleteIndexEntry(String colname, int idxPos)
			throws Exception {
		long s = System.currentTimeMillis();
		if (!indexExists(colname)) {
			throw new Exception("No index created");
		}
		int pkindex = def.getColPosition(colname);
		if (pkindex == -1) {
			throw new Exception("Column does not exist");
		}
		Integer[] size = def.getSizes();
		int recordSize = idxFixedRecordLength() + size[pkindex];

		indexFile.seek(idxPos * recordSize);
		String sLine = indexFile.readLine();
		String[] parts = sLine.split("#");
		if (Integer.parseInt(parts[0].trim()) != idxPos) {
			throw new Exception("Index not found in index file");
		} else {
			indexFile.seek(idxPos * recordSize + 6);
			String flag = "D";
			indexFile.write(flag.toString().getBytes());
		}
		long e = System.currentTimeMillis();
		return e - s;
	}

	protected boolean indexExists(String colname) {
		return new File(BaseIO.getInitDir() + File.separator
				+ def.getTableName() + "_" + colname + ".idx").exists();
	}

	private boolean checkForIndex() throws Exception {
		if (!indexExists(def.getPK())) {
			throw new Exception("No index created");
		}
		return true;
	}

	private boolean checkForPrimaryKey(int indexnum) throws Exception {
		if (indexnum == -1) {
			throw new Exception("Primary key does not exist");
		}

		return true;
	}

	private boolean checkPKValueLength(String value) throws Exception {
		if (value.length() > TableDefinition.INTEGER_SIZE) {
			throw new Exception("Supplied pkValue too large");
		}

		return true;
	}

	private FileReader getFileReader() throws FileNotFoundException {
		return new FileReader(BaseIO.getInitDir() + File.separator
				+ def.getTableName() + ".tbl");
	}
}
