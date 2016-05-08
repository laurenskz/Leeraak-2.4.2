package nl.hanze.db.io;

import java.io.File;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

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

		RandomAccessFile r = getRandomAccessFile();
		r.getChannel().position(r.getChannel().size());
		ByteBuffer contents = ByteBuffer.wrap(prepareRecord(record).getBytes());
		r.getChannel().write(contents);
		long e = System.currentTimeMillis();
		return e - s;
	}


	/*** OPGAVE 3g ***/
	@Override
	public long delete(String colname, String value) throws Exception {
		if(!colname.equals(def.getPK()))throw new Exception("Deleting non PK is not implemented");
		long s = System.currentTimeMillis();

		int location = pkValLocation(value);

		if(location==-1)throw new Exception("No record with col: " + colname + " and value " + value);

		byte[] bytes = rawRecordAt((int) numOfRecords() - 1).getBytes();
		getRandomAccessFile().seek(location*recordLength());
		getRandomAccessFile().write(bytes);
		getRandomAccessFile().getChannel().truncate((numOfRecords()-1)*recordLength());

		long e = System.currentTimeMillis();
		return e - s;
	}

	/*** OPGAVE 3h ***/
	@Override
	public long update(String[] record) throws Exception {
		long s = System.currentTimeMillis();
		int col = def.getColPosition(def.getPK());
		int location = pkValLocation(record[col]);
		if(location==-1)throw new Exception("No record found to update");
		getRandomAccessFile().seek(location*recordLength());
		getRandomAccessFile().write(prepareRecord(record).getBytes());
		long e = System.currentTimeMillis();
		return e - s;
	}

	/*** OPGAVE 3i ***/
	@Override
	public long search(String colname, String value, ArrayList<String[]> result)
			throws Exception {
		long s = System.currentTimeMillis();
		int col = def.getColPosition(colname);
		for (int i = 0; i < numOfRecords(); i++) {
			String[] record = recordAt(i);
			if(record[col].equals(value)){
				result.add(record);
			}
		}
		long e = System.currentTimeMillis();
		return e - s;
	}

	/*** OPGAVE 3c ***/
	public int pkValLocation(String pkval) throws Exception {
		int col = def.getColPosition(def.getPK());
		boolean found = false;
		int i = 0;
		for (i = 0; i < numOfRecords() && !found; i++)
			found = recordAt(i)[col].equals(pkval);

		return found ? i - 1 : -1;
	}

}
