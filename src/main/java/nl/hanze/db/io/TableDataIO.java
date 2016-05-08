package nl.hanze.db.io;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import nl.hanze.db.def.TableDefinition;

public abstract class TableDataIO {
	protected TableDefinition def;

	public TableDataIO(TableDefinition def) {
		this.def = def;
	}

	protected String appendSpaces(String s, int length) {
		StringBuffer temp = new StringBuffer();
		temp.append(s);
		for (int i = 0; i < length - s.length(); i++) {
			temp.append(' ');
		}

		return temp.toString();
	}

	protected String stripSpaces(String s) {
		return s.trim();
	}

	protected String prepareRecord(String[] record) throws Exception {
		StringBuffer temp = new StringBuffer();
		Integer[] size = def.getSizes();
		if (size.length != record.length) {
			throw new Exception("Malformed record");
		}

		for (int i = 0; i < record.length; i++) {
			temp.append(appendSpaces(record[i], size[i]));
			if (i != record.length - 1) {
				temp.append("#");
			}
		}
		temp.append("\r\n");

		return temp.toString();
	}

	protected long recordLength() throws Exception {
		int recordlength = 0;
		for (int size : def.getSizes()) {
			recordlength += size;
		}
		// for each column add a # except last one
		recordlength += def.getSizes().length - 1;
		// add LF = "\r\n"
		recordlength += 2;
		return recordlength;
	}

	protected long numOfRecords() throws Exception {
		long length = getRandomAccessFile().length();
		return length / recordLength();
	}

	/*** OPGAVE 3b ***/
	public String[] recordAtOld(int i) throws Exception {
        if (i < 0 || i >= numOfRecords())
			throw new Exception("Record position out of bounds");

		BufferedReader buf = new BufferedReader(new FileReader(
				BaseIO.getInitDir() + File.separator + def.getTableName()
						+ ".tbl"));
		buf.skip(i * recordLength());
        return parseLine(buf.readLine());
	}

    private String[] parseLine(String sLine) throws IOException {
        String[] temp = sLine.split("#");
        for (int j = 0; j < temp.length; j++) {
            temp[j] = stripSpaces(temp[j]);
        }
        return temp;
    }



    public String[] recordAt(int i) throws Exception {
        String line = rawRecordAt(i);
        return parseLine(line);
    }

    protected String rawRecordAt(int i) throws Exception {
        if (i < 0 || i >= numOfRecords())
            throw new Exception("Record position out of bounds");
        RandomAccessFile randomAccessFile = getRandomAccessFile();//Open a randomaccesfile
        randomAccessFile.getChannel().position(i*recordLength());
        ByteBuffer buffer = ByteBuffer.allocate((int)recordLength());
        randomAccessFile.getChannel().read(buffer);
        return new String(buffer.array());
    }

    protected RandomAccessFile getRandomAccessFile() throws Exception {
        return BaseIO.getFile(def.getTableName()
                + ".tbl");
    }

    public abstract long add(String[] record) throws Exception;

	public abstract long delete(String colname, String value) throws Exception;

	public abstract long update(String[] record) throws Exception;

	public abstract long search(String colname, String value,
			ArrayList<String[]> result) throws Exception;

}
