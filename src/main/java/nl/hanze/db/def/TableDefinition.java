package nl.hanze.db.def;

import java.util.ArrayList;

public class TableDefinition {
	// to keep things simple, all fields have fixed size
	public static final int INTEGER_SIZE = 8;
	public static final int STRING_SIZE = 18;
	public static final int INTEGER_TYPE = 0;
	public static final int STRING_TYPE = 1;

	private String tablename;
	private ArrayList<String> colname;
	private ArrayList<Integer> type;
	private ArrayList<Integer> size;
	private String pk;

	public TableDefinition(String tablename) {
		this.tablename = tablename;
		colname = new ArrayList<String>();
		type = new ArrayList<Integer>();
		size = new ArrayList<Integer>();
	}

	public void addIntCol(String name) throws Exception {
		if (colname.contains(name)) {
			throw new Exception("Column already defined");
		}
		colname.add(name);
		type.add(TableDefinition.INTEGER_TYPE);
		size.add(TableDefinition.INTEGER_SIZE);
	}

	public void addStringCol(String name) throws Exception {
		if (colname.contains(name)) {
			throw new Exception("Column already defined");
		}
		colname.add(name);
		type.add(TableDefinition.STRING_TYPE);
		size.add(TableDefinition.STRING_SIZE);
	}

	public String getTableName() {
		return tablename;
	}

	public String[] getColnames() {
		return colname.toArray(new String[0]);
	}

	public Integer[] getTypes() {
		return type.toArray(new Integer[0]);
	}

	public Integer[] getSizes() {
		return size.toArray(new Integer[0]);
	}

	public void setPK(String pk) throws Exception {
		if (!colname.contains(pk)) {
			throw new Exception("Primary key does not exist");
		}
		if (getType(pk) != TableDefinition.INTEGER_TYPE) {
			throw new Exception("Primary key have type INTEGER");
		}
		this.pk = pk;
	}

	public String getPK() {
		return pk;
	}

	public int getColPosition(String name) {
		for (int i = 0; i < colname.size(); i++) {
			if (colname.get(i).equals(name)) {
				return i;
			}
		}
		return -1;
	}

	public int getType(String name) throws Exception {
		int pos = getColPosition(name);
		if (pos == -1) {
			throw new Exception("Column not found");
		}
		return type.get(pos);
	}

	public int getSize(String name) throws Exception {
		int pos = getColPosition(name);
		if (pos == -1) {
			throw new Exception("Column not found");
		}
		return size.get(pos);
	}

}
