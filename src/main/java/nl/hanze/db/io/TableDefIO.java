package nl.hanze.db.io;

import java.io.RandomAccessFile;

import nl.hanze.db.def.TableDefinition;

public class TableDefIO {

	public TableDefinition load(String tablename) throws Exception {
		RandomAccessFile raf = null;
		raf = BaseIO.getFile(tablename + ".def");
		String deftext = raf.readLine();
		raf.close();

		String[] splitdeftext = deftext.split("#");
		if (splitdeftext.length % 3 != 1) {
			throw new Exception("Illegal table definition");
		}

		TableDefinition def = new TableDefinition(tablename);
		for (int i = 1; i < splitdeftext.length; i += 3) {
			String name = splitdeftext[i];
			Integer type = new Integer(splitdeftext[i + 1]);
			switch (type) {
			case TableDefinition.INTEGER_TYPE:
				def.addIntCol(name);
				break;
			case TableDefinition.STRING_TYPE:
				def.addStringCol(name);
				break;
			default:
				throw new Exception("Illegal table definition");
			}
		}
		def.setPK(splitdeftext[1]);
		return def;
	}

	// store the .def file
	public void save(TableDefinition def) throws Exception {
		String name = def.getTableName();
		String pk = def.getPK();
		if (pk == null) {
			throw new Exception("Primary key not defined");
		}
		String[] colname = def.getColnames();
		Integer[] type = def.getTypes();
		Integer[] size = def.getSizes();

		BaseIO.openFile(name + ".def");

		StringBuffer temp = new StringBuffer();
		temp.append(pk);
		for (int i = 0; i < colname.length; i++) {
			temp.append("#");
			temp.append(colname[i]);
			temp.append("#");
			temp.append(type[i]);
			temp.append("#");
			temp.append(size[i]);
		}

		RandomAccessFile raf = BaseIO.getFile(name + ".def");

		raf.write(temp.toString().getBytes());
		raf.close();
	}
}
