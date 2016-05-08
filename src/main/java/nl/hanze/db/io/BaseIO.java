package nl.hanze.db.io;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.HashMap;

import nl.hanze.db.test.Tester;

public class BaseIO {
	private final static String PATH = "Tables" + File.separator;
	private static String initDir;
	private static HashMap<String, RandomAccessFile> openFiles;

	/**
	 * Creates a directory and defines a HashMap openFiles which contains the
	 * open files in this directory
	 * 
	 * @param init
	 *            the name of the directory
	 */

	public static void setInitDir(String init) {
		initDir = PATH + init;
		new File(initDir).mkdirs();
		openFiles = new HashMap<String, RandomAccessFile>();
	}

	public static String getInitDir() {
		return initDir;
	}

	/**
	 * Opens a file and place in in the HashMap openFiles under a key equal to
	 * fileName.
	 * 
	 * @param fileName
	 *            the name of the file.
	 */
	public static void openFile(String fileName) throws Exception {
		if (openFiles.containsKey(fileName)) {
			System.out.println("File " + fileName + "already open");
			return;
		}

		File tmpFile = new File(getInitDir() + File.separator + fileName);

		// If add-procedure is tested, remove the file if it exists, start from
		// scratch
		if (Tester.TEST_ADD) {
			if (tmpFile.exists()) {
				tmpFile.delete();
			}
			tmpFile.createNewFile();
		}

		openFiles.put(fileName, new RandomAccessFile(tmpFile, "rw"));
	}

	/**
	 * close the file by removing its pointer from openFiles.
	 * 
	 * @param fileName
	 *            the name of the file.
	 */
	public static void closeFile(String fileName) {
		if (openFiles.containsKey(fileName)) {
			openFiles.remove(fileName);
		}
	}

	/**
	 * @param fileName
	 *            name of requested file.
	 * @return requested File object.
	 */
	public static RandomAccessFile getFile(String fileName) throws Exception {
		if (!openFiles.containsKey(fileName)) {
			openFile(fileName);
		}
		return openFiles.get(fileName);
	}

}
