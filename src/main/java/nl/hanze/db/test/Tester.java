package nl.hanze.db.test;

import java.util.ArrayList;
import java.util.Arrays;

import nl.hanze.db.def.*;
import nl.hanze.db.io.*;

public class Tester
{
	private TableDefinition tableDefinition;
	private TableDefIO tableDefIO;
	
	// gebruik onderstaande enum om aan te geven welk type je op dit moment wilt testen.
	private enum databaseMode {
		UNSORTED,
		INDEXED
	}
	public static final databaseMode testMode = databaseMode.UNSORTED;
	
	private static final int numberOfRecords = 10000;
	
	//gebruik onderstaande switches om te bepalen welke test moeten worden uitgevoerd.
	public static final boolean TEST_ADD = true;
	public static final boolean TEST_SEARCH_PK = true;
	public static final boolean TEST_SEARCH_NON_PK = true;
	public static final boolean TEST_UPDATE = true;
	public static final boolean TEST_DELETE = true;

	// gebruik deze switch om aan te geven of je bij elke iteratie info in je console wilt hebben.
	private static final boolean DISPLAY_ITERATION_INFO = false;

	private static final String UNSORTED_DIRECTORY = "./unsorted";
	private static final String INDEXED_DIRECTORY = "./indexed";

	public Tester()
	{
		tableDefIO = new TableDefIO();
	}

	public static void main(String[] args) throws Exception
	{
		Tester t = new Tester();
		switch(testMode){
		case UNSORTED:
			t.testUnsorted();
			break;
		case INDEXED:
			t.testIndexed();
			break;
		}
	}

	public void testUnsorted() throws Exception
	{
		BaseIO.setInitDir(UNSORTED_DIRECTORY);

		createTestDefinition();
		TableDataIO tdu = new TableDataIO_Unsorted(tableDefinition);

		runTests(tdu);
	}

	public void testIndexed() throws Exception
	{
		BaseIO.setInitDir(INDEXED_DIRECTORY);
		createTestDefinition();

		// create the index file
		TableDataIO_Indexed tdi = new TableDataIO_Indexed(tableDefinition);
		tdi.createIndexFile();

		runTests(tdi);
	}

	private void runTests(TableDataIO td) throws Exception
	{
//		testRecordAt(td);
		testAdd(td);
		testSearchNonPK(td);
		testSearchPK(td);
		testUpdate(td);
		testDelete(td);
	}

	private void createTestDefinition() throws Exception
	{
		tableDefinition = new TableDefinition("products");
		tableDefinition.addIntCol("PID");
		tableDefinition.addStringCol("Name");
		tableDefinition.addStringCol("Description");
		tableDefinition.addIntCol("InternalCode");
		tableDefinition.setPK("PID");
		
		tableDefIO.save(tableDefinition);
	}

	private void testAdd(TableDataIO td) throws Exception
	{
		long s;
		long e;
		if (TEST_ADD)
		{
			s = System.currentTimeMillis();
			String[] record = null;

			for (int i = 0; i < numberOfRecords; i++)
			{
				record = new String[] { "" + (numberOfRecords * 2 - 2 * i), "Naam", "Product", "" + ((2 * i + 1) % 11) };
				td.add(record);
				if (i % 100 == 0 && DISPLAY_ITERATION_INFO)
				{
					System.out.println("INSERT " + i);
				}
			}

			e = System.currentTimeMillis();
			System.out.println("Inserten van " + numberOfRecords + " records duurde " + (e - s) + " ms");
		}
	}

	private void testRecordAt(TableDataIO td){
		try {
			td.delete("PID","1500");
			if(true)return;
			long time = System.currentTimeMillis();
			for (int i = 0; i < 800; i++) {
				td.recordAt(i);
			}
			System.out.println(System.currentTimeMillis()-time);
			time = System.currentTimeMillis();
			for (int i = 0; i < 800; i++) {
				td.recordAtOld(i);
			}
			System.out.println(System.currentTimeMillis()-time);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void testSearchPK(TableDataIO td) throws Exception
	{
		long s;
		long e;
		if (TEST_SEARCH_PK)
		{
			s = System.currentTimeMillis();
			int numberOfTests = 5000;
			int totalResults = 0;
			ArrayList<String[]> result;
			
			for(int n = 0; n < numberOfTests; n++)
			{
				result = new ArrayList<String[]>();
				td.search("PID", "" + (n+1)*2 , result);
				totalResults += result.size();
				
				if(DISPLAY_ITERATION_INFO)
				{
					System.out.println("Search (on PK) " + n + " returned " + result.size() + " elements");
				}
			}

			e = System.currentTimeMillis();
			System.out.println("Search (on PK) ("+numberOfTests+" keer) met gemiddeld "+ Math.ceil(totalResults/numberOfTests) + " resultaten duurde " + (e - s) + " ms");
		}
	}
	
	private void testSearchNonPK(TableDataIO td) throws Exception
	{
		long s;
		long e;
		if (TEST_SEARCH_NON_PK)
		{
			s = System.currentTimeMillis();
			int numberOfTests = 100;
			int totalResults = 0;
			ArrayList<String[]> result;
			
			for(int n = 0; n < numberOfTests; n++)
			{
				result = new ArrayList<String[]>();
				td.search("InternalCode", "" + (n%11) , result);
				totalResults += result.size();
				
				if(DISPLAY_ITERATION_INFO)
				{
					System.out.println("Search (on non-PK) " + n + " returned " + result.size() + " elements");
				}
			}

			e = System.currentTimeMillis();
			System.out.println("Search (on non-PK) ("+numberOfTests+" keer) met gemiddeld "+ totalResults/numberOfTests + " resultaten duurde " + (e - s) + " ms");
		}
	}

	private void testUpdate(TableDataIO td) throws Exception
	{
		long s;
		long e;
		if (TEST_UPDATE)
		{
			int from = 2000;
			int to = 5000;
			s = System.currentTimeMillis();
			String[] record;

			for (int i = from; i <= to; i += 2)
			{
				record = new String[] { "" + i, "UPDATED_Naam", "UPDATED_Product", "" + ((2 * i + 1) % 12) };
				td.update(record);

				if (DISPLAY_ITERATION_INFO)
				{
					System.out.println("UPDATE PID " + i);
				}
			}

			e = System.currentTimeMillis();
			System.out.println("Updaten van " + (to - from) + " records duurde " + (e - s) + " ms");
		}
	}

	private void testDelete(TableDataIO td) throws Exception
	{
		long s;
		long e;
		if (TEST_DELETE)
		{
			int from = 5000;
			int to = 8000;
			s = System.currentTimeMillis();
			for (int i = from; i <= to; i += 2)
			{
				td.delete("PID", "" + i);
				if (DISPLAY_ITERATION_INFO)
				{
					System.out.println("DELETE PID " + i);
				}
			}
			e = System.currentTimeMillis();
			System.out.println("Deleten van " + (to - from) + " records duurde " + (e - s) + " ms");
		}
	}
}
