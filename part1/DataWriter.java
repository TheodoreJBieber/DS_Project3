import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

/** 
 * @author Theodore Bieber
 * Distributed Systems
 * Project 3
 *
 * A program that creates two datasets and lists them into .txt in csv format
 * USAGE: javac -cp . DataWriter <# Customers> <# Transactions> (also detailed in readme.txt)
 */
public final class DataWriter {

	private ArrayList<String> prevCustPKs = new ArrayList<>();

	public static void main(String[] args) {
		
		DataWriter dc = new DataWriter();
		
		if (args.length != 2) {
			System.out.println("Usage: java -cp . DataWriter <# Customers> <# Transactions>");
			return;
		}
		
		int custEntries;
		int transEntries;

		try {
			custEntries = Integer.parseInt(args[0]);
			transEntries = Integer.parseInt(args[1]);
		} catch (NumberFormatException e) {
			System.out.println(args[0] + " is not valid number of entries.");
			e.printStackTrace();
			return;
		}
		
		if (custEntries > 50000) {
			System.out.println("Cannot create more than 50000 unique Customers");
			return;
		} else if (transEntries > 5000000) {
			System.out.println("Cannot create more than 5000000 unique Transactions");
			return;
		}
		
		File custcsv = new File("customers" + ".txt");
		File transcsv = new File("transactions" + ".txt");
		
		// if previous file exist, append numbers
		for (int i = 1; custcsv.exists(); i++) {
			custcsv = new File("customers" + i + ".txt");
		} for (int i = 1; transcsv.exists(); i++) {
			transcsv = new File("transactions" + i + ".txt");
		}

		try {
			for (int i = 0; i < custEntries; i++)
				dc.writeToCSV(custcsv, new Customer(), true);
			for (int i = 0; i < transEntries; i++) {
				Customer c = new Customer();
				String ID = dc.getPrevPK(dc.prevCustPKs);
				c.setValue("ID", ID);
				dc.writeToCSV(transcsv, new Transaction(c), false);
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(1);
		}

	}

	public void writeToCSV(File csv, Datatype entry, Boolean collectPKs) throws IOException {
		FileWriter fw = new FileWriter(csv.getName(), true); // append to end of file
		BufferedReader br = new BufferedReader(new FileReader(csv.getName()));
		String value;

		for (String attr : entry.getAttributes()) {
			// this value must be unique, use BufferedReader to ensure other entries do not have it
			if (entry.getPrimaryKey().equals(attr)) {
				value = entry.getRandomValueFor(attr);
				while (this.prevCustPKs.contains(value)) // need a unique primary key
					value = entry.getRandomValueFor(attr);
				if (collectPKs)
					this.prevCustPKs.add(value);
				fw.write(value);
			} else {
				value = entry.getRandomValueFor(attr);
				if (value != null)
					fw.write("," + value);
			}
		}

		// add newline for next entry
		fw.write("\n");

		br.close();
		fw.close();
	}

	// return primary key
	public String getPrevPK(ArrayList<String> prevPKList) {
		Random rand = new Random();
		int idx = rand.nextInt(prevPKList.size());
		return prevPKList.get(idx);
	}

}