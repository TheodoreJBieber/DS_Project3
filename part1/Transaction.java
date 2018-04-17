import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

/** Theodore Bieber
 * Distributed Systems
 * Project 3
 *
 * A Datatype representing a Transaction
 *
 */

public class Transaction implements Datatype {
	
	private HashMap<String,String> attributes = new HashMap<>();

	public Transaction(Customer custID) {
		attributes.put("TransID", null);
		attributes.put("CustID", custID.getValue(custID.getPrimaryKey()));
		attributes.put("TransValue", null);
		attributes.put("TransNumItems", null);
		attributes.put("TransDesc", null);
	}
	
	public Transaction(int transID, Customer custID, double transValue, int transItems, String transDesc) {
		attributes.put("TransID", Integer.toString(transID));
		attributes.put("CustID", custID.getValue(custID.getPrimaryKey()));
		attributes.put("TransValue", Double.toString(transValue));
		attributes.put("TransNumItems", Integer.toString(transItems));
		attributes.put("TransDesc", transDesc);
	}
	
	public String getPrimaryKey() {
		return "TransID";
	}
	
	public ArrayList<String> getAttributes() {
		ArrayList<String> attrNames = new ArrayList<>();
		attrNames.add("TransID");
		attrNames.add("CustID");
		attrNames.add("TransValue");
		attrNames.add("TransNumItems");
		attrNames.add("TransDesc");
		return attrNames;
	}

	public String getValue(String key) {
		if (this.attributes.containsKey(key)) {
			return this.attributes.get(key);
		} return null;
	}

	public void setValue(String key, String value) {
		if (this.attributes.containsKey(key)) {
			this.attributes.put(key, value);
		}
	}
	
	protected String randomAlphabetStr(int min, int max) {
		String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
		Random rand = new Random();
		int count = rand.nextInt(max) + min;
		
		StringBuilder randStr = new StringBuilder();
		int index;
		rand = new Random();
		for (; count >= 0; count--) {
			index = (int) (rand.nextFloat() * alphabet.length());
            randStr.append(alphabet.charAt(index));
		}
		
		return randStr.toString();
	}
	
	public String getRandomValueFor(String key) {
		Random rand;
		Integer adjusted;
		Double adjustedDouble;
		if (key.equals("TransID")) {
			rand = new Random();
			adjusted = rand.nextInt(5000000) + 1;
			return adjusted.toString();
		} else if (key.equals("CustID")) { // don't change value for foreign key
			return this.attributes.get(key);
		} else if (key.equals("TransValue")) { 
			rand = new Random();
			adjustedDouble = (10000.0 - 10.0)*rand.nextDouble() + 10.0;
			return String.valueOf(Math.round(adjustedDouble * 100.0) / 100.0); // round to hundredths place (cents)
		} else if (key.equals("TransNumItems")) {
			rand = new Random();
			adjusted = rand.nextInt(10) + 1;
			return adjusted.toString();
		} else if (key.equals("TransDesc")) {
			return randomAlphabetStr(20,50);
		} else {
			return null;
		}
	}

}