import java.util.ArrayList;

/** 
 * @author Theodore Bieber
 * Distributed Systems
 * Project 3
 *
 * An Interface representing a Datatype in a data set
 *
 */

public interface Datatype {
	
	// returns the name of the primary key
	String getPrimaryKey();

	// returns the names of the attributes
	ArrayList<String> getAttributes();

	String getValue(String key);

	void setValue(String key, String value);

	// gets a randomized, nonunique value for an attribute
	String getRandomValueFor(String key);
}