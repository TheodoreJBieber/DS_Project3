Theodore Bieber
Distributed Systems
Project 3

Part 1:
compiling my code:
javac -classpath /usr/local/java/jdk1.7.0_45 -d classes Customer.java Datatype.java Transaction.java DataWriter.java
jar -cvf ./DataWriter.jar -C classes/ .

running it:
java -cp DataWriter.jar DataWriter 50000 50000

this produces customers.txt with 50000 entries and transactions.txt with 50000 entries (note that this may take a while to run due to how i wrote the number generation)

Part 2:

The commands I used to put files onto the hadoop fs were:

sudo hadoop fs -put customers.txt /home/hadoop/Customers.txt
sudo hadoop fs -put transactions.txt /home/hadoop/Customers.txt

to double check that they were there, i copied them back onto my desktop

sudo hadoop fs -copyToLocal /home/hadoop/input /home/hadoop/Desktop/

and the files showed up on my desktop, so I knew it worked.

Part 3:

compiling a query: (example for Query1): 
/////// also note that you must have the Query1_classes folder created already. I did so with mkdir Query1_classes

javac -classpath /usr/share/hadoop/hadoop-core-1.2.1.jar -d Query1_classes ./Query1.java
jar -cvf ./Query1.jar -C Query1_classes/ .

running the job:

sudo hadoop jar ./JavaFileName.jar MainClassName <input file path> <new output file path>

(after making the jar):
running Query1:
	sudo hadoop jar ./Query1.jar Query1 /home/hadoop/Customers.txt /home/hadoop/query1_output1

running Query2a:
	sudo hadoop jar ./Query2a.jar Query2a /home/hadoop/Transactions.txt /home/hadoop/query2a_output1

running Query2b:
	sudo hadoop jar ./Query2b.jar Query2b /home/hadoop/Transactions.txt /home/hadoop/query2b_output1

running Query3:
	sudo hadoop jar ./Query3.jar Query3 /home/hadoop/Customers.txt /home/hadoop/Transactions.txt /home/hadoop/query3_output1

running Query4:
	sudo hadoop jar ./Query4.jar Query4 /home/hadoop/Transactions.txt /home/hadoop/query4_output1