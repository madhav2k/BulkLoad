/**
 * Disclaimer:
 * This file is an example on how to use the Cassandra SSTableSimpleUnsortedWriter class to create
 * sstables from a csv input file.
 * While this has been tested to work, this program is provided "as is" with no guarantee. Moreover,
 * it's primary aim is toward simplicity rather than completness. In partical, don't use this as an
 * example to parse csv files at home.
 */
import java.nio.ByteBuffer;
import java.io.*;
import java.util.UUID;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.UUIDGen.decompose;

public class DataImportExample
{
    static String filename;

    public static void main(String[] args) throws IOException
    {
        if (args.length == 0)
        {
            System.out.println("Expecting <csv_file> as argument");
            System.exit(1);
        }
        filename = "Beneficiary_Summary_File_Sample.csv";
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        RandomPartitioner part = new RandomPartitioner();
        
        String keyspace = "Demo";
        File directory = new File(keyspace);
        if (!directory.exists())
            directory.mkdir();

//        SSTableSimpleUnsortedWriter usersWriter = new SSTableSimpleUnsortedWriter(
//                directory,
//                keyspace,
//                "Users",
//                AsciiType.instance,
//                null,
//                64);
        
//        SSTableSimpleUnsortedWriter loginWriter = new SSTableSimpleUnsortedWriter(
//                directory,
//                keyspace,
//                "Logins",
//                AsciiType.instance,
//                null,
//                64);
        
        SSTableSimpleUnsortedWriter usersWriter = new SSTableSimpleUnsortedWriter(
                directory,
                part,
                keyspace,
                "outpatient_claims",
                AsciiType.instance,
                null,
                64);        


        String line;
        int lineNumber = 1;
        CsvEntry entry = new CsvEntry();
        // There is no reason not to use the same timestamp for every column in that example.
        long timestamp = System.currentTimeMillis() * 1000;
        while ((line = reader.readLine()) != null)
        {
            if (entry.parse(line, lineNumber))
            {
                ByteBuffer uuid = ByteBuffer.wrap(decompose(entry.key));
                usersWriter.newRow(uuid);
                usersWriter.addColumn(bytes("firstname"), bytes(entry.firstname), timestamp);
                usersWriter.addColumn(bytes("lastname"), bytes(entry.lastname), timestamp);
                usersWriter.addColumn(bytes("password"), bytes(entry.password), timestamp);
                usersWriter.addColumn(bytes("age"), bytes(entry.age), timestamp);
                usersWriter.addColumn(bytes("email"), bytes(entry.email), timestamp);

//                loginWriter.newRow(bytes(entry.email));
//                loginWriter.addColumn(bytes("password"), bytes(entry.password), timestamp);
//                loginWriter.addColumn(bytes("uuid"), uuid, timestamp);
            }
            lineNumber++;
        }
        // Don't forget to close!
        usersWriter.close();
        reader.close();
        //loginWriter.close();
        System.exit(0);
    }

    static class CsvEntry
    {
        UUID key;
        String firstname;
        String lastname;
        String password;
        long age;
        String email;

        boolean parse(String line, int lineNumber)
        {
            // Ghetto csv parsing
            String[] columns = line.split(",");
            if (columns.length != 6)
            {
                System.out.println(String.format("Invalid input '%s' at line %d of %s", line, lineNumber, filename));
                return false;
            }
            try
            {
                key = UUID.fromString(columns[0].trim());
                firstname = columns[1].trim();
                lastname = columns[2].trim();
                password = columns[3].trim();
                age = Long.parseLong(columns[4].trim());
                email = columns[5].trim();
                return true;
            }
            catch (NumberFormatException e)
            {
                System.out.println(String.format("Invalid number in input '%s' at line %d of %s", line, lineNumber, filename));
                return false;
            }
        }
    }
}