import java.nio.ByteBuffer;
import java.io.*;
import java.util.UUID;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;
import org.apache.cassandra.dht.RandomPartitioner;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.UUIDGen.decompose;

public class BulkLoadCassandraWithCSV {

    static String filename;

    // Number of columns & names
    static int numCols = 31;
    static String colNames[] = {"DESYNPUF_ID","BENE_BIRTH_DT","BENE_DEATH_DT","BENE_SEX_IDENT_CD","BENE_RACE_CD","BENE_ESRD_IND","SP_STATE_CODE","BENE_COUNTY_CD","BENE_HI_CVRAGE_TOT_MONS","BENE_SMI_CVRAGE_TOT_MONS","BENE_HMO_CVRAGE_TOT_MONS","PLAN_CVRG_MOS_NUM","SP_ALZHDMTA","SP_CHF","SP_CHRNKIDN","SP_CNCR","SP_COPD","SP_DEPRESSN","SP_DIABETES","SP_ISCHMCHT","SP_OSTEOPRS","SP_RA_OA","SP_STRKETIA","MEDREIMB_IP","BENRES_IP","PPPYMT_IP","MEDREIMB_OP","BENRES_OP","PPPYMT_OP","MEDREIMB_CAR","BENRES_CAR","PPPYMT_CAR"};

    public static void main(String[] args) throws IOException {

//        if (args.length < 3)
//        {
//            System.out.println("Expecting 3 arguments - <keyspace>, <column>, <csv_file>");
//            System.exit(1);
//        }

        long start = System.currentTimeMillis();

        String keyspace = "cms_desynpuf";
        String col = "outpatient_claims";
        filename = "Beneficiary_Summary_File_Sample.csv";

        BufferedReader reader = new BufferedReader(new FileReader(filename));
        RandomPartitioner part = new RandomPartitioner();

        File directory = new File(keyspace);
        if (!directory.exists())
            directory.mkdir();

        SSTableSimpleUnsortedWriter usersWriter = new SSTableSimpleUnsortedWriter(
            directory,
            part,
            keyspace,
            col,
            AsciiType.instance,
            null,
            64
        );

        String line;
        int lineNumber = 0;
        CsvParse entry = new CsvParse();

        long timestamp = System.currentTimeMillis() * 1000;

        while ((line = reader.readLine()) != null) {

            // Parse & Add Values
            entry.parse(line, ++lineNumber);
            ByteBuffer uuid = ByteBuffer.wrap(decompose(UUID.randomUUID()));
            usersWriter.newRow(uuid);
            for (int i=0;i<numCols;i++) {
                usersWriter.addColumn(bytes(colNames[i]), bytes(entry.d[i]), timestamp);
            }

            // Print nK
            if (lineNumber % 10000 == 0) {
              System.out.println((lineNumber / 1000) + "K");
            }

        }

        long end = System.currentTimeMillis();

        System.out.println("Successfully parsed " + lineNumber + " lines.");
        System.out.println("Execution time was "+(end-start)+" ms.");

        usersWriter.close();
        reader.close();
        System.exit(0);

    }

    static class CsvParse {

        String d[] = new String[numCols];

        void parse(String line, int lineNumber) {
          String[] col = line.split(",");
          for (int j=0;j<numCols;j++) {
            d[j] = col[j].trim();
          }
        }

    }
}