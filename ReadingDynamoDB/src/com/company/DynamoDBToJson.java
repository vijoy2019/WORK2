package com.company;

/*
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
*/

public class DynamoDBToJson {

    public static void main(String[] args) {
	// write your code here


        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);

        Table table = dynamoDB.getTable("Movies");

        int year = 2015;
        String title = "The Big New Movie";

        GetItemSpec spec = new GetItemSpec().withPrimaryKey("year", year, "title", title);

        try {
            System.out.println("Attempting to read the item...");
            Item outcome = table.getItem(spec);
            System.out.println("GetItem succeeded: " + outcome);

        }
        catch (Exception e) {
            System.err.println("Unable to read item: " + year + " " + title);
            System.err.println(e.getMessage());
        }


        /*
        static void listMyTables() {

            TableCollection<ListTablesResult> tables = dynamoDB.listTables();
            Iterator<Table> iterator = tables.iterator();

            System.out.println("Listing table names");

            while (iterator.hasNext()) {
                Table table = iterator.next();
                System.out.println(table.getTableName());
            }
        }
        */

        /*
        static void getTableInformation() {

            System.out.println("Describing " + tableName);

            TableDescription tableDescription = dynamoDB.getTable(tableName).describe();
            System.out.format(
                    "Name: %s:\n" + "Status: %s \n" + "Provisioned Throughput (read capacity units/sec): %d \n"
                            + "Provisioned Throughput (write capacity units/sec): %d \n",
                    tableDescription.getTableName(), tableDescription.getTableStatus(),
                    tableDescription.getProvisionedThroughput().getReadCapacityUnits(),
                    tableDescription.getProvisionedThroughput().getWriteCapacityUnits());
        }
        */
    }
}
