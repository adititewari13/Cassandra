package org.cassandra;

import com.datastax.driver.core.*;

import com.datastax.driver.core.LocalDate;

public class BasicCommands {

    private static Cluster cluster = null;
    private static Session session;
   private static BoundStatement bound;

//    private static PreparedStatement prepared;

    public static void main(String [] args) {



        try
        {
            cluster = Cluster.builder()
                    .addContactPoint("127.0.0.1")
                    .build();
            session = cluster.connect();

            //   createKeyspace();

            //   createTable();

            //   insertAsSimpleStatement();

            //   insertAsPreparedStatement();

            //   fetchOneRecord(session);

            //   dropKeyspace();


        }

        finally {
            if (cluster != null)
                cluster.close();
        }
}

    private static void insertAsPreparedStatement() {
        PreparedStatement statement = session.prepare(" Insert into demo1.customers(cust_id,name, address,gender, country,email,dob)"+
                "values (?,?,?,?,?,?,?);");

        BoundStatement boundStatement = new BoundStatement (statement);

        LocalDate dob = LocalDate.fromYearMonthDay(2014, 9, 11);

            session.execute(boundStatement.bind("C002","Aditya", "Dallas TX", "Male",
                    "USA", "adityatewari13@gmail.com",dob));
        System.out.println("1 row inserted!");

    }

    private static void insertAsSimpleStatement() {
        String statement = "Insert into demo1.customers(cust_id,name, address,gender, country,dob,email)"+
                           "values ('C001','Aditi','Uttarakhand','Female','India','2000-11-13','adititewari13@gmail.com')";

        executeStatement(statement);
        System.out.println("1 row inserted!");
    }

    private static void createKeyspace() {
        session.execute("create keyspace demo1 "+
                "with replication = {'class': 'SimpleStrategy', 'replication_factor':1}");
        System.out.println("Keyspace Created!");
    }

    private static void dropKeyspace()
    {

        executeStatement("drop keyspace if exists demo1");
        System.out.println("Keyspace deleted!");
    }

    private static void executeStatement(String statement) {
        System.out.println(statement);
        session.execute(statement);
        System.out.println(("Done!"));
    }

    private static void createTable()
    {
       String statement = "create table demo1.customers("+
                            "cust_id varchar primary key,"+
                          "name varchar," +
                           "address text,"+
                         "gender text,"+
                         "country text,"+
                          "dob date,"+
                            "email text)";

       executeStatement(statement);
       System.out.println("Table Created!");
    }

    private static void fetchOneRecord(Session session)
      {
          String statement = "Select * from demo1.customers;";
          ResultSet rs = session.execute(statement);
          Row row = rs.one();
          System.out.println(row);
      }
}
