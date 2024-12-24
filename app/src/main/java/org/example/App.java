package org.example;

import java.util.stream.IntStream;

import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.TransactionHandle;

public class App {
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String tableName = "test_table";
        String drop = String.format("DROP TABLE %s", tableName);
        String create = String.format("create table %s (id int primary key , name int , note int)", tableName);
        String select = String.format("select name,note,count(*) from %s group by name,note", tableName);

        try (Session session = SessionBuilder.connect("ipc://tsurugi").create();
                SqlClient sql = SqlClient.attach(session);
                KvsClient kvs = KvsClient.attach(session)) {
            try (Transaction transaction = sql.createTransaction().get()) {
                transaction.executeStatement(drop).await();
                transaction.commit().await();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            try (Transaction transaction = sql.createTransaction().await()) {
                transaction.executeStatement(create).await();
                transaction.commit().await();
                transaction.close();
            }
            long kvsstartTime = System.nanoTime();
            try (TransactionHandle tx = kvs.beginTransaction().await()) {
                IntStream.range(0, 100000).forEach(i -> {
                    RecordBuffer record = new RecordBuffer();
                    record.add("id", i);
                    record.add("name", 1);
                    record.add("note", 1);
                    try {
                        kvs.put(tx, "test_table", record).await();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                kvs.commit(tx).await();
                tx.close();
                kvs.close();
            }
            long kvsendTime = System.nanoTime();
            System.out.println("kvs " + (kvsendTime - kvsstartTime) + " nm");
            PreparedStatement preparedStatement = sql.prepare(select, Placeholders.of("id", int.class),
                    Placeholders.of("name", int.class), Placeholders.of("note", int.class)).get();
            try (Transaction transaction = sql.createTransaction().get()) {
                try (FutureResponse<ResultSet> resultSet = transaction.executeQuery(preparedStatement,
                        Parameters.of("id", (int) 999), Parameters.of("name", (int) 999),
                        Parameters.of("note", (int) 999))) {
                    ResultSet r = resultSet.await();
                    while (r.nextRow()) {
                        while (r.nextColumn()) {
                            if (!r.isNull()) {
                                System.out.print(r.fetchInt4Value() + " ");
                            }
                        }
                        System.out.println("");
                    }
                }
                sql.close();
                transaction.close();
            }
            session.close();
        }
    }
}
