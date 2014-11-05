package org.drupal.project.recommender.utils;

import org.apache.commons.dbutils.QueryRunner;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class implements a "producer-consumer" queue that updates data periodically in a separate thread.
 *
 * Here's how you use AsyncQueueProcessor in you application:
 *
 * <code>
 * queue = new AsyncQueueProcessor(...);
 * queue.start();
 *
 * // in a loop in the main thread, produce data to be persisted to the db.
 * while (...) {
 *     queue.put(...);
 * }
 * // notify that there's no data to be produced. program can exit.
 * queue.accomplish();
 *
 * // wait till the db "consumer" finishes persisting data.
 * queue.join();
 * // continue running the rest of the code.
 * </code>
 *
 * To handle SQLException and InterruptedException in run(), use these code:
 *
 * <code>
 *     queue.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
 *         void uncaughtException(Thread t, Throwable e) {
 *             if (e instanceof RuntimeException && e.getCause() instanceof SQLException) {
 *                 // handle sql exception.
 *             }
 *         }
 *     });
 * </code>
 *
 */
public class AsyncQueueProcessor extends Thread {

    private final BlockingQueue<Object[]> queue = new LinkedBlockingQueue<Object[]>();
    private final Connection connection;
    private final DataSource dataSource;
    private String sqlStatement;
    private int batchSize;

    // no need to use AtomicBoolean as accomplish() is synchronized, which is the only method that sets this property.
    private boolean accomplished = false;

    /**
     * @param dataSource JDBC dataSource to use.
     * @param sql the sql statement to process data with.
     */
    public AsyncQueueProcessor(DataSource dataSource, String sql) throws SQLException {
        this(null, "Default AsyncQueueProcessor Thread", dataSource, null, sql, 0);
    }

    public AsyncQueueProcessor(Connection connection, String sql) throws SQLException {
        this(null, "Default AsyncQueueProcessor Thread", null, connection, sql, 0);
    }

    /**
     * Constructor for the class.
     *
     * @param threadGroup The thread group this thread is associated with.
     * @param threadName A name for this BatchUploader for identification purpose in multi-threading environment. Could be null.
     * @param dataSource JDBC datasource to use. Either provide a DataSource or a connection.
     * @param connection An optional JDBC connection to use. Either provide a connection, or the DataSource.
     * @param sql The full SQL statement. E.g., "INSERT INTO users(id, name) VALUES(?, ?)".
     * @param batchSize
     */
    public AsyncQueueProcessor(ThreadGroup threadGroup, String threadName, DataSource dataSource, Connection connection, String sql, int batchSize) throws SQLException {
        super(threadGroup, threadName);
        assert dataSource != null || connection != null;
        this.dataSource = dataSource;
        this.connection = connection;
        this.batchSize = batchSize;
        this.sqlStatement = sql;
    }

    synchronized public void put(Object... row) throws InterruptedException {
        try {
            queue.put(row);
        } finally {
            notifyAll();
        }
    }

    synchronized public void accomplish() {
        accomplished = true;
        notifyAll();
    }

    @Override
    synchronized public void run() {
        QueryRunner qr = new QueryRunner(); // this is a dummy so we can use fillStatement().
        List<Object[]> rows = new ArrayList<Object[]>();
        int num = 0;
        int index = 0;
        PreparedStatement preparedSql = null;

        while (true) {
            // retrieve items and save them.
            num = queue.drainTo(rows);
            if (num > 0) {
                try {
                    for (index = 0; index < num; index++) {
                        // during wait(), connection might be timeout. need to handle that case.
                        if (preparedSql == null || preparedSql.isClosed()) {
                            Connection conn = (this.connection == null) ? dataSource.getConnection() : this.connection;
                            preparedSql = conn.prepareStatement(sqlStatement);
                        }
                        qr.fillStatement(preparedSql, rows.get(index));
                        preparedSql.addBatch();
                        // periodically execute batch before the batch is too large.
                        if ((batchSize > 0) && (index > 0) && (index % batchSize == 0)) {
                            preparedSql.executeBatch();
                        }
                    }
                    // finally execute batch for the rest of the items.
                    preparedSql.executeBatch();
                    rows.clear();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            // check if accomplished then break out of the loop and ends the thread.
            // otherwise wait() till get notified.
            if (accomplished) {
                break;
            } else {
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}