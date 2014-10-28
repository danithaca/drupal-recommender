package org.drupal.project.recommender.test;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.builder.RecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.drupal.project.computing.DApplication;
import org.drupal.project.computing.DConfig;
import org.drupal.project.computing.exception.DNotFoundException;
import org.drupal.project.recommender.DefaultApplication;
import org.drupal.project.recommender.utils.AsyncQueueProcessor;
import org.junit.Test;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

public class RecommenderTest {

    @Test
    public void testLaunch() {
        DApplication application = new DefaultApplication();
        application.launch();
    }

    @Test
    public void testDB() throws Exception {
        DConfig config = DConfig.loadDefault();
        DataSource dataSource = BasicDataSourceFactory.createDataSource(config.getDatabaseProperties());

        AsyncQueueProcessor queue = new AsyncQueueProcessor(dataSource.getConnection(), "INSERT INTO recommender_prediction(uid, eid, score, updated) VALUES(?, ?, ?, ?)");
        queue.start();

        QueryRunner qr = new QueryRunner(dataSource);
        List<Map<String, Object>> result = qr.query("SELECT nid, created FROM node", new MapListHandler());
        for (Map<String, Object> map: result) {
            //System.out.println(ReflectionToStringBuilder.toString(map, new RecursiveToStringStyle()));;
            //System.out.println(map.values());
            queue.put(map.get("nid"), map.get("nid"), RandomUtils.nextInt(0, 6), map.get("created"));
        }
        queue.accomplish();
        queue.join();
        System.out.println("Done.");
    }
}
