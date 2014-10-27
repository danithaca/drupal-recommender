package org.drupal.project.recommender.algorithm;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.jdbc.MySQLJDBCDataModel;
import org.apache.mahout.cf.taste.impl.model.jdbc.PostgreSQLJDBCDataModel;
import org.apache.mahout.cf.taste.impl.model.jdbc.SQL92JDBCDataModel;
import org.drupal.project.computing.DCommand;
import org.drupal.project.computing.DConfig;
import org.drupal.project.computing.DUtils;
import org.drupal.project.computing.exception.DCommandExecutionException;
import org.drupal.project.recommender.RecommenderCommand;

import javax.script.Bindings;
import javax.sql.DataSource;


public class CollaborativeFiltering extends RecommenderCommand {

    @Override
    public void execute() throws DCommandExecutionException {
        logger.info("Initializing connection to database.");
        try {
            // attention: we already used DBCP for pooling so no need for wrapping into ConnectionPooling again.
            //ConnectionPoolDataSource wrapperDataSource = new ConnectionPoolDataSource(database.getDataSource());
            this.dataSource = (BasicDataSource) BasicDataSourceFactory.createDataSource(dbProperties);
        } catch (Exception e) {
            e.printStackTrace();
            logger.severe("Cannot initialized database connection.");
            throw new DCommandExecutionException(e);
        }

        logger.info("Initializing data model.");
        initDataModel();

        // get basic info.
        try {
            numUsers = dataModel.getNumUsers();
            numItems = dataModel.getNumItems();
        } catch (TasteException e) {
            e.printStackTrace();
            logger.severe("Cannot get number of users or items. " + e.getMessage());
            throw new DCommandExecutionException(e);
        }

        // initialize similarity class.
        initSimilarity();

        // initialize recommender class.
        initRecommender();

        logger.info("Computing and saving similarity data.");
        computeSimilarity();

        logger.info("Computing and saving prediction data.");
        computePrediction();

        // handle result data
        this.result.put("num_user", numUsers);
        this.result.put("num_item", numItems);
        this.message.append("Successfully run Recommender from: ").append(DConfig.loadDefault().getAgentName());
    }

    protected void initDataModel() throws DCommandExecutionException {
        if (dbProperties.containsKey("driver") && dbProperties.getProperty("driver").equals("mysql")) {
            dataModel = new MySQLJDBCDataModel(dataSource, dataStructure.getProperty("preference name"), dataStructure.getProperty("preference user field"), dataStructure.getProperty("preference item field"), dataStructure.getProperty("preference score field"), dataStructure.getProperty("preference timestamp field"));
        } else if (dbProperties.containsKey("driver") && dbProperties.getProperty("driver").equals("postgresql")) {
            dataModel = new PostgreSQLJDBCDataModel(dataSource, dataStructure.getProperty("preference name"), dataStructure.getProperty("preference user field"), dataStructure.getProperty("preference item field"), dataStructure.getProperty("preference score field"), dataStructure.getProperty("preference timestamp field"));
        } else {
            dataModel = new SQL92JDBCDataModel(dataSource, dataStructure.getProperty("preference name"), dataStructure.getProperty("preference user field"), dataStructure.getProperty("preference item field"), dataStructure.getProperty("preference score field"), dataStructure.getProperty("preference timestamp field"));
        }
    }

    protected void initSimilarity() {

    }

    protected void initRecommender() {

    }

    protected void computeSimilarity() {

    }

    protected void computePrediction() {

    }

}
