package org.drupal.project.recommender.algorithm;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.jdbc.MySQLJDBCDataModel;
import org.apache.mahout.cf.taste.impl.model.jdbc.PostgreSQLJDBCDataModel;
import org.apache.mahout.cf.taste.impl.model.jdbc.SQL92JDBCDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.*;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.drupal.project.computing.DCommand;
import org.drupal.project.computing.DConfig;
import org.drupal.project.computing.DUtils;
import org.drupal.project.computing.exception.DCommandExecutionException;
import org.drupal.project.recommender.RecommenderCommand;

import javax.script.Bindings;
import javax.sql.DataSource;


public class CollaborativeFiltering extends RecommenderCommand {

    protected DataSource dataSource;
    protected DataModel dataModel;
    protected ItemSimilarity itemSimilarity;
    protected UserSimilarity userSimilarity;
    protected Recommender recommender;

    @Override
    public void execute() throws DCommandExecutionException {
        logger.info("Initializing connection to database.");
        try {
            // attention: we already used DBCP for pooling so no need for wrapping into ConnectionPooling again.
            //ConnectionPoolDataSource wrapperDataSource = new ConnectionPoolDataSource(database.getDataSource());
            this.dataSource = BasicDataSourceFactory.createDataSource(dbProperties);
        } catch (Exception e) {
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

    protected void initDataModel() {
        if (dbProperties.containsKey("driver") && dbProperties.getProperty("driver").equals("mysql")) {
            dataModel = new MySQLJDBCDataModel(dataSource, dataStructure.getProperty("preference name"), dataStructure.getProperty("preference user field"), dataStructure.getProperty("preference item field"), dataStructure.getProperty("preference score field"), dataStructure.getProperty("preference timestamp field"));
        } else if (dbProperties.containsKey("driver") && dbProperties.getProperty("driver").equals("postgresql")) {
            dataModel = new PostgreSQLJDBCDataModel(dataSource, dataStructure.getProperty("preference name"), dataStructure.getProperty("preference user field"), dataStructure.getProperty("preference item field"), dataStructure.getProperty("preference score field"), dataStructure.getProperty("preference timestamp field"));
        } else {
            dataModel = new SQL92JDBCDataModel(dataSource, dataStructure.getProperty("preference name"), dataStructure.getProperty("preference user field"), dataStructure.getProperty("preference item field"), dataStructure.getProperty("preference score field"), dataStructure.getProperty("preference timestamp field"));
        }
    }

    protected void initSimilarity() throws DCommandExecutionException {
        try {
            if (extraOptions.containsKey("similarity algorithm")) {
                switch (extraOptions.getProperty("similarity algorithm")) {
                    case "CityBlock":
                        itemSimilarity = new CityBlockSimilarity(dataModel);
                        userSimilarity = new CityBlockSimilarity(dataModel);
                        break;
                    case "Euclidean":
                        itemSimilarity = new EuclideanDistanceSimilarity(dataModel);
                        userSimilarity = new EuclideanDistanceSimilarity(dataModel);
                        break;
                    case "Tanimoto":
                        itemSimilarity = new TanimotoCoefficientSimilarity(dataModel);
                        userSimilarity = new TanimotoCoefficientSimilarity(dataModel);
                        break;
                    case "Spearman":
                        itemSimilarity = null;
                        userSimilarity = new SpearmanCorrelationSimilarity(dataModel);
                        break;
                    case "LogLikelihood":
                        itemSimilarity = new LogLikelihoodSimilarity(dataModel);
                        userSimilarity = new LogLikelihoodSimilarity(dataModel);
                        break;
                    case "Pearson":
                        itemSimilarity = new PearsonCorrelationSimilarity(dataModel);
                        userSimilarity = new PearsonCorrelationSimilarity(dataModel);
                        break;
                    case "Cosine":
                        itemSimilarity = new UncenteredCosineSimilarity(dataModel);
                        userSimilarity = new UncenteredCosineSimilarity(dataModel);
                        break;
                }
            }

            // set default if no overrides.
            if (itemSimilarity == null) {
                itemSimilarity = new PearsonCorrelationSimilarity(dataModel);
            }
            if (userSimilarity == null) {
                userSimilarity = new PearsonCorrelationSimilarity(dataModel);
            }

        } catch (TasteException e) {
            logger.severe("Cannot create similarity object." + e.getMessage());
            throw new DCommandExecutionException(e);
        }
    }


    protected void initRecommender() throws DCommandExecutionException {
        int nearestN = Integer.parseInt(extraOptions.getProperty("k nearest neighbors", "20"));
        float minSimilarityScore = Float.parseFloat(extraOptions.getProperty("min similarity score", "0.1"));
        NearestNUserNeighborhood neighbor = null;
        try {
            neighbor = new NearestNUserNeighborhood(nearestN, minSimilarityScore, userSimilarity, dataModel);
        } catch (TasteException e) {
            logger.severe("Cannot create nearest neighbor. " + e.getMessage());
            throw new DCommandExecutionException(e);
        }
        recommender = new GenericUserBasedRecommender(dataModel, neighbor, userSimilarity);
    }

    protected void computeSimilarity() {

    }

    protected void computePrediction() {

    }

}
