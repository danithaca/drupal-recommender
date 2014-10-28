package org.drupal.project.recommender.algorithm;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.jdbc.MySQLJDBCDataModel;
import org.apache.mahout.cf.taste.impl.model.jdbc.PostgreSQLJDBCDataModel;
import org.apache.mahout.cf.taste.impl.model.jdbc.SQL92JDBCDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.*;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.drupal.project.computing.DConfig;
import org.drupal.project.computing.exception.DCommandExecutionException;
import org.drupal.project.recommender.RecommenderCommand;
import org.drupal.project.recommender.utils.AsyncQueueProcessor;
import org.drupal.project.recommender.utils.RecommendationTuple;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


abstract public class CollaborativeFiltering extends RecommenderCommand {

    protected BasicDataSource dataSource;
    protected DataModel dataModel;
    protected ItemSimilarity itemSimilarity;
    protected UserSimilarity userSimilarity;
    protected Recommender recommender;

    protected int numSimilarity;
    protected int numPrediction;

    @Override
    public void execute() throws DCommandExecutionException {
        logger.info("Initializing connection to database.");
        try {
            // attention: we already used DBCP for pooling so no need for wrapping into ConnectionPooling again.
            //ConnectionPoolDataSource wrapperDataSource = new ConnectionPoolDataSource(database.getDataSource());
            dataSource = (BasicDataSource) BasicDataSourceFactory.createDataSource(dbProperties);
        } catch (Exception e) {
            throw new DCommandExecutionException(e);
        }

        logger.info("Initializing data model.");
        initDataModel();

        // get basic info.
        try {
            numUsers = dataModel.getNumUsers();
            numItems = dataModel.getNumItems();
        } catch (TasteException e) {
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
        this.result.put("num_similarity", numSimilarity);
        this.result.put("num_prediction", numPrediction);
        this.message.append("Successfully run Recommender from: ").append(DConfig.loadDefault().getAgentName());

        // close database
        try {
            dataSource.close();
        } catch (SQLException e) {
            throw new DCommandExecutionException(e);
        }
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
        NearestNUserNeighborhood neighbor;
        try {
            neighbor = new NearestNUserNeighborhood(nearestN, minSimilarityScore, userSimilarity, dataModel);
        } catch (TasteException e) {
            logger.severe("Cannot create nearest neighbor. " + e.getMessage());
            throw new DCommandExecutionException(e);
        }
        // note: mahout requires user neighborhood for user-user recommendations. this leads to different results from the grouplens paper.
        recommender = new GenericUserBasedRecommender(dataModel, neighbor, userSimilarity);
    }

    protected int getMaxKeep() {
        return extraOptions.containsKey("max entities to keep") ? Integer.parseInt(extraOptions.getProperty("max entities to keep")) : 50;
    }

    protected void computeSimilarity() throws DCommandExecutionException {
        QueryRunner queryRunner = new QueryRunner(dataSource);
        //List<RecommendationTuple> similarities = new ArrayList<>();
        long timestamp = (new Date()).getTime() / 1000;
        int count = 0;
        AsyncQueueProcessor dataProcessor;

        try {
            // we are using a quick and dirty approach of clean up everything before saving new stuff.
            // with this approach, there's no need to use db transactions.
            queryRunner.update("DELETE FROM " + dataStructure.getProperty("user similarity name"));
            dataProcessor = new AsyncQueueProcessor(dataSource, String.format("INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)", dataStructure.getProperty("user similarity name"), dataStructure.getProperty("user similarity user1 field"), dataStructure.getProperty("user similarity user2 field"), dataStructure.getProperty("user similarity score field"), dataStructure.getProperty("user similarity timestamp field")));
            dataProcessor.start();

            LongPrimitiveIterator userIterator = dataModel.getUserIDs();

            while (userIterator.hasNext()) {
                long userID = userIterator.nextLong();
                long[] similarUserIDs = ((UserBasedRecommender) recommender).mostSimilarUserIDs(userID, getMaxKeep());
                for (long similarUserID : similarUserIDs) {
                    //similarities.add(new RecommendationTuple(userID, similarUserID, new Float(userSimilarity.userSimilarity(userID, similarUserID))));
                    dataProcessor.put(userID, similarUserID, new Float(userSimilarity.userSimilarity(userID, similarUserID)), timestamp);
                    count++;
                }
            }

            // mark it as done and wait till it's done.
            dataProcessor.accomplish();
            dataProcessor.join();

        } catch (TasteException | SQLException | InterruptedException e) {
            throw new DCommandExecutionException(e);
        }

        // handle similarities data.
        //numSimilarity = similarities.size();
        numSimilarity = count;
    }

    protected void computePrediction() throws DCommandExecutionException {
        QueryRunner queryRunner = new QueryRunner(dataSource);
        //List<RecommendationTuple> predictions = new ArrayList<>();
        long timestamp = (new Date()).getTime() / 1000;
        int count = 0;
        AsyncQueueProcessor dataProcessor;

        try {
            queryRunner.update("DELETE FROM " + dataStructure.getProperty("prediction name"));
            dataProcessor = new AsyncQueueProcessor(dataSource, String.format("INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)", dataStructure.getProperty("prediction name"), dataStructure.getProperty("prediction user field"), dataStructure.getProperty("prediction item field"), dataStructure.getProperty("prediction score field"), dataStructure.getProperty("prediction timestamp field")));
            dataProcessor.start();

            LongPrimitiveIterator userIterator = dataModel.getUserIDs();

            while (userIterator.hasNext()) {
                long userID = userIterator.nextLong();
                List<RecommendedItem> recommendedItemList = recommender.recommend(userID, getMaxKeep());
                for (RecommendedItem recommendItem : recommendedItemList) {
                    //predictions.add(new RecommendationTuple(userID, recommendItem.getItemID(), recommendItem.getValue()));
                    dataProcessor.put(userID, recommendItem.getItemID(), recommendItem.getValue(), timestamp);
                    count++;
                }
            }

            // mark it as done and wait till it's done.
            dataProcessor.accomplish();
            dataProcessor.join();

        } catch (TasteException | SQLException | InterruptedException e) {
            throw new DCommandExecutionException(e);
        }

        // handle similarities data.
        //numPrediction = predictions.size();
        numPrediction = count;
    }

}
