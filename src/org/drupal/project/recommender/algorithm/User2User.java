package org.drupal.project.recommender.algorithm;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.drupal.project.computing.exception.DCommandExecutionException;
import org.drupal.project.recommender.utils.AsyncQueueProcessor;

import java.sql.SQLException;
import java.util.Date;
import java.util.List;

public class User2User extends MahoutCF {

    @Override
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

    @Override
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

}
