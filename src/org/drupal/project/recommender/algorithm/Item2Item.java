package org.drupal.project.recommender.algorithm;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.drupal.project.computing.exception.DCommandExecutionException;
import org.drupal.project.recommender.utils.AsyncQueueProcessor;

import java.sql.SQLException;
import java.util.Date;
import java.util.List;


public class Item2Item extends MahoutClassicCF {

    @Override
    protected void initRecommender() throws DCommandExecutionException {
        recommender = new GenericItemBasedRecommender(dataModel, itemSimilarity);
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
            queryRunner.update("DELETE FROM " + dataStructure.getProperty("item similarity name"));
            dataProcessor = new AsyncQueueProcessor(dataSource, String.format("INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)", dataStructure.getProperty("item similarity name"), dataStructure.getProperty("item similarity item1 field"), dataStructure.getProperty("item similarity item2 field"), dataStructure.getProperty("item similarity score field"), dataStructure.getProperty("item similarity timestamp field")));
            dataProcessor.start();

            LongPrimitiveIterator itemIterator = dataModel.getItemIDs();

            while (itemIterator.hasNext()) {
                long itemID = itemIterator.nextLong();
                List<RecommendedItem> similarItemsID = ((ItemBasedRecommender) recommender).mostSimilarItems(itemID, getMaxKeep());
                for (RecommendedItem recommendedItem : similarItemsID) {
                    //similarities.add(new RecommendationTuple(userID, similarUserID, new Float(userSimilarity.userSimilarity(userID, similarUserID))));
                    dataProcessor.put(itemID, recommendedItem.getItemID(), recommendedItem.getValue(), timestamp);
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
