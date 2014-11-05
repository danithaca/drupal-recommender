package org.drupal.project.recommender.algorithm;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.jdbc.MySQLBooleanPrefJDBCDataModel;
import org.apache.mahout.cf.taste.impl.model.jdbc.PostgreSQLBooleanPrefJDBCDataModel;
import org.apache.mahout.cf.taste.impl.model.jdbc.SQL92BooleanPrefJDBCDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericBooleanPrefUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.drupal.project.computing.exception.DCommandExecutionException;


public class User2UserBoolean extends User2User {

    @Override
    protected void initDataModel() throws DCommandExecutionException {
        if (dbProperties.containsKey("driver") && dbProperties.getProperty("driver").equals("mysql")) {
            dataModel = new MySQLBooleanPrefJDBCDataModel(dataSource, dataStructure.getProperty("preference name"), dataStructure.getProperty("preference user field"), dataStructure.getProperty("preference item field"), dataStructure.getProperty("preference timestamp field"));
        } else if (dbProperties.containsKey("driver") && dbProperties.getProperty("driver").equals("postgresql")) {
            dataModel = new PostgreSQLBooleanPrefJDBCDataModel(dataSource, dataStructure.getProperty("preference name"), dataStructure.getProperty("preference user field"), dataStructure.getProperty("preference item field"), dataStructure.getProperty("preference timestamp field"));
        } else {
            dataModel = new SQL92BooleanPrefJDBCDataModel(dataSource, dataStructure.getProperty("preference name"), dataStructure.getProperty("preference user field"), dataStructure.getProperty("preference item field"), dataStructure.getProperty("preference timestamp field"));
        }
    }


    // Only change the Recommender class.
    @Override
    protected void initRecommender() throws DCommandExecutionException {
        UserNeighborhood neighbor = getNeighborhood();
        recommender = new GenericBooleanPrefUserBasedRecommender(dataModel, neighbor, userSimilarity);
    }

    @Override
    protected UserSimilarity getDefaultUserSimilarity() throws TasteException {
        // PersonCorrelationSimilarity will have problem for Boolean preference tables.
        return new LogLikelihoodSimilarity(dataModel);
    }

    @Override
    protected ItemSimilarity getDefaultItemSimilarity() throws TasteException {
        return null; // This recommender doesn't really need ItemSimilarity.
    }
}
