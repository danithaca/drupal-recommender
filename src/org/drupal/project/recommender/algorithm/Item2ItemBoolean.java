package org.drupal.project.recommender.algorithm;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.jdbc.MySQLBooleanPrefJDBCDataModel;
import org.apache.mahout.cf.taste.impl.model.jdbc.PostgreSQLBooleanPrefJDBCDataModel;
import org.apache.mahout.cf.taste.impl.model.jdbc.SQL92BooleanPrefJDBCDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericBooleanPrefItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.drupal.project.computing.exception.DCommandExecutionException;


public class Item2ItemBoolean extends Item2Item {

    // This is the same to User2UserBoolean. Since Java doesn't allow multi-derivation, we'll just copy code.
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

    @Override
    protected void initRecommender() throws DCommandExecutionException {
        recommender = new GenericBooleanPrefItemBasedRecommender(dataModel, itemSimilarity);
    }

    @Override
    protected ItemSimilarity getDefaultItemSimilarity() throws TasteException {
        // PersonCorrelationSimilarity will have problem for Boolean preference tables.
        return new LogLikelihoodSimilarity(dataModel);
    }

    protected UserSimilarity getDefaultUserSimilarity() throws TasteException {
        return null; // This recommender doesn't really need UserSimilarity.
    }

}
