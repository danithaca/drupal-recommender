package org.drupal.project.recommender;

import org.drupal.project.computing.DApplication;

import java.util.Properties;


/**
 * This registers the Recommender Application to Drupal
 */
public class DefaultApplication extends DApplication {

    public DefaultApplication() {
        super("recommender");
    }

    @Override
    protected Properties declareCommandMapping() {
        Properties defaultCommandMapping = new Properties();
        defaultCommandMapping.put("user2user", "org.drupal.project.recommender.algorithm.User2User");
        defaultCommandMapping.put("user2user_boolean", "org.drupal.project.recommender.algorithm.User2UserBoolean");
        defaultCommandMapping.put("item2item", "org.drupal.project.recommender.algorithm.Item2Item");
        defaultCommandMapping.put("item2item_boolean", "org.drupal.project.recommender.algorithm.Item2ItemBoolean");
        return defaultCommandMapping;
    }

    public static void main(String[] args) {
        DApplication application = new DefaultApplication();
        application.launch();
    }
}
