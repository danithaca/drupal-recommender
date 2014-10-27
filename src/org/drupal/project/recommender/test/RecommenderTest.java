package org.drupal.project.recommender.test;

import com.google.common.annotations.VisibleForTesting;
import org.drupal.project.computing.DApplication;
import org.drupal.project.recommender.DefaultApplication;
import org.junit.Test;

public class RecommenderTest {

    @Test
    public void testLaunch() {
        DApplication application = new DefaultApplication();
        application.launch();
    }
}
