package org.drupal.project.recommender.utils;

/**
 * This saves the tuple of (entity1, entity2, score)
 */
public class RecommendationTuple {

    private long id1;
    private long id2;
    private float score;

    public RecommendationTuple(long id1, long id2, float score) {
        this.id1 = id1;
        this.id2 = id2;
        this.score = score;
    }

    public long getId1() {
        return id1;
    }

    public long getId2() {
        return id2;
    }

    public float getScore() {
        return score;
    }
}
