package org.drupal.project.recommender;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.mahout.cf.taste.model.DataModel;
import org.drupal.project.computing.DCommand;
import org.drupal.project.computing.DConfig;
import org.drupal.project.computing.DUtils;
import org.drupal.project.computing.exception.DNotFoundException;

import javax.script.Bindings;
import javax.sql.DataSource;
import java.util.Properties;

abstract public class RecommenderCommand extends DCommand {

    // settings loaded from prepare().
    protected Properties dbProperties;
    protected Properties dataStructure;
    protected Properties extraOptions;

    // data initialized from execute().
    protected long numUsers;
    protected long numItems;

    @Override
    public void prepare(Bindings params) throws IllegalArgumentException {
        // handle databases properties.
        if (params.containsKey("database")) {
            // if set from record, then use that.
            dbProperties = DUtils.getInstance().bindingsToProperties((Bindings) params.get("database"));
        } else {
            try {
                dbProperties = DConfig.loadDefault().getDatabaseProperties();
            } catch (DNotFoundException e) {
                e.printStackTrace();
                throw new IllegalArgumentException("Cannot get database settings from either the computing record or config.properties.", e);
            }
        }

        // test dbProperties and further process.
        if (!dbProperties.containsKey("driver") || !dbProperties.containsKey("username") || !dbProperties.containsKey("password")) {
            throw new IllegalArgumentException("Illegal database properties.");
        }
        if (!dbProperties.containsKey("driverClassName")) {
            switch (dbProperties.getProperty("driver")) {
                case "mysql":
                    dbProperties.setProperty("driverClassName", "com.mysql.jdbc.Driver");
                    break;
                case "postgresql":
                    dbProperties.setProperty("driverClassName", "org.postgresql.Driver");
                    break;
            }
        }
        if (!dbProperties.containsKey("url")) {
            String url = String.format("jdbc:%s://%s%s/%s", dbProperties.getProperty("driver"), dbProperties.getProperty("host", "localhost"), StringUtils.isNotEmpty(dbProperties.getProperty("port")) ? ":" + dbProperties.getProperty("port") : "", dbProperties.getProperty("database", ""));
            System.out.println(url);
            dbProperties.put("url", url);
        }

        // build data structure
        dataStructure = new Properties();
        Bindings dataStructureBindings = (Bindings) params.get("data structure");
        for (String tableKey: new String[] {"preference", "item similarity", "user similarity", "prediction"}) {
            if (dataStructureBindings.containsKey(tableKey)) {
                Bindings tableStructure = (Bindings) dataStructureBindings.get(tableKey);
                for (String tableField: tableStructure.keySet()) {
                    dataStructure.put(tableKey + ' ' + tableField, tableStructure.get(tableField));
                }
            }
        }

        // handle extra options
        try {
            extraOptions = params.containsKey("options") ? DUtils.getInstance().bindingsToProperties((Bindings) params.get("options")) : new Properties();
        } catch (ClassCastException e) {
            extraOptions = new Properties();
        }
    }

}
