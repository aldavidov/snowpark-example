package com.ataccama.snowpark.example;

import com.snowflake.snowpark_java.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final double THRESHOLD = 0.7;

    public static void main(String[] args) {
        Session session = Session.builder()
                .config("URL", "https://sampleIdentifier.snowflakecomputing.com")
                .config("USER", "SAMPLE_USER")
                .config("PASSWORD", "samplePassword")
                .config("ROLE", "SAMPLE_ROLE")
                .config("WAREHOUSE", "SAMPLE_WH")
                .config("DB", "SAMPLE_DB")
                .config("SCHEMA", "SAMPLE_SCHEMA")
                .create();

        try {
            DataFrame table = session.table("payments");

            // Transforms table data frame to aggregated form.
            table = transformDataFrame(table);

            // Collects and prints the result.
            printResults(table);
        } finally {
            session.close();
        }
    }

    private static DataFrame transformDataFrame(DataFrame df) {
        // Creates an expression that checks if the given value obeys the abbreviation rule.
        Column currencyLength = Functions.length(df.col("currency"));
        Column ruleColumn = currencyLength.leq(Functions.lit(3));

        // First transformation of the data frame. Here we add new columns to the table.
        // The change modifies the dataframe only, no computation is happening at this point.
        df = df.withColumn("is_abbreviation", ruleColumn);

        // We count the rows of the table. It is also possible to use the call df.count() instead.
        // This way we can omit one SQL query to the Warehouse which slightly improves performance.
        Column count = Functions.count(Functions.lit(1)).as("total_count");

        // Expression for getting an average length for currency. We can reuse the existing currencyLength variable.
        Column avgLength = Functions.avg(currencyLength).as("avg_length_currency");

        // Expression that computes an average value for the value.
        Column avgValue = Functions.avg(df.col("value")).as("avg_value");

        // Counts how many rows satisfy our rule.
        // Note: Snowpark does not directly contain the function count_if but we can work around that by
        // calling Functions.callUDF which allows to specify arbitrary function and parameters.
        Column ruleCount = Functions.callUDF("count_if", ruleColumn);

        // This is a second transformation of the data frame. The transformation aggregates all the rows in the table
        // to one. The change in the data frame still does not compute anything.
        return df.select(count, avgLength, avgValue, ruleCount);
    }

    private static void printResults(DataFrame df) {
        // All the magic happens here. Snowpark transforms the data frame to SQL, pushes down the query into
        // the Snowflake Warehouse and retrieves the result we want.
        Row[] collected = df.collect();

        if (collected.length != 1) {
            String msg = MessageFormat.format(
                    "Expected exactly 1 record returned from the server, got {0} instead.", collected.length);
            throw new IllegalStateException(msg);
        }

        Row res = collected[0];

        // Result retrieval.
        long cnt = res.getLong(0);
        double avgLength = res.isNullAt(1) ? 0 : res.getDecimal(1).doubleValue();
        double avgValue = res.isNullAt(2) ? 0 : res.getDecimal(2).doubleValue();
        long ruleCount = res.isNullAt(3) ? 0 : res.getLong(3);
        double satisfiedRatio = cnt == 0 ? 0 : (double) ruleCount / cnt;

        log.info("===== PROFILE RESULTS OF TABLE PAYMENTS =====");
        log.info("Number of rows: {}", cnt);
        log.info("Average length of the currency column: {}", avgLength);
        log.info("Average value of the value column: {}", avgValue);

        // Threshold value is 0.7 (70%)
        log.info("===== DOMAIN DETECTION =====");
        log.info("Detection threshold: {}%", THRESHOLD * 100);
        log.info("Rule abbreviation: The rule is satisfied in {}/{} ({}%) cases. Detected: {}", ruleCount, cnt,
                satisfiedRatio * 100, satisfiedRatio >= THRESHOLD);
    }
}