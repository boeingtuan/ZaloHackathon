package com.bk.thesis.zalohackathon;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

/**
 *
 * @author boeingtuan
 */
public class ModelTest {

    public static class Rating implements Serializable {

        private int userId;
        private int movieId;
        private float rating;
        private long timestamp;

        public Rating() {
        }

        public Rating(int userId, int movieId, float rating, long timestamp) {
            this.userId = userId;
            this.movieId = movieId;
            this.rating = rating;
            this.timestamp = timestamp;
        }

        public int getUserId() {
            return userId;
        }

        public int getMovieId() {
            return movieId;
        }

        public float getRating() {
            return rating;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public static Rating parseRating(String str) {
            String[] fields = str.split("::");
            if (fields.length != 4) {
                throw new IllegalArgumentException("Each line must contain 4 fields");
            }
            int userId = Integer.parseInt(fields[0]);
            int movieId = Integer.parseInt(fields[1]);
            float rating = Float.parseFloat(fields[2]);
            long timestamp = Long.parseLong(fields[3]);
            return new Rating(userId, movieId, rating, timestamp);
        }

        public static Rating parseRatingDocument(Document doc) {
            int userId = (int) doc.get("userId");
            int movieId = (int) doc.get("movieId");
            float rating = (float) (double) doc.get("rating");
            long timestamp = (int) doc.get("timestamp");
            return new Rating(userId, movieId, rating, timestamp);
        }
    }

    public static void train_test() {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/zalo.hackathon")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/zalo.hackathon")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        JavaRDD<Rating> ratingRDD = rdd.map(Rating::parseRatingDocument);
        Dataset<Row> ratings = spark.createDataFrame(ratingRDD, Rating.class);
        Dataset<Row>[] splits = ratings.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> training = splits[0];
        Dataset<Row> test = splits[1];

        // Build the recommendation model using ALS on the training data
        ALS als = new ALS()
                .setMaxIter(5)
                .setRegParam(0.01)
                .setUserCol("userId")
                .setItemCol("movieId")
                .setRatingCol("rating");
        ALSModel model = als.fit(training);

        // Evaluate the model by computing the RMSE on the test data
        // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
        model.setColdStartStrategy("drop");
        Dataset<Row> predictions = model.transform(test);

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setMetricName("rmse")
                .setLabelCol("rating")
                .setPredictionCol("prediction");
        Double rmse = evaluator.evaluate(predictions);
        System.out.println("Root-mean-square error = " + rmse);

        // Generate top 10 movie recommendations for each user
        Dataset<Row> userRecs = model.recommendForAllUsers(10);
        // Generate top 10 user recommendations for each movie
        Dataset<Row> movieRecs = model.recommendForAllItems(10);

        // Generate top 10 movie recommendations for a specified set of users
        Dataset<Row> users = ratings.select(als.getUserCol()).distinct().limit(3);
        Dataset<Row> userSubsetRecs = model.recommendForAllUsers(10);
        // Generate top 10 user recommendations for a specified set of movies
        Dataset<Row> movies = ratings.select(als.getItemCol()).distinct().limit(3);
        Dataset<Row> movieSubSetRecs = model.recommendForAllItems(10);
        // $example off$
        userRecs.show();
        movieRecs.show();
        userSubsetRecs.show();
        movieSubSetRecs.show();

        jsc.stop();
        spark.stop();
    }

    public static void putDemo() {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/zalo.hackathon")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/zalo.hackathon")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        try (BufferedReader br = new BufferedReader(new FileReader("data_test.txt"))) {

            String sCurrentLine;
            List ratings = new ArrayList();
            while ((sCurrentLine = br.readLine()) != null) {
                Rating rating = Rating.parseRating(sCurrentLine);
                ratings.add(rating);
            }
            JavaRDD<Document> sparkDocuments = jsc.parallelize(ratings)
                    .map(new Function<Rating, Document>() {
                        @Override
                        public Document call(Rating data) throws Exception {
                            try {
                                ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
                                String json = ow.writeValueAsString(data);
                                Document dataParse = Document.parse(json);
                                return dataParse;
                            } catch (Exception ex) {
                            }
                            return null;
                        }
                    });
            MongoSpark.save(sparkDocuments);

        } catch (IOException e) {
            e.printStackTrace();
        }
        spark.close();

    }

    public static void main(String[] args) {
        train_test();
//        putDemo();
    }
}
