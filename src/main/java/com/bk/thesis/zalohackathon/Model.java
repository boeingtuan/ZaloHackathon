package com.bk.thesis.zalohackathon;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.model.DBCollectionUpdateOptions;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;
import spark.Request;
import spark.Response;
import spark.Route;
import spark.Spark;

/**
 *
 * @author boeingtuan
 */
public class Model implements Serializable {

    public static final Mongo mongo = new MongoClient("0.0.0.0", 2701);
    public static final DB db = mongo.getDB("test");
    public static final DBCollection collection = db.getCollection("zmp3");

    private static final MatrixFactorizationModel _model;
    private static final Map<Integer, String> _cache;

    static {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/zalo.hackathon")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/zalo.hackathon")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        _model = MatrixFactorizationModel.load(jsc.sc(), "model_save");
        _cache = new HashMap();
    }

    public static class Song implements Serializable {

        private int userId;
        private int songId;
        private float numberListen;

        public Song() {
        }

        public Song(int userId, int movieId, float rating) {
            this.userId = userId;
            this.songId = movieId;
            this.numberListen = rating;
        }

        public int getUserId() {
            return userId;
        }

        public int getSongId() {
            return songId;
        }

        public float getNumberListen() {
            return numberListen;
        }

        public static Song parseSongDocument(Document doc) {
            int userId = Integer.parseInt((String) doc.get("user"));
            int songId = Integer.parseInt((String) doc.get("item"));
            int numberListen = (int) doc.get("count");
            return new Song(userId, songId, numberListen);
        }
    }

    public static void train() throws IOException {
//        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN);
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://0.0.0.0:2701/test.training_zmp3")
                .config("spark.mongodb.output.uri", "mongodb://0.0.0.0:2701/test.training_zmp3")
                .config("spark.ui.showConsoleProgress", true)
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        JavaRDD<Rating> songRDD = rdd.map(new Function<Document, Rating>() {
            @Override
            public Rating call(Document doc) throws Exception {
                int userId = Integer.parseInt((String) doc.get("userId"));
                int songId = Integer.parseInt((String) doc.get("songId"));
                int numberListen = (int) doc.get("count");
                return new Rating(userId, songId, numberListen);
            }
        });

        MatrixFactorizationModel model = ALS.trainImplicit(JavaRDD.toRDD(songRDD), 10, 5);

        JavaRDD<Tuple2<Object, Object>> userProducts
                = songRDD.map(r -> new Tuple2<>(r.user(), r.product()));
        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
                model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD()
                        .map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating()))
        );
        JavaRDD<Tuple2<Double, Double>> ratesAndPreds = JavaPairRDD.fromJavaRDD(
                songRDD.map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating())))
                .join(predictions).values();
        double MSE = ratesAndPreds.mapToDouble(pair -> {
            double err = pair._1() - pair._2();
            return err * err;
        }).mean();
        System.out.println("Mean Squared Error = " + MSE);

        model.save(jsc.sc(), "model_save");

        jsc.stop();
        spark.stop();
    }

    public static String predict(int userId) {
        try {
            Rating[] recommendProducts = _model.recommendProducts(userId, 100);
            List<Integer> res = new ArrayList();
            for (Rating song : recommendProducts) {
                res.add(song.product());
            }
            String ret = Arrays.toString(res.toArray());
            _cache.put(userId, ret);
            return ret;

        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Fail: " + userId);
        }
        return "[]";
    }

    public static void predictAll() {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/zalo.hackathon")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/zalo.hackathon")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        MatrixFactorizationModel model = MatrixFactorizationModel.load(jsc.sc(), "model_save");

        RDD<Tuple2<Object, Rating[]>> recommendProductsForUsers = model.recommendProductsForUsers(100);
        System.out.println("OKKKKKKKKKKKKKKKKKKKK");

        recommendProductsForUsers.foreach(new ForEachRecommend());

    }

    public static class ForEachRecommend extends AbstractFunction1<Tuple2<Object, Rating[]>, BoxedUnit> implements Serializable {

        @Override
        public BoxedUnit apply(Tuple2<Object, Rating[]> data) {
            System.out.println("----------------111---------------");
            Integer userId = (Integer) data._1;
            Rating[] ratings = data._2;
            try {
                BasicDBObject query = new BasicDBObject();
                query.put("_id", userId + "");

                BasicDBObject songDoc = new BasicDBObject();
                for (Rating song : ratings) {
                    songDoc.put(song.product() + "", song.rating());
                }

                BasicDBObject newDocument = new BasicDBObject();
                newDocument.put("songRecommend", songDoc);

                BasicDBObject updateObj = new BasicDBObject();
                updateObj.put("$set", newDocument);

                collection.update(query, updateObj, new DBCollectionUpdateOptions().upsert(true));
            } catch (Exception ex) {
                System.out.println("Fail: " + userId);
            }
            return BoxedUnit.UNIT;
        }

    }

    public static void start() {
        Spark.port(8080);
        Spark.get("/predict", new Route() {
            @Override
            public Object handle(Request request, Response response) throws Exception {
                int userId = Integer.parseInt((String) request.queryParams("userId"));
                if (_cache.get(userId) != null) {
                    return _cache.get(userId);
                }
                String render = predict(userId);
                return render;
            }
        });
    }

    public static void main(String[] args) throws IOException {
//        train();
//        load(330531430);
//        predictAll();
        start();
    }
}
