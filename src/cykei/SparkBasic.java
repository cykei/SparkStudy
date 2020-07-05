package cykei;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SparkBasic {
    public static JavaSparkContext connectSpark(){
        SparkConf sparkConf = new SparkConf().setAppName("SparkBasic").setMaster("local[3]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        return jsc;
    }
    public static void main(String[] args) {
        // 1. spark context 생성 -rdd를 생성을 위함
        SparkConf sparkConf = new SparkConf().setAppName("SparkBasic").setMaster("local[3]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // 2. RDD만들기 - parallelize()
        String text = "안녕! 안녕 만나서 반가워 으하핳 으힣";
        JavaRDD<String> rdd = jsc.parallelize(Arrays.asList(text.split(" ")));

        // 3. map()
        JavaRDD<String> rdd1 = rdd.map((String s) -> s+"!");

        // 4. RDD를 배열로 - collect()
        List<String> result = rdd1.collect();
        for (String i : result){
            System.out.println(i);
        }

        // 5. RDD 전체 요소 수 반환 - Count()
        long cnt = rdd.count();
        System.out.println(cnt);
    }
}
