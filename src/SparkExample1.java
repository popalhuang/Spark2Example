import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;



public class SparkExample1 {
	public static void main(String[] args){
		
		SparkSession spark = SparkSession.builder().appName("SparkHiveTest").enableHiveSupport().getOrCreate();
		
		Dataset<Row> df = spark.sql("select * from "+args[0]);
		
		df.printSchema();
		
		List<Row> list = df.collectAsList();
		for(Row row:list){
			System.out.println("name:"+row.getString(0));
		}
		
		
	
	}
}
