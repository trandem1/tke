package test;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.JSONArray;
import org.json.JSONObject;

public class Read_Demo {
	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf().setAppName("update_db")
				.set("spark.sql.parquet.binaryAsString", "true");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		jsc.setLogLevel("ERROR");
		SQLContext sqlContext = new SQLContext(jsc);
		DataFrame df = sqlContext.read().load("/data/Parquet/AdnLog/2018_07_16");
		df.registerTempTable("data");
		DataFrame url = sqlContext.sql("select distinct domain , path, concat(domain,path) as url from data");
		JavaRDD<Row> url_rdd = url.javaRDD().map(new Function<Row, Row>() {

			public String makeGetRequest(String stringUrl) throws IOException {
				
				try {
					URL url = new URL(stringUrl);
					URLConnection con = url.openConnection();
					HttpURLConnection httpCon = (HttpURLConnection) con;
					httpCon.setRequestMethod("GET");
					BufferedReader br = new BufferedReader(new InputStreamReader(httpCon.getInputStream()));
					String line = "";
					while (br.ready()) {
						line += br.readLine();
					}
					br.close();
					JSONObject json = new JSONObject(line);
					JSONArray arr = json.getJSONArray("list_cate");
					float sum = 0.0F;
					String result = "";
					for (int i = 0; i < arr.length(); i++) {
						String[] object = arr.getString(i).split(":");
						sum += Float.parseFloat(object[1]);
						if (sum >= 0.8) {
							result += object[0] + " ";
							break;
						} else {
							result += object[0] + " ";
						}
					}
					return result.trim();
				}catch (Exception e) {
					// TODO: handle exception
					return "-1";
				}
			
			}

			@Override
			public Row call(Row row) throws Exception {
				// TODO Auto-generated method stub
				String url = "http://192.168.23.189:5000/search_detail?url=http://" + row.getString(2);
				String result = makeGetRequest(url);
				Row kq = RowFactory.create(row.getString(0), row.getString(1), row.getString(2), result);
				return kq;
			}
		});
		StructType schema = new StructType(
				new StructField[] { new StructField("domain", DataTypes.StringType, true, Metadata.empty()),
						new StructField("path", DataTypes.StringType, true, Metadata.empty()),
						new StructField("url", DataTypes.StringType, true, Metadata.empty()),
						new StructField("cate", DataTypes.StringType, true, Metadata.empty()) });
		DataFrame d4 = sqlContext.createDataFrame(url_rdd, schema);
		d4.write().format("parquet").save("/user/demtv/url");
	}
}