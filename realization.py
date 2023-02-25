import os 

from datetime import datetime 

from pyspark.sql import SparkSession 

 

# Завожу сессию 

spark_jars_packages = ",".join( 

    [ 

        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0", 

        "org.postgresql:postgresql:42.4.0", 

    ] 

) 

spark = SparkSession.builder \ 

    .appName("RestaurantSubscribeStreamingService") \ 

    .config("spark.sql.session.timeZone", "UTC") \ 

    .config("spark.jars.packages", spark_jars_packages) \ 

    .getOrCreate() 

     

# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров 

from pyspark.sql import functions as f 


def foreach_batch_function(df, epoch_id): 

    df.write \ 

        .format('jdbc') \ 

        .option('url', 'jdbc:postgresql://') \ 

        .option('driver', 'org.postgresql.Driver') \ 

        .option('dbtable', 'subscribers_restaurants') \ 

        .option('user', '') \ 

        .option('password', '') \ 

        .mode("append") \ 

        .save() 

    # записываем df в PostgreSQL с полем feedback 

    df.withColumn("feedback", f.lit(None).cast(StringType())).drop('id') \ 

        .write.format("jdbc") \ 

        .mode('append') \ 

        .option('url', 'jdbc:postgresql://') \ 

        .option('dbtable', 'public.subscribers_feedback') \ 

        .option('user', '') \ 

        .option('password', '') \ 

        .option('driver', 'org.postgresql.Driver') \ 

        .save() 

    # создаём df для отправки в Kafka. Сериализация в json. 

    json_df = df.withColumn('value', f.to_json(f.struct("restaurant_id", "adv_campaign_id", \ 

            "adv_campaign_content", "adv_campaign_owner", "adv_campaign_owner_contact", \ 

            "adv_campaign_datetime_start","adv_campaign_datetime_end", "client_id", "datetime_created", \ 

            "trigger_datetime_created"))) \ 

            .select('value') 

     

    # отправляем сообщения в результирующий топик Kafka без поля feedback 

    kafka_security_options = { 

        'kafka.bootstrap.servers':'yandexcloud.net:port', 

        'kafka.security.protocol': '', 

        'kafka.sasl.mechanism': '', 

        'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"\" password=\"\";', 

        "subscribe": '' 

    } 


    query = (json_df 

        .writeStream 

        .outputMode("append") 

        .format("kafka") 

        .option('kafka.bootstrap.servers', 'yandexcloud.net:port') 

        .options(**kafka_security_options) 

        .option("topic", '') 

        .trigger(processingTime="15 seconds") 

        .option("truncate", False) 

        .start()) 

     

    # очищаем память от df 

    df.unpersist() 

     

restaurant_read_stream_df = spark.readStream \ 

    .format('kafka') \ 

    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \ 

    .option('kafka.security.protocol', 'SASL_SSL') \ 

    .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username="login" password="password";') \ 

    .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \ 

    .option('kafka.ssl.truststore.location', '') \ 

    .option('kafka.ssl.truststore.password', '') \ 

    .option('subscribe', '') \ 

    .load() 

 

# определяем схему входного сообщения для json 

from pyspark.sql.types import * 

raw_incomming_message_schema = [ 

                           StructField("restaurant_id", StringType(), True), 

                           StructField("adv_campaign_id", StringType(), True), 

                           StructField("adv_campaign_content", StringType(), True), 

                           StructField("adv_campaign_owner", StringType(), True), 

                           StructField("adv_campaign_owner_contact", StringType(), True), 

                           StructField("adv_campaign_datetime_start", IntegerType(), True), 

                           StructField("adv_campaign_datetime_end", IntegerType(), True), 

                           StructField("client_id", StringType(), True), 

                           StructField("datetime_created", IntegerType(), True), 

                           StructField("trigger_datetime_created", IntegerType(), True), 

] 


incomming_message_schema = StructType(fields = raw_incomming_message_schema) 
 

# определяем текущее время в UTC в миллисекундах 

current_timestamp_utc = int(round(datetime.utcnow().timestamp())) 


# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции 

from pyspark.sql import functions as f 

filtered_read_stream_df = restaurant_read_stream_df \ 

                          .select(f.col("value").cast(StringType()).alias("value_str")) \ 

                          .withColumn("deserialized_value", f.from_json(f.col("value_str"), schema=incomming_message_schema)) \ 

                          .withColumn('timestamp', f.to_timestamp('datetime_created').alias("timestamp")) \ 

                          .select("deserialized_value.*") \ 

                          .dropDuplicates(['restaurant_id','timestamp']) \ 

                          .withWatermark('datetime_created', '5 minute') 

# вычитываем всех пользователей с подпиской на рестораны 

subscribers_restaurant_df = spark.read \ 

                    .format('jdbc') \ 

                    .option('url', 'yandexcloud.net:port/db') \ 

                    .option('driver', 'org.postgresql.Driver') \ 

                    .option('dbtable', 'subscribers_restaurants') \ 

                    .option('user', '') \ 

                    .option('password', '') \ 

                    .load() 

                     

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события. 

from pyspark.sql.functions import * 


result_df= filtered_read_stream_df.join(subscribers_restaurant_df, 'restaurant_id')\ 

                                     .withColumn("trigger_datetime_created", lit(int(round(datetime.utcnow().timestamp())))) 


# запускаем стриминг 

result_df.writeStream \ 

    .foreachBatch(foreach_batch_function) \ 

    .start() \ 

    .awaitTermination()  
