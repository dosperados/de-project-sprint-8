# %%
import os

from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType

kafka_bootstrap_servers = 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091'

kafka_user = 'kafka-admin'
kafka_pass = 'de-kafka-admin-2022'

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="{kafka_user}" password="{kafka_pass}";',
}

postgresql_settings = {
    'user': 'jovyan',
    'password': 'jovyan',
    'url': 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de',
    'driver': 'org.postgresql.Driver',
    'dbtable': 'public.subscribers_feedback',
}
# .option('user', 'student') \
# .option('password', 'de-student') \

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

TOPIC_NAME_IN = 'student.topic.cohort6.dosperados_in'
TOPIC_NAME_OUT = 'student.topic.cohort6.dosperados_out'


# %%
# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()
    
    # Добавляем пустое поле 'feedback'
    feedback_df = df.withColumn('feedback', F.lit(None).cast(StringType()))
    
    # записываем df в PostgreSQL с полем feedback
    # #Первый вариант записи в PostgreSQL
    # postgresql_save = (feedback_df
    #     .select("restaraunt_id", "adv_campaign_id", "adv_campaign_content", "adv_campaign_owner", "adv_campaign_owner_contact", "adv_campaign_datetime_start", "adv_campaign_datetime_end", "datetime_created", "client_id", "trigger_datetime_created", "feedback")
    #     .write.format("jdbc")
    #     .mode('append')
    #     .options(**postgresql_settings)
    #     .save()
    # )

    
    #Отправляем в postgresql
    feedback_df.write.format('jdbc').mode('append') \
    .options(**postgresql_settings).save()

    # # создаём df для отправки в Kafka. Сериализация в json.
    df_to_stream = (feedback_df
        .select(F.to_json(F.struct(F.col('*'))).alias('value')) 
        .select('value')
    )

    # отправляем сообщения в результирующий топик Kafka без поля feedback
    df_to_stream.write \
        .format('kafka') \
        .option('kafka.bootstrap.servers', kafka_bootstrap_servers) \
        .options(**kafka_security_options) \
        .option('topic', TOPIC_NAME_OUT) \
        .option('truncate', False) \
        .save()

    # очищаем память от df
    df.unpersist()


# %%
# Функция создания сессии Spark с бибилиотеками 'spark_jars_packages'
def spark_init(Spark_Session_Name) -> SparkSession:

    return (SparkSession
        .builder
        .appName({Spark_Session_Name})
        .config("spark.jars.packages", spark_jars_packages)
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    )


# %%
# определяем текущее время в UTC в миллисекундах
current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

def restaurant_read_stream(spark):
    df = spark.readStream \
        .format('kafka') \
        .options(**kafka_security_options) \
        .option('kafka.bootstrap.servers', kafka_bootstrap_servers) \
        .option('subscribe', TOPIC_NAME_IN) \
        .load()

 
    df_json = df.withColumn('key_str', F.col('key').cast(StringType())) \
        .withColumn('value_json', F.col('value').cast(StringType())) \
        .drop('key', 'value')
 
    # определяем схему входного сообщения для json
    incoming_message_schema = StructType([
        StructField('restaurant_id', StringType(), nullable=True),
        StructField('adv_campaign_id', StringType(), nullable=True),
        StructField('adv_campaign_content', StringType(), nullable=True),
        StructField('adv_campaign_owner', StringType(), nullable=True),
        StructField('adv_campaign_owner_contact', StringType(), nullable=True),
        StructField('adv_campaign_datetime_start', LongType(), nullable=True),
        StructField('adv_campaign_datetime_end', LongType(), nullable=True),
        StructField('datetime_created', LongType(), nullable=True),
    ])
 
    # десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
    df_string = df_json \
        .withColumn('key', F.col('key_str')) \
        .withColumn('value', F.from_json(F.col('value_json'), incoming_message_schema)) \
        .drop('key_str', 'value_json')
 
    df_filtered = df_string.select(
        F.col('value.restaurant_id').cast(StringType()).alias('restaurant_id'),
        F.col('value.adv_campaign_id').cast(StringType()).alias('adv_campaign_id'),
        F.col('value.adv_campaign_content').cast(StringType()).alias('adv_campaign_content'),
        F.col('value.adv_campaign_owner').cast(StringType()).alias('adv_campaign_owner'), 
        F.col('value.adv_campaign_owner_contact').cast(StringType()).alias('adv_campaign_owner_contact'), 
        F.col('value.adv_campaign_datetime_start').cast(LongType()).alias('adv_campaign_datetime_start'), 
        F.col('value.adv_campaign_datetime_end').cast(LongType()).alias('adv_campaign_datetime_end'), 
        F.col('value.datetime_created').cast(LongType()).alias('datetime_created'),
    )\
    .filter((F.col('adv_campaign_datetime_start') <= current_timestamp_utc) & (F.col('adv_campaign_datetime_end') > current_timestamp_utc))
 
    return df_filtered

# %%
# вычитываем всех пользователей с подпиской на рестораны
def subscribers_restaurants(spark):
    df = spark.read \
        .format('jdbc') \
        .options(**postgresql_settings) \
        .load()
    df_dedup = df.dropDuplicates(['client_id', 'restaurant_id'])
    return df_dedup

# %%
# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
 
def join(restaurant_read_stream_df, subscribers_restaurant_df):
    df = restaurant_read_stream_df \
        .join(subscribers_restaurant_df, 'restaurant_id') \
        .withColumn('trigger_datetime_created', F.lit(current_timestamp_utc))\
        .select(
            'restaurant_id',
            'adv_campaign_id',
            'adv_campaign_content',
            'adv_campaign_owner',
            'adv_campaign_owner_contact',
            'adv_campaign_datetime_start',
            'adv_campaign_datetime_end',
            'datetime_created',
            'client_id',
            'trigger_datetime_created')
    return df



# %%
if __name__ == '__main__':
    # создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
    spark = spark_init("RestaurantSubscribeStreamingService")
    
    spark.conf.set('spark.sql.streaming.checkpointLocation', 'test_query')
    #client_stream = read_client_stream(spark)
    restaurant_read_stream_df = restaurant_read_stream(spark)
    subscribers_restaurant_df = subscribers_restaurants(spark)
    result = join(restaurant_read_stream_df, subscribers_restaurant_df)
 
    result.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start()



