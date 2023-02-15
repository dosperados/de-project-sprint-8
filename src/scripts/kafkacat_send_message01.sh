kafkacat -P -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
-X security.protocol=SASL_SSL \
-X sasl.mechanisms=SCRAM-SHA-512 \
-X sasl.username="de-student" \
-X sasl.password="ltcneltyn" \
-X ssl.ca.location=/s8-lessons/CA.pem \
-t student.topic.cohort6.dosperados_in \
-K : \
-l /data/restaurant_stream_message01.txt
