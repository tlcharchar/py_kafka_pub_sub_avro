# py_kafka_pub_sub_avro
Python scripts that produce and consume avro events from a kafka broker obeying an avro schema from a schema registry


![image](https://github.com/user-attachments/assets/f2f38aef-dfea-4a51-9253-4f0599869c59)


** Commands to be executed in macOS or Linux environment **


1 - Clone this repository to your environment:

  git clone https://github.com/tlcharchar/py_kafka_pub_sub_avro.git
  (You will have a $CLONED_REPOSITORY in your environment)

  
2 - Turn the project repository you cloned into a Python Virtual Environment. You must have Python installed in your environment:

  python -m venv $CLONED_REPOSITORY
  source $CLONED_REPOSITORY/bin/activate
  cd $CLONED_REPOSITORY


3 - Create your stack (Kafka, Schema Registry and Zookeeper). You must have Docker and Docker Compose installed in your environment:
  
  docker-compose up -d


4 - Install the dependencies for your Python scripts:

  pip install -r requirements.txt


5 - Produce an avro event for the Kafka Broker:

  python produce_avro.py


6 - Consume the previously produced avro event from the Kafka Broker:

  python consume_avro.py


7 - To stop the stack you created:

  docker-compose stop


8 - To disable the Python Virtual Environment:

  $CLONED_REPOSITORY/deactivate
