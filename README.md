# ScalableMine Task

## Exercise 1
Checkout the repo <https://github.com/HenryWedge/KubernetesEnvironmentBuilder>.
Follow the instructions in the `README` of that repository. With that you set up a Kubernetes
environment which is running inside minikube. A kafka cluster, a load generator and a monitoring 
stack including Prometheus and Grafana will be deployed. 

Show that your setup is running. Open the Kafka-UI and submit a screenshot of the messages in the 
`input` topic.

## Exercise 2
Implement the DFG miner from the guest lecture with [Apache Flink](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/overview/). 
The code in the class `FlinkPipeline` can be used as a starting point.

As a result an instance of the `CountedDirectlyFollowsRelations` class should be created and be written to the 
`model` Kafka topic.

In the second step filter the least frequent Directly-Follows-Relations from the result. Decide on a reasonable threshold!

## Exercise 3
Now it is time for testing your stream processing pipeline. 
Run your pipeline from Exercise 2 with the load generator.
Now you can see the algorithms performance via Grafana.

The credentials to open Grafana are
`username: admin`
`password: prom-operator`

Open Grafana and navigate to the DPM-Bench Dashboard. There you can view the number of incoming messages as well 
as the number of processed messages. The record lag states the difference between incoming and produced messages. 

Run the load generator together with your Flink Pipeline and view how the graphs behave. Submit a screenshot of the 
graphs together with a short text how to interpret them.