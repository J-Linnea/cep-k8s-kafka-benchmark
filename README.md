# ScalableMine Task

##

## Exercise 2

Implement the DFG miner from the lecture with Flink. 
The code in the class `FlinkPipeline` can be used as a starting point.

## Exercise 3

Now it is time for testing your stream processing pipeline. 
Run your pipeline from Exercise 2 with the load generator.
Now you can see how good your algorithm performs via Grafana.

Open Grafana and see the metrics below:

To create a NodePort-Service you can run the 
Grafana Login:
`username: admin`
`password: prom-operator`

Produced messages: `sum by (topic) (kafka_server_brokertopicmetrics_messagesin_total)`
Message Lag: `sum by(consumergroup, topic) (kafka_consumergroup_lag >= 0)`

Test how your pipeline behaves and whether you find limits of their sustainable throughput.

Submit the Grafana Graphs of produced messages and the corresponding lag and state how you ran your experiment.