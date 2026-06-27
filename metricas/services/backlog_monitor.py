import time
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.errors import NoBrokersAvailable
from kafka.structs import TopicPartition


class BacklogMonitor:
    def __init__(self, bootstrap_servers, topics, group_id):
        self.bootstrap = bootstrap_servers
        self.topics = topics
        self.group_id = group_id
        self.admin = self._connect_admin()

    def _connect_admin(self, max_retries=30):
        for i in range(max_retries):
            try:
                return KafkaAdminClient(bootstrap_servers=self.bootstrap, client_id='metrics-admin')
            except NoBrokersAvailable:
                time.sleep(2)
        return None

    def lag(self):
        out = {}
        for topic in self.topics:
            try:
                consumer = KafkaConsumer(
                    bootstrap_servers=self.bootstrap,
                    group_id=self.group_id,
                    enable_auto_commit=False,
                )
                partitions = consumer.partitions_for_topic(topic) or set()
                tps = [TopicPartition(topic, p) for p in partitions]
                if not tps:
                    consumer.close()
                    out[topic] = {"end_offsets": 0, "committed": 0, "lag": 0, "partitions": 0}
                    continue
                end_offsets = consumer.end_offsets(tps)
                total_end = sum(end_offsets.values())
                total_committed = 0
                for tp in tps:
                    c = consumer.committed(tp)
                    if c is not None:
                        total_committed += c
                consumer.close()
                out[topic] = {
                    "end_offsets": int(total_end),
                    "committed": int(total_committed),
                    "lag": int(max(0, total_end - total_committed)),
                    "partitions": len(tps),
                }
            except Exception as e:
                out[topic] = {"error": f"{type(e).__name__}: {e}"}
        return out
