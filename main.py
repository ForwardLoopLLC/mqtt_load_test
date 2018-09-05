from load_test import *

import os

if __name__ == '__main__':
    name = 'test0'
    test_time = 30 
    load_test(
        name=name,
        n_sub_clients=2,
        n_pub_clients=2,
        host=os.environ['MQTT_BROKER_HOST'],
        port=1883,
        keepalive=60,
        test_time=test_time,
        sub_connect_rate=2,
        pub_message_rate=2,
        n_topics=2,
        clean_session=True
    )
    message_data, connect_data = aggregate_test_data(name)
    print(len(message_data))
    received_stats_data = message_received_statistics(message_data, connect_data)
    latency_data = message_latency_statistics(message_data, connect_data)
    _, ax = plt.subplots(2, 1, figsize=(20, 10))
    plot_missed_pattern(latency_data, connect_data, ax=ax[0])
    plot_message_pattern(message_data, connect_data, ax=ax[0])
    plot_latency_pattern(latency_data, connect_data, ax=ax[1])
    ax[0].set_yticks(list(range(len(set(connect_data['client_id'].values)))))
    for axis in ax:
        axis.set_xlim(0, test_time+5)
    plt.savefig('test.png')
