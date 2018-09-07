from load_test import *

import os

if __name__ == '__main__':
    name = os.environ['LOAD_TEST_NAME']
    host = os.environ['LOAD_TEST_HOST']
    port = int(os.environ['LOAD_TEST_PORT'])
    n_sub_clients = int(os.environ['LOAD_TEST_N_SUB_CLIENTS'])
    n_pub_clients = int(os.environ['LOAD_TEST_N_PUB_CLIENTS'])
    test_time = int(os.environ['LOAD_TEST_TIME'])
    sub_connect_rate = int(os.environ['LOAD_TEST_SUB_CONNECT_RATE'])
    pub_message_rate = int(os.environ['LOAD_TEST_PUB_MESSAGE_RATE'])
    n_topics = int(os.environ['LOAD_TEST_N_TOPICS'])
    subscriber_headstart_time = int(os.environ['LOAD_TEST_SUB_HEADSTART'])
    message_drain_time = int(os.environ['LOAD_TEST_DRAIN_TIME'])
    plot_file = os.environ['LOAD_TEST_PLOT_FILE']

    wait_for_client_to_publish_to_broker(host, port)

    load_test(
        name=name,
        n_sub_clients=n_sub_clients,
        n_pub_clients=n_pub_clients,
        host=host,
        port=port,
        keepalive=60,
        test_time=test_time,
        sub_connect_rate=sub_connect_rate,
        pub_message_rate=pub_message_rate,
        n_topics=n_topics,
        clean_session=True,
        subscriber_headstart_time=subscriber_headstart_time,
        message_drain_time=message_drain_time
    )
    message_data, connect_data = aggregate_test_data(name)
    print(len(message_data))
    plot_time = max(message_data['time']) - min(connect_data['time'])
    received_stats_data = message_received_statistics(message_data, connect_data)
    latency_data = message_latency_statistics(message_data, connect_data)
    _, ax = plt.subplots(2, 1, figsize=(20, 10))
    plot_missed_pattern(latency_data, connect_data, ax=ax[0])
    plot_message_pattern(message_data, connect_data, ax=ax[0])
    plot_latency_pattern(latency_data, connect_data, ax=ax[1])
    ax[0].set_yticks(list(range(len(set(connect_data['client_id'].values)))))
    for axis in ax:
        axis.set_xlim(0, plot_time+5)
    plt.savefig(plot_file)
