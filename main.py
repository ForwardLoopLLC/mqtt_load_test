from load_test import *

import os

if __name__ == '__main__':
    host = os.environ['LOAD_TEST_HOST']
    port = int(os.environ['LOAD_TEST_PORT'])

    wait_for_client_to_publish_to_broker(host, port)

    load_test(
        name=os.environ['LOAD_TEST_NAME'], 
        n_sub_clients=int(os.environ['LOAD_TEST_N_SUB_CLIENTS']),
        n_pub_clients=int(os.environ['LOAD_TEST_N_PUB_CLIENTS']),
        host=host,
        port=port,
        keepalive=60,
        test_time=int(os.environ['LOAD_TEST_TIME']),
        sub_connect_rate=int(os.environ['LOAD_TEST_SUB_CONNECT_RATE']),
        pub_message_rate=int(os.environ['LOAD_TEST_PUB_MESSAGE_RATE']),
        n_topics=int(os.environ['LOAD_TEST_N_TOPICS']),
        clean_session=True
    )
    message_data, connect_data = aggregate_test_data(os.environ['LOAD_TEST_NAME'])
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
    plt.savefig(os.environ['LOAD_TEST_PLOT_FILE'])
