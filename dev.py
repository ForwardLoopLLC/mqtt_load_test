from load_test import *

import os

if __name__ == '__main__':
    name = 'data/test0'
    plot_file = 't.png'
    message_data, connect_data = aggregate_test_data(name)
    plot_time = max(message_data['time']) - min(connect_data['time'])
    print(plot_time)
    print(len(message_data))
    print(received_stats_data.mean())
    latency_data = message_latency_statistics(message_data, connect_data)
    _, ax = plt.subplots(3, 1, figsize=(20, 20))
    plot_message_pattern(message_data, connect_data, client_types=['pub'], ax=ax[0])
    ax[0].set_ylabel('Publisher Client ID')
    plot_missed_pattern(latency_data, connect_data, ax=ax[1])
    plot_message_pattern(message_data, connect_data, client_types=['sub'], ax=ax[1])
    ax[1].set_ylabel('Subscriber Client ID')
    plot_latency_pattern(latency_data, connect_data, ax=ax[2])
    ax[2].set_ylabel('Latency (s)')
    #n_sub_clients = 9#int(max(connect_data['client_id']))
    #ax[0].set_yticks(np.round(np.append(np.linspace(0, n_sub_clients, 9), n_sub_clients)))
    for axis in ax:
        axis.set_xlim(0, plot_time+5)
        axis.set_xlabel('')
    ax[-1].set_xlabel('Time (s)')
    plt.savefig(plot_file)
