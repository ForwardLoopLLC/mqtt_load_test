from load_test import *
from datetime import datetime
import os

if __name__ == '__main__':
    name = 'data/test0'
    plot_file = 't.png'
    message_data, connect_data = aggregate_test_data(name)
    plot_time = max(message_data['time']) - min(connect_data['time'])
    print(plot_time)
    print(len(message_data))
    latency_data = message_latency_statistics(message_data, connect_data)
    _, ax = plt.subplots(3, 1, figsize=(20, 20))
    plot_missed_pattern(latency_data, connect_data, ax=ax[0])
    plot_message_pattern(message_data, connect_data, client_types=['pub'], ax=ax[0])
    ax[0].set_ylabel('Publisher Client ID')
    plot_message_pattern(message_data, connect_data, client_types=['sub'], ax=ax[1])
    ax[1].set_ylabel('Subscriber Client ID')
    plot_latency_pattern(latency_data, connect_data, ax=ax[2])
    ax[2].set_ylabel('Latency (s)')
    for axis in ax:
        axis.set_xlim(0, 1.4*plot_time)
        axis.set_xlabel('')
    ax[-1].set_xlabel('Time (s)')
    details_text = '''Test Details ({test_start_date} UTC)
BROKER_AZ={{broker_az}}
BROKER_INSTANCE={{broker_instance}}
BROKER_AMI={{broker_ami}}
BROKER_TIMEOUT={{broker_timeout}}
BROKER_IP={{broker_ip_type}}

LOAD_AZ={{load_az}}
LOAD_INSTANCE={{load_instance}}
LOAD_AMI={{load_ami}}
LOAD_TIMEOUT={{load_timeout}}
LOAD_TOPICS={{load_topics}}
'''.format(test_start_date=str(datetime.utcnow()))
    add_test_details_to_plot(details_text=details_text, ax0=ax[0])
    plt.savefig(plot_file)
