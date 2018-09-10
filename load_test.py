import matplotlib
# use Agg instead of tk
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import paho.mqtt.client as paho
import paho.mqtt.publish as publish
import pandas as pd
pd.set_option('display.max_columns', 20)
pd.options.mode.chained_assignment = None
import threading
import os.path
import os

from random import choice
from time import time, sleep

COLORS = {
    'sub' : 'g',
    'pub' : 'b',
    'connection' :'y',
    'missed' : 'r',
    'latency' : 'k'
}

def wait_for_client_to_publish_to_broker(host, port):
    '''
    Block and wait for single client to send and receive message from broker

    This is useful for starting a broker and load testing server at the
    same time and then waiting for the broker to initialize and begin
    accepting connections and passing pub/sub messages.

    Args:
        host (str):
            URL or IP address of broker
        port (int):
            MQTT port of broker
    '''
    wait_topic = 'wait/wait'
    wait_message = 'wait'
    is_ready = False
    def on_message(mqttc, obj, msg):
        if msg.topic == wait_topic and msg.payload.decode('utf8') == wait_message:
            mqttc.disconnect()
            mqttc.is_ready = True
    client = paho.Client('wait')
    client.on_message = on_message
    client.is_ready = False
    while(not client.is_ready):
        try:
            client.connect(host, port)
            client.subscribe(wait_topic)
            client.loop_start()
            for _ in range(3):
                publish.single(
                    wait_topic,
                    payload=wait_message,
                    hostname=host,
                    port=port
                )
                sleep(2)
        except (OSError, ConnectionRefusedError):
            sleep(10)

def _sub_client(client_id, start_delay, host, port, keepalive, clean_session, n_topics, disconnect, message_drain_time):
    '''
    Start a parallel subscriber client

    Args:
        client_id (int):
            unique integer to identify client
        start_delay (float):
            time in seconds to wait before establishing connection; this
            parameter allows for client subscriptions to be staggered
            across time
        host (str):
            URL or IP address of broker
        port (int):
            MQTT port of broker
        keepalive (int):
            time in seconds between MQTT keepalive messages; every keepalive
            seconds the client will send a message to the broker to
            keep the persistent TCP connection open
        clean_session (bool):
            clean session flag to pass to broker during connection; if True,
            the client will request that the broker discard information about
            previous sessions associated with the client
        n_topics (int):
            number of test topics to which to subscribe
        disconnect (Disconnect):
            object with a .is_set() method that indicates whether
            disconnect has been signaled; this is how the main process
            communicates with client threads to tell them to disconnect
            clients and stop running
        message_drain_time (float):
            time in seconds to maintain connection and process subscribed
            topics AFTER disconnect has been set; if your broker-to-client
            latency is greater than message_drain_time, you may miss messages
            at the end of the test because the subscriber clients disconnect
            before the messages arrive, so choose a value that is greater
            than your expected network latency
    '''
    sleep(start_delay)
    name = client_id.split('_')[0]
    sub_client_log_file = '{name}/sub/{client_id}.csv'.format(
        name=name,
        client_id=client_id
    )
    def on_connect(mqttc, obj, flags, rc):
        csv_row = '{time},CONNECT\n'.format(time=time())
        with open(sub_client_log_file, 'a') as sclf:
            sclf.write(csv_row)
    def on_message(mqttc, obj, msg):
        csv_row = '{time},{topic},{payload}\n'.format(
            time=str(time()),
            topic=msg.topic,
            payload=msg.payload.decode('utf8'))
        with open(sub_client_log_file, 'a') as sclf:
            sclf.write(csv_row)
    client = paho.Client(client_id, clean_session=clean_session)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(host, port, keepalive)
    for topic in range(n_topics):
        client.subscribe('test/{topic}'.format(topic=topic))
    client.loop_start()
    while(not disconnect.is_set()):
        pass
    sleep(message_drain_time)
    csv_row = '{time},DISCONNECT\n'.format(time=str(time()))
    with open(sub_client_log_file, 'a') as sclf:
        sclf.write(csv_row)
    client.disconnect()

def _pub_client(client_id, pub_interval, host, port, n_topics, disconnect):
    '''
    Start a parallel publisher with no persistent connection to broker

    Args:
        client_id (int):
            unique integer to identify client
        pub_interval (float):
            time in seconds to wait between publishing on all topics
        host (str):
            URL or IP address of broker
        port (int):
            MQTT port of broker
        n_topics (int):
            number of test topics to which to subscribe
        disconnect (Disconnect):
            object with a .is_set() method that indicates whether
            disconnect has been signaled; this is how the main process
            communicates with client threads to tell them to stop
            publishing
    '''

    name = client_id.split('_')[0]
    pub_client_log_file = '{name}/pub/{client_id}.csv'.format(
        name=name,
        client_id=client_id
    )
    while(not disconnect.is_set()):
        with open(pub_client_log_file, 'a') as pclf:
            for topic in range(n_topics):
                topic = 'test/{topic}'.format(topic=topic)
                message = '{message}|{client_id}'.format(
                    message=time(),
                    client_id=client_id.split('_')[-1]
                )
                publish.single(
                    topic,
                    payload=message,
                    hostname=host,
                    port=port
                )
                csv_row = '{time},{topic},{message}\n'.format(
                    time=time(),
                    topic=topic,
                    message=message
                )
                pclf.write(csv_row)
        sleep(pub_interval)

def load_test(
    name,
    n_sub_clients,
    n_pub_clients,
    host,
    port,
    keepalive,
    test_time,
    sub_connect_rate,
    pub_message_rate,
    n_topics,
    clean_session,
    subscriber_headstart_time,
    message_drain_time):

    '''
    Run a controlled load test on an MQTT broker

    The load test brings up n_sub_clients number of subscribers that
    subscribe to all n_topics number of test topics. 
    These subscribers come up at a rate of
    sub_connect_rate. After 5 seconds, the load test brings up
    n_pub_clients number of publishers all at once. Together, these
    publishers publish at an average rate of pub_message_rate. At the end
    of test_time, the load test disconnects all subscribers and stops
    all publishers from publish further messages then waits
    for 5 seconds.

    Args:
        name (str):
            name of the test, which will be used in recording message
            publish and subscribe results to files on disk
        n_sub_clients (int):
            number of clients that act as subscribers
        n_pub_clients (int):
            number of clients that act as persistent publishers
        host (str):
            URL or IP address of the MQTT broker to load test
        port (int):
            port of the MQTT broker to load test
        keepalive (int):
            the number of seconds between client keepalive messages
            for subscribed clients (publisher clients DO NOT maintain
            a persistent connection)
        test_time (int):
            time in seconds from the beginning of the test to the end;
            at the end of test_time, all clients will be terminated
        sub_connect_rate (float):
            number of subscribers per second to connect (on average) until
            the number of subscribers matches n_sub_clients
        pub_message_rate (float):
            number of messages per second (on average) that publishers
            will publish ACROSS ALL PUBLISHERS
        n_topics (int):
            number of test topics to which to publish
        messages ([str]):
            list of messages which publishers will publish; these messages are
            sampled at random for persistent publishers
        clean_session (bool):
            the clean session flag to use when subscribers connect to the 
            broker; if True, the client will request that the broker
            discard any previous session information (MQTT maintains sticky
            sessions) 
        subscriber_headstart_time (float):
            time in seconds to start subscriber clients BEFORE starting
            publisher clients; note that depending on your sub_connect_rate,
            not all subscriber clients may have started before your
            publisher clients start
        message_drain_time (float):
            time in seconds to maintain connection and process subscribed
            topics AFTER disconnect has been set; if your broker-to-client
            latency is greater than message_drain_time, you may miss messages
            at the end of the test because the subscriber clients disconnect
            before the messages arrive, so choose a value that is greater
            than your expected network latency

    '''

    if os.path.isdir(name):
        raise
    if not os.path.isdir(name):
        os.makedirs(name)
    sub_dir = '{name}/sub/'.format(name=name)
    if not os.path.isdir(sub_dir):
        os.makedirs(sub_dir)
    pub_dir = '{name}/pub/'.format(name=name)
    if not os.path.isdir(pub_dir):
        os.makedirs(pub_dir)
    test_start_time = time()
    class Disconnect(object):
        def __init__(self):
            self._set = False
        def set(self):
            self._set = True
        def is_set(self):
            return self._set
    disconnect = Disconnect()
    for sub_id in range(n_sub_clients):
        start_delay = float(sub_id)/sub_connect_rate
        client_id = '{name}_sub_{sub_id}'.format(
                    name=name,
                    sub_id=str(sub_id)
       )
        sub_thread = threading.Thread(
            target = _sub_client,
            args= (
                client_id,
                start_delay,
                host,
                port,
                keepalive,
                clean_session,
                n_topics,
                disconnect,
                message_drain_time,
            )
        )
        sub_thread.setDaemon(True)
        sub_thread.start()
    sleep(subscriber_headstart_time)
    pub_interval = n_pub_clients/pub_message_rate
    for pub_id in range(n_pub_clients):
        client_id = '{name}_pub_{pub_id}'.format(
                    name=name,
                    pub_id=str(pub_id)
        )
        pub_thread = threading.Thread(
            target = _pub_client,
            args= (
                client_id,
                pub_interval,
                host,
                port,
                n_topics,
                disconnect,
            )
        )
        pub_thread.setDaemon(True)
        pub_thread.start()
    # block until test_time has elapsed
    # daemon clients run in the background
    while(test_time >= time() - test_start_time):
        pass
    disconnect.set()
    sleep(2*message_drain_time)
   
def aggregate_test_data(name):
    '''
    After a load test, aggregate data into dataframes for processing

    This function should be able to process data written during load_test.

    Args:
        name (str):
            path of base folder containing load test data; this folder
            should contain two subfolders: ./pub/ and ./sub/. Each of these
            subfolders should contain message data for each publisher and
            subscriber client respectively in .csv format. Subscriber .csv
            files can contain CONNECT and DISCONNECT messages in addition
            to normal message subscription data.
    Returns:
        message_data (pd.DataFrame):
            dataframe of all messages from publishers and subscribers,
            excluding CONNECT and DISCONNECT messages
        connect_data (pd.DataFrame):
            dataframe of all CONNECT and DISCONNECT messages from subscribers
    '''
    test_dirs = ['{name}/{sub}/'.format(
        name=name,sub=sub) for sub in ['pub', 'sub']]
    for test_dir in test_dirs:
        if not os.path.isdir(test_dir):
            raise
    message_data = pd.DataFrame([])
    for publisher in ['{name}/pub/{csv}'.format(name=name,csv=csv)
            for csv in os.listdir('{name}/pub/'.format(name=name))
            if csv.endswith('.csv')]:
        df = pd.read_csv(publisher, header=None)
        df['type'] = 'pub'
        df['client_id'] = publisher.split('_')[-1].replace('.csv', '')
        df.columns = ['time', 'topic', 'message', 'type', 'client_id']
        message_data = message_data.append(df)
    connect_data = pd.DataFrame([])
    for subscriber in ['{name}/sub/{csv}'.format(name=name,csv=csv)
            for csv in os.listdir('{name}/sub/'.format(name=name))
            if csv.endswith('.csv')]:
        client_id = subscriber.split('_')[-1].replace('.csv', '')
        df = pd.read_csv(subscriber, header=None, names=['time', 'topic', 'message', 'client_id', 'type'])
        df['client_id'] = client_id
        df['type'] = 'sub'
        connect = df[df['topic'].isin(['CONNECT', 'DISCONNECT'])]
        connect = connect[['time', 'topic', 'client_id']]
        df = df[~df['topic'].isin(['CONNECT', 'DISCONNECT'])]
        message_data = message_data.append(df, sort=True)
        connect_data = connect_data.append(connect, sort=True)
    message_data['client_id'] = message_data['client_id'].astype(int)
    connect_data['client_id'] = connect_data['client_id'].astype(int)
    message_data = message_data.sort_values('time')
    connect_data = connect_data.sort_values('time')
    return (message_data, connect_data)

def message_received_statistics(message_data, connect_data):
    '''
    Compute dataframe of message reception statistics for all messages

    Args:
        message_data (pd.DataFrame):
            dataframe of all messages from publishers and subscribers,
            excluding CONNECT and DISCONNECT messages
        connect_data (pd.DataFrame):
            dataframe of all CONNECT and DISCONNECT messages from subscribers
    Returns:
        stats_data (pd.DataFrame):
            dataframe of all subscriber clients showing how many messages
            could be received and how many messages were actually received
    '''
    stats_data = pd.DataFrame([])
    for client_id in set(connect_data['client_id']):
        connect_time = min(connect_data[
            (connect_data['topic'] == 'CONNECT') &
            (connect_data['client_id'] == client_id)
        ]['time'])
        disconnect_time = max(connect_data[
            (connect_data['topic'] == 'DISCONNECT') &
            (connect_data['client_id'] == client_id)
        ]['time'])
        n_messages_receivable = len(message_data[
            (message_data['type'] == 'pub') &
            (message_data['time'] >= connect_time) &
            (message_data['time'] <= disconnect_time)
        ])
        n_messages_received = len(message_data[
            (message_data['type'] == 'sub') &
            (message_data['client_id'] == client_id)
        ])
        data_row = [client_id, n_messages_receivable, n_messages_received]
        stats_data = stats_data.append([data_row])
    stats_data.columns = ['client_id', 'n_receivable', 'n_received']
    stats_data['percent_received'] = stats_data['n_received']/stats_data['n_receivable']
    return stats_data

def message_latency_statistics(message_data, connect_data):
    '''
    Compute dataframe of message latency statistics for all messages

    Args:
        message_data (pd.DataFrame):
            dataframe of all messages from publishers and subscribers,
            excluding CONNECT and DISCONNECT messages
        connect_data (pd.DataFrame):
            dataframe of all CONNECT and DISCONNECT messages from subscribers
    Returns:
        lat_data (pd.DataFrame):
            dataframe of latency between message publish and message subscribe
            for all messages for all subscribers; messages that never arrived
            have 'message' field equal to MISSED, and the 'time' field
            indicates when that message was published
    '''
    latency_data = pd.DataFrame([])
    _message_data = message_data.copy()
    # compute subscriber message latencies from message payload
    _message_data['message_time'], _message_data['pub_client'] \
        = _message_data['message'].str.split('|').str
    _message_data['message_time'] = _message_data['message_time'].astype(float)
    _message_data['pub_client'] = _message_data['pub_client'].astype(int)
    _message_data['latency'] = _message_data['time'] - _message_data['message_time']
    latency_data = latency_data.append(_message_data[_message_data['type'] == 'sub'])
    # find unreceived messages and mark as infinite latency
    ## find pub messages after connect then find the disjoint between
    ## received messages and receivable messages
    for client_id in set(connect_data['client_id']):
        connect_time = min(connect_data[
            (connect_data['topic'] == 'CONNECT') &
            (connect_data['client_id'] == client_id)
        ]['time'])
        disconnect_time = max(connect_data[
            (connect_data['topic'] == 'DISCONNECT') &
            (connect_data['client_id'] == client_id)
        ]['time'])
        missed = set(
            _message_data[
                (_message_data['type'] == 'pub') &
                (_message_data['time'] >= connect_time) &
                (_message_data['time'] <= disconnect_time)
            ]['message']).symmetric_difference(
                _message_data[
                    (_message_data['type'] == 'sub') &
                    (_message_data['client_id'] == client_id)
                ]['message']
            )
        missed_origin = _message_data[
            (_message_data['type'] == 'pub') &
            (_message_data['message'].isin(missed))
        ]
        missed_origin['client_id'] = int(client_id)
        missed_origin['latency'] = np.inf
        missed_origin['type'] = 'sub' 
        missed_origin['message'] = 'MISSED' 
        latency_data = latency_data.append(missed_origin)
    return latency_data

def plot_message_pattern(message_data, connect_data, client_types, ax=None):
    '''
    Plot all message publish and subscribe events for all clients

    Args:
        message_data (pd.DataFrame):
            dataframe of all messages from publishers and subscribers,
            excluding CONNECT and DISCONNECT messages
        connect_data (pd.DataFrame):
            dataframe of all CONNECT and DISCONNECT messages from subscribers
        ax (matplotlib.axes.Axes):
            axes plot on which to plot message pattern; if ax is None, then
            a new set of axes is created using plt.subplots
    '''
    if not 'sub' in client_types and not 'pub' in client_types:
        return
    _message_data = message_data.copy()
    _connect_data = connect_data.copy()
    received_stats_data = message_received_statistics(
        _message_data, _connect_data)
    # calculate all times relative to first subscriber connect event
    min_time = min(connect_data['time'])
    _connect_data['time'] = _connect_data['time'] - min_time
    _message_data['time'] = _message_data['time'] - min_time 
    if ax is None:
        _, ax = plt.subplots()
    n_clients = 0
    if 'sub' in client_types:
        # subscriber connection events are big yellow circles
        _connect_data.plot.scatter(
                x='time', y='client_id', ax=ax, color=COLORS['connection'], s=16)
        # subscriber received message events are small red circles
        sub_data = _message_data[
            _message_data['type'] == 'sub']
        n_clients = max(sub_data['client_id'])
        sub_data.plot.scatter(
                x='time',y='client_id', ax=ax, color=COLORS['sub'], s=1)
        mean_received = received_stats_data.mean()
        std_received = received_stats_data.std()
        sub_text_data = '''Subscriber Messages
{n_messages} total message(s)
{rate} per second
{avg_received}±{std_received} received
of {avg_receivable}±{std_receivable} receivable (avg per client)
{avg_perc_received}±{std_perc_received} fraction received'''.format(
            n_messages=str(len(sub_data)),
            rate=round(len(sub_data)/max(sub_data['time']), 2),
            avg_received=round(mean_received['n_received'], 2),
            std_received=round(std_received['n_received'], 2),
            avg_receivable=round(mean_received['n_receivable'], 2),
            std_receivable=round(std_received['n_receivable'], 2),
            avg_perc_received=round(
                mean_received['n_received']/mean_received['n_receivable'], 2),
            std_perc_received=round(
                std_received['n_received']/mean_received['n_receivable'], 2)
        )
        ax.text(
            1.1*max(sub_data['time']),
            0.75*n_clients,
            sub_text_data,
            color=COLORS['sub']
        )
        connects = _connect_data[_connect_data['topic'] == 'CONNECT']
        conn_text_data = '''Subscriber Connections
{n_connects} total connect(s)
{n_disconnects} total disconnect(s)
{avg_connects}±{std_connects} avg connects 
'''.format(
            n_connects=str(len(connects)),
            n_disconnects=str(
                len(
                    _connect_data[
                        _connect_data['topic'] == 'DISCONNECT'])),
            avg_connects=connects.groupby('client_id').count()['topic'].mean(),
            std_connects=connects.groupby('client_id').count()['topic'].std()
        )
        ax.text(
            1.1*max(sub_data['time']),
            0.3*n_clients,
            conn_text_data,
            color=COLORS['connection']
        )
    if 'pub' in  client_types:
        # publisher published message events are medium-size blue circles
        pub_data = _message_data[
            _message_data['type'] == 'pub']
        max_pub_clients = max(pub_data['client_id'])
        # if combining pub and sub, always show the total number of clients
        if max_pub_clients > n_clients:
            n_clients = max_pub_clients
        pub_data.plot.scatter(
                x='time',y='client_id', ax=ax, color=COLORS['pub'], s=4)
        pub_count = pub_data.groupby('client_id').count()
        pub_mean = pub_count['message'].mean()
        pub_std = pub_count['message'].std()
        text_data = '''Publisher Messages
{n_messages} total message(s)
{rate} per second
{avg_pub}±{std_pub} (avg per client)
'''.format(
            n_messages=str(len(pub_data)),
            rate=round(len(pub_data)/max(pub_data['time']), 2),
            avg_pub=round(pub_mean, 2),
            std_pub=round(pub_std, 2)
        )
        y_offset_factor = 0.75
        if 'sub' in client_types:
            y_offset_factor = 0.5
        ax.text(
            1.1*max(pub_data['time']),
            y_offset_factor*n_clients,
            text_data,
            color=COLORS['pub']
        )
    n_yticks = 10
    if n_clients < n_yticks:
        n_yticks = n_clients
    ax.set_yticks(
        np.round(
            np.append(
                np.linspace(0, n_clients, 10),
                n_clients
            )
        )
    )

def plot_missed_pattern(latency_data, connect_data, ax=None):
    '''
    Plot all missed messages for all clients

    Args:
        lat_data (pd.DataFrame):
            dataframe of latency between message publish and message subscribe
            for all messages for all subscribers; messages that never arrived
            have 'message' field equal to MISSED, and the 'time' field
            indicates when that message was published
        connect_data (pd.DataFrame):
            dataframe of all CONNECT and DISCONNECT messages from subscribers
        ax (matplotlib.axes.Axes):
            axes plot on which to plot missed message pattern; 
            if ax is None, then
            a new set of axes is created using plt.subplots
    '''

    _latency_data = latency_data.copy()
    min_time = min(connect_data['time'])
    n_clients = max(connect_data['client_id']) + 1
    _latency_data['time'] = _latency_data['time'] - min_time
    max_time = max(_latency_data['time'])
    missed_data = _latency_data[_latency_data['message'] == 'MISSED']
    if not missed_data.empty:
        if ax is None:
            _, ax = plt.subplots()
        missed_data.plot.scatter(
            x='time',
            y='client_id',
            color=COLORS['missed'], 
            s=24,
            ax=ax
        )
        avg_time_frac = missed_data['time'].mean()/max_time
        std_time_frac = missed_data['time'].std()/max_time
        text_data = '''Missed Messages 
{n_missed} total message(s)
{avg_time_frac}±{std_time_frac} avg frac time elapsed
'''.format(
            n_missed=str(len(missed_data)),
            avg_time_frac=round(avg_time_frac, 2),
            std_time_frac=round(std_time_frac, 2)
        )
        ax.text(
            1.1*max(_latency_data['time']),
            0.1*n_clients,
            text_data,
            color=COLORS['missed']
        )
def plot_latency_pattern(latency_data, connect_data, ax=None):
    '''
    Plot latencies between publish and subscribe for all messages for all clients

    Args:
        lat_data (pd.DataFrame):
            dataframe of latency between message publish and message subscribe
            for all messages for all subscribers; messages that never arrived
            have 'message' field equal to MISSED, and the 'time' field
            indicates when that message was published
        connect_data (pd.DataFrame):
            dataframe of all CONNECT and DISCONNECT messages from subscribers
        ax (matplotlib.axes.Axes):
            axes plot on which to plot latency pattern; 
            if ax is None, then
            a new set of axes is created using plt.subplots
    '''
    _latency_data= latency_data.copy()
    _connect_data = connect_data.copy()
    min_time = min(connect_data['time'])
    _connect_data['time'] = _connect_data['time'] - min_time
    _latency_data['time'] = _latency_data['time'] - min_time 
    if ax is None:
        _, ax = plt.subplots()
    _latency_data.plot.scatter(x='time', y='latency', s=1, color=COLORS['latency'], ax=ax)
    received_latency = _latency_data.replace([np.inf], np.nan)
    text_data = '''Latency Statistics
max  {max_lat}
avg   {avg_lat}±{std_lat}
min   {min_lat}
'''.format(
        max_lat = round(max(received_latency['latency']), 2),
        avg_lat = round(received_latency['latency'].mean(), 2),
        std_lat = round(received_latency['latency'].std(), 2),
        min_lat = round(min(received_latency['latency']), 2)
    )
    ax.text(
        1.1*max(_latency_data['time']),
        0.75*max(received_latency['latency']),
        text_data,
        color=COLORS['latency']
    )

def add_test_details_to_plot(details_text, ax0):
    '''
    Add test details text to top left corner of test plots

    Args:
        details_text (str):
            test details
        ax0 (matplotlib.axis.Axes):
            axes of topleftmost subplot
    '''
    _, max_x = ax0.get_xlim()
    _, max_y = ax0.get_ylim()
    ax0.text(-0.12*max_x, 1.05*max_y, details_text)
