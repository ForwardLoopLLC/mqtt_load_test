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


def _sub_client(client_id, start_delay, host, port, keepalive, clean_session, n_topics, disconnect):
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
    clean_session=True):

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
            sessions so it works in sit

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
            )
        )
        sub_thread.setDaemon(True)
        sub_thread.start()
    sleep(5)
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
    sleep(10)
   
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
                (_message_data['time'] >= connect_time)
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
        missed_origin.loc[:, 'client_id'] = int(client_id)
        missed_origin.loc[:, 'latency'] = np.inf
        missed_origin.loc[:, 'type'] = 'sub' 
        missed_origin.loc[:, 'message'] = 'MISSED' 
        latency_data = latency_data.append(missed_origin)
    return latency_data

def plot_message_pattern(message_data, connect_data, ax=None):
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
    _message_data = message_data.copy()
    _connect_data = connect_data.copy()
    # calculate all times relative to first subscriber connect event
    min_time = min(connect_data['time'])
    _connect_data['time'] = _connect_data['time'] - min_time
    _message_data['time'] = _message_data['time'] - min_time 
    if ax is None:
        _, ax = plt.subplots()
    # subscriber connection events are big yellow circles
    _connect_data.plot.scatter(
            x='time', y='client_id', ax=ax, color='y', s=16)
    # subscriber received message events are small red circles
    _message_data[
        _message_data['type'] == 'sub'].plot.scatter(
            x='time',y='client_id', ax=ax, color='r', s=1)
    # publisher published message events are medium-size blue circles
    _message_data[
        _message_data['type'] == 'pub'].plot.scatter(
            x='time',y='client_id', ax=ax, color='b', s=4)

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
    _latency_data['time'] = _latency_data['time'] - min_time
    missed_data = _latency_data[_latency_data['message'] == 'MISSED']
    if not missed_data.empty:
        if ax is None:
            _, ax = plt.subplots()
        missed_data.plot.scatter(x='time', y='client_id', color='k', s=24, ax=ax)

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
    _latency_data.plot.scatter(x='time', y='latency', s=1, ax=ax)
