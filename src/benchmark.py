from datetime import datetime as dt, timedelta as td
import sys
import subprocess
import pandas as pd
import os
from time import sleep
from dotenv import load_dotenv 


def run_apache_bench(url, env_vars, output_file):
    # avg machine should be able to handle about 64K TCP connections
    # client_limit = 6000

    clients = env_vars['clients']
    requests = clients
    cookie = '{}={}'.format(env_vars['cookie-name'], env_vars['cookie-value'])
    if env_vars['use-cookie']:
        cmd = f'ab -c {clients} -n {requests} -C {cookie} {url} | tee {output_file}'
    else:
        cmd = f'ab -c {clients} -n {requests} {url} | tee {output_file}'
    print('cmd: ', cmd)
    os.system(cmd)


class ApacheBenchParser():

    columns = [
        'Time',
        'Complete requests',
        'Failed requests',
        'Requests per second',
        '50%',
        '90%',
        '95%',
        '99%',
        '100%'
    ]
    column_dict = {
        'Time': {
            'type': str
        },
        'Complete requests': {
            'type': int
        },
        'Failed requests': {
            'type': int
        },
        'Requests per second': {
            'type': float
        },
        '50%': {
            'type': float
        },
        '90%': {
            'type': float
        },
        '95%': {
            'type': float
        },
        '99%': {
            'type': float
        },
        '100%': {
            'type': float
        }
    }
    bench_metrics = {}

    def __init__(self, output_file):
        self.output_file = output_file

    def clean_entry(self, val):
        remove_strs = [
            'seconds',
            '[#/sec]',
            '(mean)',
            '[ms]', 
            '(mean, across all concurrent requests)',
            '[Kbytes/',
            'sec]',
            'received',
            'bytes',
            '(longest request)'
        ]

        for remove_str in remove_strs:
            val = val.replace(remove_str, '')

        return val.strip()
    

    def make_metrics_numerical(self):

        for column in self.columns:
            if column == 'Time':
                continue
            self.bench_metrics[column] = float(self.bench_metrics[column])


    def manage_parsing(self):

        skip_phrases = ['\n', '']
        has_started = False

        batches = [
            {
                'start_phrase': 'Server Software',
                'end_phrase': 'Transfer rate',
                'split_key': ':',
                'key_appendix': ''
            },
            {
                'start_phrase': '50%',
                'end_phrase': '(longest request)',
                'split_key': '%',
                'key_appendix': '%'
            }
        ]

        with open(self.output_file) as f:
            for line in f:

                if batches == []:
                    break

                if not has_started:
                    if batches[0]['start_phrase'] in line:
                        has_started = True
                    else:
                        continue

                try:
                    split_key = batches[0]['split_key']
                    key = self.clean_entry(line.split(split_key)[0] + batches[0]['key_appendix'])
                    val = self.clean_entry(split_key.join(line.split(split_key)[1:]))
                except Exception as e:
                    continue

                if key in skip_phrases:
                    continue

                self.bench_metrics[key] = val

                if batches[0]['end_phrase'] in line:
                    batches.pop(0)
                    has_started = False
                
            print('self.bench_metrics: ', self.bench_metrics)
            print('self.bench_metrics keys: \n', self.bench_metrics.keys())

        self.make_metrics_numerical()
        
    def create_row_list(self, dt_now):

        if self.bench_metrics == {}:
            self.bench_metrics['Complete requests'] = 0
            self.bench_metrics['Failed requests'] = 0
            self.bench_metrics['Requests per second'] = 0

        for k,v in self.bench_metrics.items():
            self.bench_metrics[k] = [v]

        dt_now_str = dt_now.strftime('%Y-%m-%d %H:%M:%S')

        row_list = []

        for column in self.columns:
            if column == 'Time':
                row_list.append(dt_now_str)
            else:
                row_list.append(self.bench_metrics[column][0])

        return row_list
    

    def create_last_row(self, df_all):

        row_list = []

        for column in self.columns:
            if column == 'Time':
                row_list.append('Total')
            elif column in ['Complete requests', 'Failed requests']:
                row_list.append(df_all[column].sum())
            else:
                row_list.append(df_all[column].mean())

        return row_list


    def summarize_output_file(self, dt_now, summary_file):

        if not os.path.exists(summary_file):
            
            with open(summary_file, 'w') as f:
                f.write(','.join(list(self.columns)))

        try:
            df_all = pd.read_csv(summary_file)

            df_all.loc[len(df_all.index)-1] = self.create_row_list(
                dt_now
            )

            if not df_all.empty:
                df_all.loc[len(df_all.index)] = self.create_last_row(
                    df_all
                )

            df_all.to_csv(summary_file, index=None)
        except Exception as e:
            raise
            print('cant read csv: ', e)


# 
load_dotenv()

env_vars = {
    'clients': int(os.getenv('CLIENT_CONCURRENCY')),
    'cookie-name': os.getenv('COOKIE_NAME'),
    'cookie-value': os.getenv('COOKIE_VALUE'),
    'domain': os.getenv('DOMAIN'),
    'use-cookie': os.getenv('USE_COOKIE'),
    'minutes_to_run': int(os.getenv('MINUTES_TO_RUN'))
}

start_time = dt.utcnow()
end_time = start_time + td(minutes=env_vars['minutes_to_run'])

while True:

    dt_now = dt.utcnow()

    data_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../data')

    url = 'https://{}/'.format(env_vars['domain'])

    apache_output_file = '{}/output_{}_cookie{}.txt'.format(
        data_dir,
        env_vars['domain'],
        env_vars['use-cookie'],
    )

    summary_file = '{}/summary_{}_cookie{}.csv'.format(
        data_dir,
        env_vars['domain'],
        env_vars['use-cookie'],
    )

    run_apache_bench(url, env_vars, apache_output_file)
    ab_parser = ApacheBenchParser(apache_output_file)
    ab_parser.manage_parsing()
    ab_parser.summarize_output_file(dt_now, summary_file)

    if end_time < dt.utcnow():
        break

    # break