import ast
import datetime
import json
import time
from urllib import parse

import requests

from commands_executors.hooks.platform_hooks.platform_hooks import ApiHook
from advm_etl_utils import wrap_if_not_list


class LogsAPIReportHook(ApiHook):
    """Ex. LogsAPIReportInputProcessor"""

    name = 'LogsAPIReportHook'

    HOST = 'https://api-metrika.yandex.ru'
    DATE_FORMAT = '%Y-%m-%d'

    def __init__(self, path_credentials: str, arguments: dict):
        super().__init__(path_credentials, arguments)
        self.fields = wrap_if_not_list(arguments['fields'])
        self.start_date_str = str(self.start_date)
        self.end_date_str = str(self.end_date)
        self.source = arguments['@source']
        with open(self.path_credentials, 'r') as f:
            data = json.loads(f.read())
            self.token = data['token']
            if arguments.get('@accountIds'):
                self.counter_id = arguments['@accountIds']
            else:
                self.counter_id = data['counterId']
        print(self.counter_id)

    def get_headers(self):
        return {"Authorization": "OAuth " + self.token}

    def get_retry_errors(self):
        return [".*504 Gateway Time-out.*", ".*Сервис временно недоступен", ".*failed"]

    def get_estimation(self):
        url_params = parse.urlencode([
            ('date1', self.start_date_str),
            ('date2', self.end_date_str),
            ('source', self.source),
            ('fields', ','.join(self.fields))]
        )
        url = '{host}/management/v1/counter/{counter_id}/logrequests/evaluate?'.format(
            host=self.HOST, counter_id=self.counter_id) + url_params
        r = requests.get(url, headers=self.get_headers())
        if r.status_code == 200:
            return json.loads(r.text)['log_request_evaluation']
        else:
            raise ValueError(r.text)

    def get_api_requests(self):
        api_requests = []
        estimation = self.get_estimation()
        if estimation['possible']:
            api_request = {
                "date1_str": self.start_date_str,
                "date2_str": self.end_date_str,
                "status": 'new'
            }
            api_requests.append(api_request)
        else:
            start_date = datetime.datetime.strptime(
                self.start_date_str,
                self.DATE_FORMAT
            )

            end_date = datetime.datetime.strptime(
                self.end_date_str,
                self.DATE_FORMAT
            )
            days = (end_date - start_date).days
            num_requests = int(days / estimation['max_possible_day_quantity']) + 1
            days_in_period = int(days / num_requests) + 1
            for i in range(num_requests):
                date1 = start_date + datetime.timedelta(i * days_in_period)
                date2 = min(
                    end_date,
                    start_date + datetime.timedelta((i + 1) * days_in_period - 1)
                )
                api_request = {
                    "date1_str": date1.strftime(self.DATE_FORMAT),
                    "date2_str": date2.strftime(self.DATE_FORMAT),
                    "status": 'new'
                }
                api_requests.append(api_request)
        return api_requests

    def create_task(self, api_request):
        url_params = parse.urlencode([
            ('date1', api_request['date1_str']),
            ('date2', api_request['date2_str']),
            ('source', self.source),
            ('fields', ','.join(sorted(self.fields, key=lambda s: s.lower())))]
        )
        url = '{host}/management/v1/counter/{counter_id}/logrequests?'.format(
            host=self.HOST, counter_id=self.counter_id) + url_params
        r = requests.post(url, headers=self.get_headers())
        if r.status_code == 200:
            response = json.loads(r.text)['log_request']
            api_request['status'] = response['status']
            api_request['request_id'] = response['request_id']
            return response
        else:
            raise ValueError(r.text)

    def get_report_info(self, request_id):
        url = '{host}/management/v1/counter/{counter_id}/logrequest/{request_id}'.format(
            request_id=request_id, counter_id=self.counter_id, host=self.HOST)
        r = requests.get(url, headers=self.get_headers())
        if r.status_code == 200:
            return json.loads(r.text)['log_request']
        else:
            raise ValueError(r.text)

    def update_status(self, api_request):
        report_info = self.get_report_info(api_request['request_id'])
        status = report_info['status']
        api_request['status'] = status
        if status == 'processed':
            size = len(report_info['parts'])
            api_request['size'] = size
        return api_request

    def delete_report(self, request_id):
        url = '{host}/management/v1/counter/{counter_id}/logrequest/{request_id}/clean'.format(
            host=self.HOST, counter_id=self.counter_id, request_id=request_id)
        r = requests.post(url, headers=self.get_headers())
        if r.status_code != 200:
            raise ValueError(r.text)

    def download_data(self, api_request, part):
        url = '{host}/management/v1/counter/{counter_id}/logrequest/{request_id}/part' \
              '/{part}/download'.format(
                host=self.HOST, counter_id=self.counter_id, request_id=api_request['request_id'],
                part=part)
        r = requests.get(url, headers=self.get_headers())
        if r.status_code != 200:
            raise ValueError(r.text)
        return r.text

    @staticmethod
    def list_to_field_dict(lst, fields_no_prefix):
        ret = {}
        processed_fields = []
        dct = dict(zip(fields_no_prefix, lst))
        for group in ["goals"]:
            matched_fields = [x for x in fields_no_prefix if x.startswith(group)]
            if len(matched_fields) > 0:
                processed_fields += matched_fields
                field_lists = [ast.literal_eval(dct[f]) for f in matched_fields]
                ret[group] = [dict(zip(matched_fields, x)) for x in zip(*field_lists)]
        for f in fields_no_prefix:
            if f not in processed_fields:
                ret[f] = dct[f]
        return ret

    def clean_reports(self):
        url = '{host}/management/v1/counter/{counter_id}/logrequests'.format(
            counter_id=self.counter_id, host=self.HOST)
        r = requests.get(url, headers=self.get_headers())
        if r.status_code == 200:
            reqs = json.loads(r.text)['requests']
            for req in reqs:
                if req['status'] == 'processed':
                    print("Cleaning LogsAPI request #{}".format(req))
                    self.delete_report(req['request_id'])
        else:
            raise ValueError(r.text)

    def __iter__(self):
        api_requests = self.get_api_requests()
        print("Downloading LogsAPI in {} request(s)".format(len(api_requests)))
        for api_request in api_requests:
            self.create_task(api_request)
            delay = 20
            while api_request['status'] != 'processed':
                if api_request['status'] == 'processing_failed':
                    raise ValueError("Logs.API status - processing failed")
                time.sleep(delay)
                api_request = self.update_status(api_request)
            print("Logs API request ready")
            for part in range(int(api_request['size'])):
                print("Downloading part {}".format(part))
                splitted_text = self.download_data(api_request, part).replace(r"\'", "'").split('\n')
                print("End downloading")
                header = splitted_text[0]
                fields_no_prefix = [x.split(":")[2] for x in header.split('\t')]
                for line in splitted_text[1:]:
                    lst = line.split('\t')
                    if len(lst) == len(fields_no_prefix):
                        yield self.list_to_field_dict(lst, fields_no_prefix)
            self.delete_report(api_request['request_id'])

    def get_client_id(self):
        return self.counter_id
