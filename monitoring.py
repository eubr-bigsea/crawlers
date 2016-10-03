import datetime
from zabbix import pyzabbix_sender


class Monitoring:
    def send(self, host, item, number_records):
        ''' Send the number of records to the Zabbix monitoring system '''
        pyzabbix_sender.send(host, item, number_records, pyzabbix_sender.get_zabbix_server())
        print "Execution completed with ", number_records, "records", str(datetime.datetime.now())

