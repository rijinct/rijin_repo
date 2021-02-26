import sys
import datetime
import threading

from connector import SparkConnector
from specification import SourceSpecification
from specification import TargetSpecification
from reader import ReaderAndViewCreator
from reader import ReaderAndViewCreatorThread
from writer import DataWriter


class Aggregator:
    def __init__(self, source_spec_list: [SourceSpecification],
                 target_spec: TargetSpecification,
                 addnl_source_spec_list: [SourceSpecification]):
        self._source_spec_list = source_spec_list
        self._target_spec = target_spec
        self._addnl_source_spec_list = addnl_source_spec_list
        self._one_to_many = True if len(
            self._target_spec.yaml_spec) > 1 else False
        self._logger = SparkConnector.get_logger(__name__)

    def start_loading(self):
        srcs = [self._source_spec_list, self._addnl_source_spec_list]
        output = tuple([] for i in range(len(srcs)))
        threads = []
        for i in range(len(srcs)):
            t = threading.Thread(target=lambda o, s: o.append(
                self._read_sources(s)), args=(output[i], srcs[i]))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        self._write_data(output[0][0], output[1][0])

    def _read_sources(self, source_spec_list):
        threads = []
        reader_objs = []
        start = datetime.datetime.now()
        for source_spec in source_spec_list:
            self._logger.info('Reading source spec {}'.format(
                source_spec.get_table_name()))
            reader = ReaderAndViewCreator(source_spec, self._one_to_many)
            reader_thread = ReaderAndViewCreatorThread(reader)
            reader_thread.start()
            reader_objs.append(reader)
            threads.append(reader_thread)
        try:
            for job in threads:
                job.join()
        except Exception as e:
            self._logger.info(
                "Error occured while reading, Reason: {}".format(e))
            sys.exit(1)
        end = datetime.datetime.now()
        self._logger.info("Total Reading Time: {} s".format(end - start))
        return reader_objs

    def _write_data(self, reader_objs, addnl_reader_objs):
        start = datetime.datetime.now()
        writer = DataWriter(self._target_spec, reader_objs, addnl_reader_objs)
        writer.write()
        end = datetime.datetime.now()
        self._logger.info("Total Writing Time: {} s".format(end - start))
