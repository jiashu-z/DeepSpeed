import grpc
import deepspeed.bubblebandit.log_service_pb2_grpc as log_service_pb2_grpc
import deepspeed.bubblebandit.log_service_pb2 as log_service_pb2


class LogClient:
    def __init__(self):
        self.chan = grpc.insecure_channel('localhost:50051')
        self.stub = log_service_pb2_grpc.LogServerStub(self.chan)

    def test(self):
        pid = 1
        ts = 2
        msg = 'abc'
        log_entry = log_service_pb2.LogEntry(pid=pid, ts=ts, msg=msg)
        self.stub.WriteLog(log_entry)

    def write_log(self, pid: int, ts: int, msg: str) -> None:
        log_entry = log_service_pb2.LogEntry(pid=pid, ts=ts, msg=msg)
        self.stub.WriteLog(log_entry)


log_client = LogClient()


def write_log_test():
    log_client = LogClient()
    log_client.test()
    log_client.write_log(2, 3, 'edf')


if __name__ == '__main__':
    write_log_test()
