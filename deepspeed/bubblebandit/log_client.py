import grpc
import log_service_pb2_grpc
import log_service_pb2


class LogClient:
    def __init__(self):
        self.chan = grpc.insecure_channel('localhost:50051')
        self.stub = log_service_pb2_grpc.LogServerStub(self.chan)

    def test(self):
        pid = 1
        msg = 'abc'
        log_entry = log_service_pb2.LogEntry(pid=pid, msg=msg)
        self.stub.WriteLog(log_entry)

    def write_log(self, pid: int, msg: str) -> None:
        log_entry = log_service_pb2.LogEntry(pid=pid, msg=msg)
        self.stub.WriteLog(log_entry)


def write_log_test():
    log_client = LogClient()
    log_client.test()
    log_client.write_log(2, 'edf')


if __name__ == '__main__':
    write_log_test()
