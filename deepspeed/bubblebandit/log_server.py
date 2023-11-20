import deepspeed.bubblebandit.log_service_pb2_grpc as log_service_pb2_grpc
import deepspeed.bubblebandit.log_service_pb2 as log_service_pb2
import grpc
from concurrent import futures
import os
from pathlib import Path


class LogServicer(log_service_pb2_grpc.LogServerServicer):
    def __init__(self, log_path=None):
        self._log_path = log_path
        dirname = os.path.dirname(self._log_path)
        Path(dirname).mkdir(parents=True, exist_ok=True)
        if self._log_path is not None:
            self._flog = open(self._log_path, 'a', buffering=1)
            self._fsched = open(self._log_path + '1', 'a', buffering=1)
            self._fstep = open(self._log_path + '2', 'a', buffering=1)
        pass

    def WriteLog(self, request, context) -> log_service_pb2.Empty:
        pid = request.pid
        ts = request.ts
        msg = request.msg
        if self._log_path is not None:
            self._flog.write(f'{pid}, {ts}, {msg}\n')
        print(f'{pid}, {ts}: {msg}')
        return log_service_pb2.Empty()

    def DumpSched(self, request, context) -> log_service_pb2.Empty:
        pid = request.pid
        ts = request.ts
        msg = request.msg
        if self._log_path is not None:
            self._fsched.write(f'{pid}, {ts}, {msg}\n')
        print(f'{pid}, {ts}: {msg}')
        return log_service_pb2.Empty()

    def DumpStepSched(self, request, context) -> log_service_pb2.Empty:
        pid = request.pid
        ts0 = request.ts0
        ts1 = request.ts1
        msg = request.msg
        if self._log_path is not None:
            self._fstep.write(f'{pid}, {ts0}, {ts1}, {msg}\n')
        return log_service_pb2.Empty()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    log_service_pb2_grpc.add_LogServerServicer_to_server(LogServicer('log/log_server.log'), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()


def main():
    serve()


if __name__ == '__main__':
    main()
