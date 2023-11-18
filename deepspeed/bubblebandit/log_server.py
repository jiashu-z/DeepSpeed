import deepspeed.bubblebandit.log_service_pb2_grpc as log_service_pb2_grpc
import deepspeed.bubblebandit.log_service_pb2 as log_service_pb2
# from . import log_service_pb2_grpc
# from . import log_service_pb2
import grpc
from concurrent import futures


class LogServicer(log_service_pb2_grpc.LogServerServicer):
    def __init__(self, log_path=None):
        self._log_path_ = log_path
        if self._log_path_ is not None:
            self.flog_ = open(self._log_path_, 'a', buffering=1)
        pass

    def WriteLog(self, request, context) -> log_service_pb2.Empty:
        pid = request.pid
        ts = request.ts
        msg = request.msg
        if self._log_path_ is not None:
            self.flog_.write(f'pid: {pid}, ts: {ts}, msg: {msg}\n')
        print(f'{pid}, {ts}: {msg}')
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
