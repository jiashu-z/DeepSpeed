import log_service_pb2
import log_service_pb2_grpc
import grpc
from concurrent import futures


class LogServicer(log_service_pb2_grpc.LogServerServicer):
    def __init__(self):
        pass

    def WriteLog(self, request, context) -> log_service_pb2.Empty:
        pid = request.pid
        msg = request.msg
        print(f'{pid}: {msg}')
        return log_service_pb2.Empty()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    log_service_pb2_grpc.add_LogServerServicer_to_server(LogServicer(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()


def main():
    serve()


if __name__ == '__main__':
    main()
