import deepspeed.bubblebandit.log_service_pb2_grpc as log_service_pb2_grpc
import deepspeed.bubblebandit.log_service_pb2 as log_service_pb2
import grpc
from concurrent import futures
import os
from pathlib import Path
from io import TextIOWrapper as TextIOWrapper


class LogServicer(log_service_pb2_grpc.LogServerServicer):
    def _get_fd(self, file_name=str) -> TextIOWrapper:
        return open(os.path.join(self._log_dir, file_name), "w", buffering=1)

    def __init__(self, log_dir="./log"):
        self._log_dir = log_dir
        Path(self._log_dir).mkdir(parents=True, exist_ok=True)
        self._flog = self._get_fd("misc.log")
        self._fsched = self._get_fd("schedule_dump.log")
        self._fstep = self._get_fd("step.log")
        self._fsr = self._get_fd("send_recv.log")

    def WriteLog(self, request, context) -> log_service_pb2.Empty:
        pid = request.pid
        ts = request.ts
        msg = request.msg
        if self._log_dir is not None:
            self._flog.write(f"{pid}, {ts}, {msg}\n")
        print(f"{pid}, {ts}: {msg}")
        return log_service_pb2.Empty()

    def DumpSched(self, request, context) -> log_service_pb2.Empty:
        pid = request.pid
        ts = request.ts
        msg = request.msg
        if self._log_dir is not None:
            self._fsched.write(f"{pid}, {ts}, {msg}\n")
        print(f"{pid}, {ts}: {msg}")
        return log_service_pb2.Empty()

    def DumpStepSched(self, request, context) -> log_service_pb2.Empty:
        pid = request.pid
        ts0 = request.ts0
        ts1 = request.ts1
        msg = request.msg
        if self._log_dir is not None:
            self._fstep.write(f"{pid}, {ts0}, {ts1}, {msg}\n")
        return log_service_pb2.Empty()

    def RecordInstr(self, request, context) -> log_service_pb2.Empty:
        pid = request.pid
        ts0 = request.ts0
        ts1 = request.ts1
        instr = request.instr
        if self._log_dir is not None:
            self._fsr.write(f"{pid}, {ts0}, {ts1}, {instr}\n")
        return log_service_pb2.Empty()

    def Clear(self, request, context) -> log_service_pb2.Empty:
        self._flog.close()
        self._fsched.close()
        self._fstep.close()
        self._fsr.close()
        self._flog = self._get_fd("misc.log")
        self._fsched = self._get_fd("schedule_dump.log")
        self._fstep = self._get_fd("step.log")
        self._fsr = self._get_fd("send_recv.log")
        return log_service_pb2.Empty()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    log_service_pb2_grpc.add_LogServerServicer_to_server(LogServicer("./log"), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()


def main():
    serve()


if __name__ == "__main__":
    main()
