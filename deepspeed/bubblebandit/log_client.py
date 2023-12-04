import grpc
import deepspeed.bubblebandit.log_service_pb2_grpc as log_service_pb2_grpc
import deepspeed.bubblebandit.log_service_pb2 as log_service_pb2


class LogClient:
    def __init__(self):
        self.chan = grpc.insecure_channel("localhost:50051")
        self.stub = log_service_pb2_grpc.LogServerStub(self.chan)

    def write_log(self, pid: int, ts: int, msg: str) -> None:
        log_entry = log_service_pb2.LogEntry(pid=pid, ts=ts, msg=msg)
        self.stub.WriteLog(log_entry)

    def dump_sched(self, pid: int, ts: int, msg: str) -> None:
        sched_entry = log_service_pb2.DumpSchedEntry(pid=pid, ts=ts, msg=msg)
        self.stub.DumpSched(sched_entry)

    def dump_step_sched(self, pid: int, ts0: int, ts1: int, msg: str) -> None:
        step_sched_entry = log_service_pb2.DumpStepSchedEntry(
            pid=pid, ts0=ts0, ts1=ts1, msg=msg
        )
        self.stub.DumpStepSched(step_sched_entry)

    def record_instr(self, pid: int, ts0: int, ts1: int, instr: str) -> None:
        record_instr_entry = log_service_pb2.RecordInstrEntry(
            pid=pid, ts0=ts0, ts1=ts1, instr=instr
        )
        self.stub.RecordInstr(record_instr_entry)

    def clear(self) -> None:
        self.stub.Clear(log_service_pb2.Empty())


log_client = LogClient()


def write_log_test():
    global log_client
    log_client.write_log(2, 3, "edf")


def clear_test():
    global log_client
    log_client.clear()


if __name__ == "__main__":
    # write_log_test()
    clear_test()
