import deepspeed.bubblebandit.log_service_pb2_grpc as log_service_pb2_grpc
import deepspeed.bubblebandit.log_service_pb2 as log_service_pb2
import grpc
from concurrent import futures
import os
from pathlib import Path
from io import TextIOWrapper as TextIOWrapper
import threading
import time
import torch
import torchvision
import torchvision.transforms as transforms
from torchvision.models import resnet152
import torch.multiprocessing as mp
# from multiprocessing import Process
from copy import copy, deepcopy

def _resnet152_inference(end: float, device: str, model, test_loader):
    # transform = transforms.Compose([
    #     transforms.Resize(224),
    #     transforms.Grayscale(3),
    #     transforms.ToTensor(),
    # ])
    fserve = open(f'serve_{device}.log', 'a')
    # mnist_test = torchvision.datasets.MNIST(root='./data', train=False, download=True, transform=transform)
    # test_loader = torch.utils.data.DataLoader(mnist_test, batch_size=64, shuffle=False)
    # model = resnet152(pretrained=True).to(device)
    # model.eval()
    model = model.to(device)
    counter: int = 0
    with torch.no_grad():
        for images, _ in test_loader:
            if time.time() < end:
                images = images.to(device)
                _ = model(images).to('cpu')
                counter += len(images)
            else:
                fserve.write(f"Serve {counter}, excessive {time.time() - end} sec\n")
                fserve.close()
                break


class Bubble():
    def __init__(self, start: float, end: float, stage_id: int, device: str):
        self._start: float = start
        self._end: float = end
        self._stage_id: int = stage_id
        self._device: str = str(device)
    
    def is_expired(self) -> bool:
        return time.time() > self._end
    
    def __repr__(self) -> str:
        return f'{{stage: {self._stage_id}, device: {self._device}, start: {self._start}, end: {self._end}}}'


class LogServicer(log_service_pb2_grpc.LogServerServicer):
    def _get_fd(self, file_name=str) -> TextIOWrapper:
        return open(os.path.join(self._log_dir, file_name), "w", buffering=1)

    def __init__(self, log_dir="./log", interval: int=0.1):
        self._log_dir = log_dir
        self._interval = interval

        Path(self._log_dir).mkdir(parents=True, exist_ok=True)
        self._flog = self._get_fd("misc.log")
        self._fsched = self._get_fd("schedule_dump.log")
        self._fstep = self._get_fd("step.log")
        self._fsr = self._get_fd("send_recv.log")
        self._fbb = self._get_fd("bubble.log")
        self._fserve = self._get_fd("serve.log")

        self._bubbles_lock: threading.Lock = threading.Lock()
        self._bubbles: list[Bubble] = []

        self._bubbles_cleaner = threading.Thread(target=self._clean_bubbles, args=(self._interval, self._bubbles, self._bubbles_lock))
        self._bubbles_cleaner.start()
        self._process_map = {}

        # self._transform = transforms.Compose([
        #     transforms.Resize(224),
        #     transforms.Grayscale(3),
        #     transforms.ToTensor(),
        # ])
        # self._mnist_test = torchvision.datasets.MNIST(root='./data', train=False, download=True, transform=self._transform)
        # self._test_loader = torch.utils.data.DataLoader(self._mnist_test, batch_size=64, shuffle=False)
        # self._model = resnet152(pretrained=True)
        # self._model.eval()

    def _clean_bubbles(self, interval: int, bubbles: list[Bubble], lock: threading.Lock):
        while True:
            with lock:
                for bubble in bubbles[:]:
                    if bubble.is_expired():
                        self._process_map[bubble].join()
                        self._process_map.pop(bubble)
                        bubbles.remove(bubble)
                # print(bubbles)
            time.sleep(interval)

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


    
    # def _resnet152_inference_1(self, bubble: Bubble):
    #     model = self._model.to(bubble._device)
    #     counter: int = 0
    #     with torch.no_grad():
    #         for images, _ in self._test_loader:
    #             if time.time() < bubble._end:
    #                 images = images.to(bubble._device)
    #                 _ = model(images).to('cpu')
    #                 counter += len(images)
    #             else:
    #                 with self._bubbles_lock:
    #                     self._fserve.write(f"Serve {counter}, excessive {time.time() - bubble.end} sec\n")
    #                     self._fserve.close()
    #                 break

    def _add_bubble(self, start: float, end: float, stage_id: int, device: str):
        with self._bubbles_lock:
            bubble = Bubble(start, end, stage_id, device)
            self._bubbles.append(bubble)
            
            # mp.set_start_method('spawn', force=True)
            p = mp.spawn(fn=_resnet152_inference, args=[bubble._end, bubble._device, deepcopy(self._model), deepcopy(self._test_loader)])
            # p = Process(target=self._resnet152_inference_1, args=[bubble])
            # p = mp.Process(target=LogServicer._resnet152_inference, args=[bubble._end, bubble._device])
            self._process_map[bubble] = p
            p.start()

    def GrantBubble(self, request, context) -> log_service_pb2.GrantBubbleReply:
        start: float = request.start
        end: float = request.end
        duration: float = end - start
        stage_id = request.stage_id
        device = request.device
        self._fbb.write(f"Grant {stage_id}, {device}, {start}, {end}, {duration}\n")
        self._add_bubble(start, end, stage_id, device)
        return log_service_pb2.GrantBubbleReply()

    def KillBubble(self, request, context) -> log_service_pb2.KillBubbleReply:
        start: float = request.start
        end: float = request.end
        duration: float = end - start
        stage_id = request.stage_id
        device = request.device
        self._fbb.write(f"Kill {stage_id}, {device}, {start}, {end}, {duration}\n")
        return log_service_pb2.KillBubbleReply()

def serve(stage_id: int=0):
    print(f'Start server {stage_id}')
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    log_service_pb2_grpc.add_LogServerServicer_to_server(LogServicer(f"./log"), server)
    server.add_insecure_port(f"[::]:{40051 - stage_id}")
    server.start()
    print(f'Server on {40051 - stage_id} ready!')
    server.wait_for_termination()


def main():
    serve()


if __name__ == "__main__":
    main()
