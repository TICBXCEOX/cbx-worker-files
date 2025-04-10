import asyncio
import json
from configs import *
from services.docker_service import DockerService
from services.logger_service import LoggerService
from services.aws_service import AwsService

class WorkerDispatcher:
    def __init__(self):
        self.aws_service = AwsService()
        self.docker_service = DockerService()
        self.logger_service = LoggerService()

    def consume_queue(self):
        # lê a fila de mensagens da entrega arquivo do zip pela API
        sucesso, messages = self.aws_service.consume_message(SQS_PROCESSAMENTO_RENOVABIO_DISPATCHER)        
        if not sucesso:
            self.logger_service.warn(messages)
            return
        
        for message in messages:            
            message_id = message["MessageId"]            
            body = json.loads(message['Body'])
            
            self.logger_service.info(f"Início mensagem {message_id}")
            
            # processa o zip
            sucesso = True
            if DEBUG:
                print(f"transaction_id = '{body['transaction_id']}'")
                print(f"file_name = '{body['file_name']}'")
                print(f"tipo = {str(body['tipo'])}")
                print(f"email = '{body['email']}'")
                print(f"s3_path = '{body['s3_path']}'")
                print(f"client_id = {str(body['client_id'])}")
                print(f"message_group = '{body['message_group']}'")
                print(f"user_id = {str(body['user_id'])}")                
                print(f"send_queue = {str(body['send_queue'])}")
                print(f"request_origin = '{body['request_origin']}'")
            else:
                #worker-processor-worker_processor
                container_name = 'worker-processor-worker_processor'
                if DEBUG:
                    container_name = 'cbx-worker-processor-worker_processor'
                self.logger_service.info(f"Container name: {container_name}")
                self.logger_service.info(f"ENVIRONMENT: {os.getenv('ENVIRONMENT')}")
                
                sucesso, msg = self.docker_service.start_worker_processor(body, container_name)
                if sucesso:
                    self.logger_service.info(msg)
                else:
                    self.logger_service.error(msg)
            
            if sucesso:
                sucesso, msg = self.aws_service.delete_message(SQS_PROCESSAMENTO_RENOVABIO_DISPATCHER, message)
                self.logger_service.info(f"Finalizado mensagem: {message_id}")
            else:
                self.logger_service.info(f"Mensagem não finalizada: {message_id}. Será processada novamente.")
            self.logger_service.info("------------------------------------------")

    async def iniciar_worker(self):        
        try:
            self.logger_service.info("<<<--- INÍCIO DISPATCHER --->>>")
            self.logger_service.info("Modo DEBUG: " + str(DEBUG))
            while True:
                self.consume_queue()
                await asyncio.sleep(5)
        except Exception as ex:
            self.logger_service.error(f"ERROR: {str(ex)}")
        finally:
            self.logger_service.info("<<<--- DISPATCHER FINALIZADO --->>>")

if __name__ == "__main__":
    worker = WorkerDispatcher()
    asyncio.run(worker.iniciar_worker())