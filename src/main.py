import asyncio
import json
from services.docker_service import DockerService
from services.logger_service import LoggerService
from services.aws_service import AwsService
from configs import *
from worker_processor.main import WorkerProcessor

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
            sucesso = False
            if DEBUG:
                worker_processor = WorkerProcessor()
                
                os.environ["TRANSACTION_ID"] = body['transaction_id']
                os.environ["FILE_NAME"] = body['file_name']
                os.environ["TIPO"] = str(body['tipo'])
                os.environ["EMAIL"] = body['email']
                os.environ["S3_PATH"] = body['s3_path']
                os.environ["CLIENT_ID"] = str(body['client_id'])
                os.environ["MESSAGE_GROUP"] = body['message_group']
                os.environ["USER_ID"] = str(body['user_id'])
                os.environ["SEND_QUEUE"] = str(body['send_queue'])
                os.environ["REQUEST_ORIGIN"] = body['request_origin']
                
                sucesso, msg = worker_processor.iniciar_worker()
            else:
                sucesso, msg = self.docker_service.start_worker_processor(body)
                if sucesso:
                    self.logger_service.info(msg)
                else:
                    self.logger_service.error(msg)
            
            if sucesso:
                #sucesso, msg = self.aws_service.delete_message(SQS_PROCESSAMENTO_RENOVABIO_DISPATCHER, message)
                self.logger_service.info(msg)
                self.logger_service.info(f"Finalizado mensagem: {message_id}")
            else:
                self.logger_service.info(f"Mensagem não finalizada: {message_id}. Será processada novamente.")
            self.logger_service.info("------------------------------------------")

    async def iniciar_worker(self):
        try:
            self.logger_service.info("<<<--- INÍCIO DISPATCHER --->>>")
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