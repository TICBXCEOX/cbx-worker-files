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
            
            transaction_id = body['transaction_id']
            file_name = body['file_name']
            tipo = str(body['tipo'])
            email_request = body['email_request'] if 'email_request' in body else ''
            email = body['email']
            s3_path = body['s3_path']            
            client_id = str(body['client_id'])
            message_group = body['message_group']
            user_id = str(body['user_id'])
            send_queue = body['send_queue']
            request_origin = body['request_origin']                       
                            
            self.logger_service.set_transaction_id(transaction_id)
            self.logger_service.info(f"file_name: {file_name} - {type(file_name)}")
            self.logger_service.info(f"tipo: {tipo} - {type(tipo)}")
            if email_request:
                self.logger_service.info(f"email solicitacao ROBO: {email_request} - {type(email_request)}")
            self.logger_service.info(f"email: {email} - {type(email)}")
            self.logger_service.info(f"s3_path: {s3_path} - {type(s3_path)}")
            self.logger_service.info(f"client_id: {client_id} - {type(client_id)}")
            self.logger_service.info(f"message_group: {message_group} - {type(message_group)}")
            self.logger_service.info(f"user_id: {user_id} - {type(user_id)}")
            self.logger_service.info(f"send_queue: {send_queue} - {type(send_queue)}")
            self.logger_service.info(f"request_origin: {request_origin} - {type(request_origin)}")
                
            #worker-processor-worker_processor
            container_name = 'worker-processor-worker_processor'
            if DEBUG:
                container_name = 'cbx-worker-processor-worker_processor'
            self.logger_service.info(f"Container name: {container_name}")
            
            sucesso, msg = self.docker_service.start_worker_processor(container_name, transaction_id, file_name, tipo, \
                email_request, email, s3_path, client_id, message_group, user_id, send_queue, request_origin) 
            if sucesso:
                self.logger_service.info(msg)
            else:
                self.logger_service.error(msg)
            
            if sucesso:
                sucesso, msg = self.aws_service.delete_message(SQS_PROCESSAMENTO_RENOVABIO_DISPATCHER, message)
                self.logger_service.info(msg)
                self.logger_service.info(f"Finalizado mensagem: {message_id}")
            else:
                self.logger_service.info(f"Mensagem não finalizada: {message_id}. Será processada novamente.")

    async def iniciar_worker(self):        
        try:
            self.logger_service.clear_transaction_id()
            self.logger_service.info("<<<--- INÍCIO DISPATCHER --->>>")            
            self.logger_service.info("ENVIRONMENT: " + ENVIRONMENT)
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