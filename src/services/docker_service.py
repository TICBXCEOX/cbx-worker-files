import docker

from configs import DEBUG, ENVIRONMENT
from services.logger_service import LoggerService

class DockerService:
    def __init__(self):
        self.docker_client = docker.from_env()
        self.logger_servicex = LoggerService()
    
    def start_worker_processor(self, container_name, transaction_id, file_name, tipo, email_request, email, 
                               s3_path, client_id, message_group, user_id, send_queue, request_origin):
        try:
            self.logger_servicex.set_transaction_id(transaction_id)
            
            image=container_name
            detach=True
            remove=False
            tty=False
            stdin_open=False
            volumes={"/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"}}
            network="net"
            mem_limit="1g"
            nano_cpus=500_000_000
            environment={
                "ENVIRONMENT": ENVIRONMENT, # docker exec -it worker-processor printenv ENVIRONMENT
                "S3_PATH": s3_path,
                "EMAIL_REQUEST": email_request,
                "EMAIL": email,
                "TRANSACTION_ID": transaction_id,
                "FILE_NAME": file_name,
                "TIPO": tipo,
                "CLIENT_ID": client_id,
                "MESSAGE_GROUP": message_group,
                "USER_ID": user_id,
                "REQUEST_ORIGIN": request_origin,
                "SEND_QUEUE": send_queue
            }
                                                   
            container = self.docker_client.containers.run(
                image=image,
                # docker cria nome aleatório para o container
                detach=detach,
                remove=remove,
                tty=tty,                # to allow interaction
                stdin_open=stdin_open,  # useful for debugging
                volumes=volumes,
                network=network,
                mem_limit=mem_limit,    # limita a memória a 1GB
                nano_cpus=nano_cpus,    # limita a 0.5 CPU (em nanosegundos)
                environment=environment
            )
            
            self.logger_servicex.info(f"Container {container.id[:12]} esta rodando.")
                        
            # espera terminar
            result = container.wait()  # bloqueia até o fim

            logs = container.logs().decode()

            # remove
            container.remove()

            # verifica se terminou com sucesso
            status_code = result.get("StatusCode", -1)
            success = (status_code == 0)

            if DEBUG:
                message = (
                    f"Processor finalizado com sucesso:\n\n{logs}\n"
                    if success else
                    f"Erro no Processor (código {status_code}):\n\n{logs}\n"
                )
            else:
                message = (
                    "Processor finalizado com sucesso."
                    if success else
                    f"Erro no Processor (código {status_code})."
                )
            return success, message
        except Exception as ex:
            msg = f"Erro ao iniciar Processor: {ex}"
            return False, msg
        
        
