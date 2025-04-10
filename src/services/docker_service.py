from logging import DEBUG
import docker

class DockerService:
    def __init__(self):
        self.docker_client = docker.from_env()
    
    def start_worker_processor(self, body, container_name):
        try:
            transaction_id = body['transaction_id']
            file_name = body['file_name']
            tipo = str(body['tipo'])
            email = body['email']
            s3_path = body['s3_path']            
            client_id = str(body['client_id'])
            message_group = body['message_group']
            user_id = str(body['user_id'])
            send_queue = body['send_queue']
            request_origin = body['request_origin']
                                       
            container = self.docker_client.containers.run(
                image=container_name,
                name=container_name,
                detach=True,
                remove=False if DEBUG else True,
                volumes={"/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"}},
                network="net",
                mem_limit="1g",             # limita a memória a 1GB
                nano_cpus=500_000_000,      # limita a 0.5 CPU (em nanosegundos)
                environment={
                    "ENVIRONMENT": "development" if DEBUG else "production", # docker exec -it worker-processor printenv ENVIRONMENT
                    "S3_PATH": s3_path,
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
            )
            
            # Refresh container info
            container.reload()
            
            logs = container.logs()
            print(logs.decode())
            
            if container.status == "running":
                return True, f"Container {container.id[:12]} esta rodando."
            else:
                f"Container {container.id[:12]} esta rodando."
                print(f"Container {container.id[:12]} status: {container.status}")                                    
        except Exception as ex:
            msg = f"Erro ao iniciar worker-processor: {ex}"
            return False, msg
        
        
