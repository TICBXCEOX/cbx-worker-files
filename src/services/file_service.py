import json
import os

class FileService:   
    def __init__(self):
        self = self
                
    def open_file(self, file, is_json = True):
        with open(file, 'r', encoding='utf-8-sig') as file:
            content = file.read()
            if not content:
                return False, None
            if is_json:
                content = json.loads(content)
            return True, content
                
    def create_folder(self, folder_path):
        try:
            if not os.path.exists(folder_path):
                os.makedirs(folder_path, exist_ok=True)
                return True, f'Pasta {folder_path} criada com sucesso'
            return True, f'Pasta {folder_path} já existe'
        except Exception as ex:
            msg = f'Erro ao criar pasta {folder_path}: {str(ex)}'
            return False, msg