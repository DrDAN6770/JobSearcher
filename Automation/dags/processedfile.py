import os

class processedfile():
    def __init__(self) -> None:
        self.directory = 'output'
        self.file_path = 'output/processed_files.txt'

    def find_csv_files(self):
        return [filename for filename in os.listdir(self.directory) if filename.endswith(".csv")]

    def load_processed_files(self) -> set():
        if os.path.exists(self.file_path):
            with open(self.file_path, 'r') as file:
                return set(file.read().splitlines())
        else:
            return set()
    
    def filiter(self) -> list:
        res = []
        csv_files = self.find_csv_files()
        visited = self.load_processed_files()
        for filename in csv_files:
            if filename not in visited:
                res.append(filename)
        return res

def save_processed_file(filename, file_path='output/processed_files.txt') -> None:
    with open(file_path, 'a') as file:
        file.write(filename + '\n')

def processedfile_main() -> list:
    obj = processedfile()
    needTodo = obj.filiter()
    return needTodo