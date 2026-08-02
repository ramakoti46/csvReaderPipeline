class CSVEngineError(Exception):
    pass

class FileNotFoundError(CSVEngineError):
    pass

class CorruptRowError(CSVEngineError):
    pass