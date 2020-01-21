import logging

class logger:
    def __init__(self,logger_name = '__name__', file_name = 'log/logs.log'):
        # Create a custom logger
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)
        # Create handlers
        # c_handler = logging.StreamHandler()
        # c_handler.setLevel(logging.DEBUG)
        f_handler = logging.FileHandler(file_name)
        f_handler.setLevel(logging.DEBUG)

        # Create formatters and add it to handlers
        # c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
        # c_handler.setFormatter(c_format)
        f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        f_handler.setFormatter(f_format)

        # Add handlers to the logger
        # self.logger.addHandler(c_handler)
        self.logger.addHandler(f_handler)


if __name__ == "__main__":

    logger = logger(file_name='testing_log').logger
    logger.debug('logger instantiated')