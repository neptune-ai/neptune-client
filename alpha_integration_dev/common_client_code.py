from datetime import datetime
import sys
import abc

import neptune
from neptune import alpha as alpha_neptune


class ClientFeatures(abc.ABC):
    params = {
        'text_parameter': 'some text',
        'number parameter': 42,
        'list': [1, 2, 3],
        'datetime': datetime.now()
    }

    img_path = 'alpha_integration_dev/data/g.png'

    @abc.abstractmethod
    def modify_tags(self):
        """NPT-9213"""

    @abc.abstractmethod
    def log_std(self):
        """system streams / monitoring logs"""

    @abc.abstractmethod
    def log_series(self):
        pass

    @abc.abstractmethod
    def handle_files_and_images(self):
        """NPT-9207"""

    @abc.abstractmethod
    def other(self):
        pass

    @abc.abstractmethod
    def run(self):
        pass

    def run(self):
        self.modify_tags()
        self.log_std()
        self.log_series()
        self.other()
        self.handle_files_and_images()
