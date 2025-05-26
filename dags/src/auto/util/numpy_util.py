import json
import numpy as np

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)  # NumPy integer'ı standart int'e çevir
        elif isinstance(obj, np.floating):
            return float(obj) # NumPy float'ı standart float'a çevir
        elif isinstance(obj, np.ndarray):
            return obj.tolist() # NumPy array'ini standart list'e çevir
        return super(NumpyEncoder, self).default(obj)