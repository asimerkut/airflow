import json


class CopyUtil:
    max_depth = 20

    def __init__(self):
        pass

    def to_dictionary(self, obj):
        dd = self.copy_depth(obj=obj, curr_depth=0)
        return dd

    def copy_depth(self, obj, curr_depth):
        if curr_depth > self.max_depth:
            return None

        if obj is None:
            result = None
        elif hasattr(obj, '__dict__'):
            result = dict()
            for k, item in obj.__dict__.items():
                if k in ["comp_service", "conn_map"]:
                    continue
                result[k] = self.copy_depth(item, curr_depth + 1)
        elif isinstance(obj, list):
            result = []
            for item in obj:
                result.append(self.copy_depth(item, curr_depth + 1))
        elif isinstance(obj, set):
            result = []
            for item in obj:
                result.append(self.copy_depth(item, curr_depth + 1))
        elif isinstance(obj, dict):
            result = dict()
            for k, item in obj.items():
                result[k] = self.copy_depth(item, curr_depth + 1)
        elif self.is_serializable(obj):
            result = obj
        else:
            try:
                if obj.dtype.name.startswith("int"):
                    result = int(obj)
                elif obj.dtype.name.startswith("float"):
                    result = float(obj)
                elif obj.dtype.name.startswith("bool"):
                    result = bool(obj)
                else:
                    result = str(obj)
            except Exception as e:
                print("Error: ", e)
                result = None
            pass
        return result

    def is_serializable(self, obj):
        try:
            json.dumps(obj)
            return True
        except (TypeError, OverflowError):
            return False
