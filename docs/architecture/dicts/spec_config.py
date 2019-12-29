

import radical.utils as ru
import spec_attribs  as a


class CFG(ru.Config):

    def validate(self):
        pass

    def __init__(self, from_dict=None):

        ru.Config.__init__(self, from_dict)

        if from_dict:
            for k, v in from_dict.items:
                self[k] = v

