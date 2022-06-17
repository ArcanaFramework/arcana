from arcana.data.stores.bids import Bids
from arcana.data.stores.bids.dataset import BidsDataset
from arcana.data.spaces.medimage import Clinical
from arcana.data.formats.common import Directory

dataset = BidsDataset(
    '/Users/tclose/dda6fb47-c7a7-49f5-b543-df853c323492/bids-dataset/',
    Bids(), ['session'], space=Clinical)
col = dataset.add_sink('derivatives/workflow', Directory)
item = col['sub-DEFAULT']

item.exists
item.row.dataset.store.file_group_stem_path(item)