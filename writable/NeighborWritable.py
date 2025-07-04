from io import BytesIO
import struct
from typing import Union


class NeighborWritable:
    def __init__(self):
        self.lab = -1
        self.dis = -1.0
    
    def set(self, v_: int, _dis: float):
        """Set the label and distance values"""
        self.dis = _dis
        self.lab = v_
        # assert self.lab >= 0, "Label must be >= 0"
    
    def __lt__(self, other: 'NeighborWritable') -> bool:
        """Less than comparison for sorting"""
        return self.lab < other.lab
    
    def __le__(self, other: 'NeighborWritable') -> bool:
        """Less than or equal comparison"""
        return self.lab <= other.lab
    
    def __gt__(self, other: 'NeighborWritable') -> bool:
        """Greater than comparison"""
        return self.lab > other.lab
    
    def __ge__(self, other: 'NeighborWritable') -> bool:
        """Greater than or equal comparison"""
        return self.lab >= other.lab
    
    def __eq__(self, other: 'NeighborWritable') -> bool:
        """Equality comparison"""
        if not isinstance(other, NeighborWritable):
            return False
        return self.lab == other.lab
    
    def __ne__(self, other: 'NeighborWritable') -> bool:
        """Not equal comparison"""
        return not self.__eq__(other)
    
    def compare_to(self, other: 'NeighborWritable') -> int:
        """Java-style compareTo method"""
        if self.lab > other.lab:
            return 1
        elif self.lab == other.lab:
            return 0
        else:
            return -1
    
    def __str__(self) -> str:
        """String representation"""
        return f"{self.lab} {self.dis}"
    
    def read_fields(self, data_input: Union[BytesIO, bytes]):
        """Read fields from binary data"""
        if isinstance(data_input, bytes):
            data_input = BytesIO(data_input)
        
        # Read int (4 bytes) and double (8 bytes)
        lab_bytes = data_input.read(4)
        dis_bytes = data_input.read(8)
        
        if len(lab_bytes) != 4 or len(dis_bytes) != 8:
            raise IOError("Insufficient data to read NeighborWritable")
        
        self.lab = struct.unpack('>i', lab_bytes)[0]  # Big-endian int
        self.dis = struct.unpack('>d', dis_bytes)[0]  # Big-endian double
    
    def write(self, data_output: BytesIO = None) -> bytes:
        """Write fields to binary data"""
        if data_output is None:
            data_output = BytesIO()
        
        # Write int (4 bytes) and double (8 bytes) in big-endian format
        data_output.write(struct.pack('>i', self.lab))
        data_output.write(struct.pack('>d', self.dis))
        
        if data_output.tell() == 12:  # If we created the BytesIO, return bytes
            data_output.seek(0)
            return data_output.read()
        
        return b''  # Return empty bytes if using provided BytesIO