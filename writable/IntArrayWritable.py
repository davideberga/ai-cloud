from typing import List, Optional
import struct
from io import BytesIO

class IntArrayWritable:
    """
    This class is for both degree and neighbors.
    """
    
    def __init__(self):
        self.neighbors: Optional[List[int]] = None
        self.no_neighbors: int = 0
    
    def set(self, no_neighbors: int, neighbors: Optional[List[int]]):
        """Set the number of neighbors and the neighbors list."""
        self.no_neighbors = no_neighbors
        self.neighbors = neighbors
        if self.neighbors is not None:
            assert self.no_neighbors == len(self.neighbors), \
                f"no_neighbors ({self.no_neighbors}) must equal neighbors list size ({len(self.neighbors)})"
    
    def read_fields(self, data_input):
        """
        Read from DataInput equivalent.
        We only read star graph. we will never read <u> <deg(u)> with this function.
        """
        # Assuming data_input has a read_int() method similar to Java's DataInput
        self.no_neighbors = data_input.read_int()
        self.neighbors = []
        for i in range(self.no_neighbors):
            self.neighbors.append(data_input.read_int())
    
    def write(self, data_output):
        """Write to DataOutput equivalent."""
        if self.neighbors is None:
            # This is only saving degree
            # data_output.write_int(self.no_neighbors)
            pass
        else:
            data_output.write_int(self.no_neighbors)
            for i in range(self.no_neighbors):
                data_output.write_int(self.neighbors[i])
    
    def __str__(self) -> str:
        """String representation of the object."""
        if self.neighbors is None:
            return str(self.no_neighbors)
        else:
            result = f"{self.no_neighbors}:"
            for neighbor in self.neighbors:
                result += f",{neighbor}"
            return result


# Alternative implementation using bytes for serialization
class IntArrayWritableBytes(IntArrayWritable):
    """
    Alternative implementation that works directly with bytes,
    similar to how Hadoop's Writable interface works with binary data.
    """
    
    def read_fields_from_bytes(self, data: bytes):
        """Read from bytes data."""
        bio = BytesIO(data)
        self.no_neighbors = struct.unpack('>i', bio.read(4))[0]  # big-endian int
        self.neighbors = []
        for i in range(self.no_neighbors):
            neighbor = struct.unpack('>i', bio.read(4))[0]
            self.neighbors.append(neighbor)
    
    def write_to_bytes(self) -> bytes:
        """Write to bytes."""
        if self.neighbors is None:
            return b''
        else:
            bio = BytesIO()
            bio.write(struct.pack('>i', self.no_neighbors))  # big-endian int
            for neighbor in self.neighbors:
                bio.write(struct.pack('>i', neighbor))
            return bio.getvalue()