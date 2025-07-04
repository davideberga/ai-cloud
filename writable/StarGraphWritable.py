"""
StarGraphWritable class - Python translation from Java

@author ben
"""

from typing import List, BinaryIO
import struct
from writable.NeighborWritable import NeighborWritable  # Assuming this class is implemented elsewhere


class StarGraphWritable:
    """
    StarGraphWritable implementation equivalent to the Java version.
    Assumes NeighborWritable class is implemented elsewhere.
    """
    
    def __init__(self):
        """Initialize StarGraphWritable with default values"""
        self.center: int = 0  # center of stargraph
        self.deg_center: int = 0  # degree of center node, needed for loading from file
        self.neighbors: List['NeighborWritable'] = []
    
    def set(self, center: int, deg_center: int, neighbors: List['NeighborWritable']) -> None:
        """
        Set the values for the StarGraphWritable
        
        Args:
            center: center node id
            deg_center: degree of center node
            neighbors: list of NeighborWritable objects
        """
        self.center = center
        self.deg_center = deg_center
        self.neighbors = neighbors
        
        # Assert equivalent to Java's Assert.assertTrue
        assert len(self.neighbors) == self.deg_center, "Mismatched Size"
    
    def read_fields(self, data_input: BinaryIO) -> None:
        """
        Read fields from binary input stream
        
        Args:
            data_input: binary input stream
        """
        # Read center as 4-byte integer
        center_bytes = data_input.read(4)
        if len(center_bytes) != 4:
            raise IOError("Cannot read center field")
        self.center = struct.unpack('>i', center_bytes)[0]
        
        # Read degCenter as 4-byte integer
        deg_center_bytes = data_input.read(4)
        if len(deg_center_bytes) != 4:
            raise IOError("Cannot read degCenter field")
        self.deg_center = struct.unpack('>i', deg_center_bytes)[0]
        
        # Initialize neighbors list
        self.neighbors = []
        
        # Read each neighbor
        for i in range(self.deg_center):
            # Assuming NeighborWritable is imported from another module
            neighbor = NeighborWritable()
            neighbor.read_fields(data_input)
            self.neighbors.append(neighbor)
    
    def write(self, data_output: BinaryIO) -> None:
        """
        Write fields to binary output stream
        
        Args:
            data_output: binary output stream
        """
        # Write center as 4-byte integer (big-endian)
        data_output.write(struct.pack('>i', self.center))
        
        # Write degCenter as 4-byte integer (big-endian)
        data_output.write(struct.pack('>i', self.deg_center))
        
        # Write each neighbor
        for i in range(self.deg_center):
            neighbor = self.neighbors[i]
            neighbor.write(data_output)
    
    def __hash__(self) -> int:
        """
        Hash function - currently returns default object hash
        TODO: Implement proper hash function
        """
        return object.__hash__(self)
    
    def __str__(self) -> str:
        """
        String representation of the StarGraphWritable
        
        Returns:
            String representation in format: "center degCenter neighbor1,neighbor2,..."
        """
        neighbor_strings = []
        for neighbor in self.neighbors:
            neighbor_strings.append("," + str(neighbor))
        
        neighbor_str = "".join(neighbor_strings)
        return f"{self.center} {self.deg_center} {neighbor_str}"