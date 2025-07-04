from typing import Any
import struct
from io import BytesIO

class JaccardEdgeWritable:
    """
    Key type to save triples
    
    Author: ben
    """
    
    # Private constants
    _PRIME = 1000003
    
    def __init__(self):
        """Constructor"""
        self.u = 0
        self.v = 0
        self.dis = 0.0
    
    def set(self, u: int, v: int, dis: float) -> None:
        """
        Set the values for u, v, and dis
        
        Args:
            u: First vertex
            v: Second vertex  
            dis: Distance/similarity value
        """
        try:
            self.u = u
            self.v = v
            self.dis = dis
        except Exception as e:
            print(f"Error in set method: {e}")
    
    def read_fields(self, data_input) -> None:
        """
        Read fields from DataInput equivalent
        
        Args:
            data_input: Input stream to read from
        """
        self.u = self._read_int(data_input)
        self.v = self._read_int(data_input)
        self.dis = self._read_double(data_input)
    
    def write(self, data_output) -> None:
        """
        Write fields to DataOutput equivalent
        
        Args:
            data_output: Output stream to write to
        """
        self._write_int(data_output, self.u)
        self._write_int(data_output, self.v)
        self._write_double(data_output, self.dis)
    
    def compare_to(self, target: 'JaccardEdgeWritable') -> int:
        """
        Compare this object with another JaccardEdgeWritable
        
        Args:
            target: The other JaccardEdgeWritable to compare with
            
        Returns:
            int: Negative if this < target, 0 if equal, positive if this > target
        """
        cmp = self.u - target.u
        if cmp != 0:
            return cmp
        cmp = self.v - target.v
        return cmp
    
    def __hash__(self) -> int:
        """
        Hash code implementation
        
        Returns:
            int: Hash value
        """
        return self.u * self._PRIME + self.v
    
    def __str__(self) -> str:
        """
        String representation
        
        Returns:
            str: String representation of the object
        """
        return f"{self.u} {self.v} {self.dis} G"
    
    def __eq__(self, other: Any) -> bool:
        """
        Equality comparison
        
        Args:
            other: Object to compare with
            
        Returns:
            bool: True if objects are equal
        """
        if not isinstance(other, JaccardEdgeWritable):
            return False
        return self.u == other.u and self.v == other.v and self.dis == other.dis
    
    def __lt__(self, other: 'JaccardEdgeWritable') -> bool:
        """
        Less than comparison
        
        Args:
            other: Object to compare with
            
        Returns:
            bool: True if this object is less than other
        """
        return self.compare_to(other) < 0
    
    def __le__(self, other: 'JaccardEdgeWritable') -> bool:
        """Less than or equal comparison"""
        return self.compare_to(other) <= 0
    
    def __gt__(self, other: 'JaccardEdgeWritable') -> bool:
        """Greater than comparison"""
        return self.compare_to(other) > 0
    
    def __ge__(self, other: 'JaccardEdgeWritable') -> bool:
        """Greater than or equal comparison"""
        return self.compare_to(other) >= 0
    
    # Helper methods for binary I/O (assuming big-endian format like Java)
    def _read_int(self, data_input) -> int:
        """Helper method to read an integer from input stream"""
        if hasattr(data_input, 'read'):
            bytes_data = data_input.read(4)
            return struct.unpack('>i', bytes_data)[0]  # Big-endian int
        else:
            # Assume it's a method that returns int directly
            return data_input.read_int()
    
    def _read_double(self, data_input) -> float:
        """Helper method to read a double from input stream"""
        if hasattr(data_input, 'read'):
            bytes_data = data_input.read(8)
            return struct.unpack('>d', bytes_data)[0]  # Big-endian double
        else:
            # Assume it's a method that returns double directly
            return data_input.read_double()
    
    def _write_int(self, data_output, value: int) -> None:
        """Helper method to write an integer to output stream"""
        if hasattr(data_output, 'write'):
            data_output.write(struct.pack('>i', value))  # Big-endian int
        else:
            # Assume it's a method that accepts int directly
            data_output.write_int(value)
    
    def _write_double(self, data_output, value: float) -> None:
        """Helper method to write a double to output stream"""
        if hasattr(data_output, 'write'):
            data_output.write(struct.pack('>d', value))  # Big-endian double
        else:
            # Assume it's a method that accepts double directly
            data_output.write_double(value)