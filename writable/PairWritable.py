from io import BytesIO
import struct

class PairWritable:
    """
    Key type to save pairs
    
    @author ben
    """
    
    ####################################
    # class constants
    ####################################
    
    PRIME = 1000003
    
    ####################################
    # constructor and methods
    ####################################
    
    def __init__(self):
        """Constructor"""
        self.left = 0
        self.right = 0
    
    def set(self, left, right=None):
        """
        Set fields
        Args:
            left: left element (int or string)
            right: right element (int or string, optional)
        """
        if right is None:
            # Assume left is a string with two values
            raise ValueError("Both left and right parameters are required")
        
        if isinstance(left, str) and isinstance(right, str):
            u = int(left)
            v = int(right)
            self.left = max(u, v)
            self.right = min(u, v)
            assert self.left > self.right, "Left should be greater than right"
        else:
            self.left = max(left, right)
            self.right = min(left, right)
    
    def read_fields(self, data_input):
        """
        Read fields from DataInput equivalent
        Args:
            data_input: input stream or bytes
        """
        if isinstance(data_input, bytes):
            # Read two integers from bytes (assuming big-endian format)
            self.left = struct.unpack('>i', data_input[0:4])[0]
            self.right = struct.unpack('>i', data_input[4:8])[0]
        else:
            # Assume data_input has read methods
            self.left = data_input.read_int()
            self.right = data_input.read_int()
    
    def write(self, data_output):
        """
        Write fields to DataOutput equivalent
        Args:
            data_output: output stream
        """
        if hasattr(data_output, 'write_int'):
            data_output.write_int(self.left)
            data_output.write_int(self.right)
        else:
            # Write as bytes (big-endian format)
            data_output.write(struct.pack('>i', self.left))
            data_output.write(struct.pack('>i', self.right))
    
    def compare_to(self, target):
        """
        Compare to another PairWritable object
        Args:
            target: PairWritable object to compare with
        Returns:
            int: negative if less than, 0 if equal, positive if greater than
        """
        cmp = self.left - target.left
        if cmp != 0:
            return cmp
        else:
            return self.right - target.right
    
    def __hash__(self):
        """Hash code implementation"""
        return hash(str(self))
    
    def __str__(self):
        """String representation"""
        return f"{self.left} {self.right}"
    
    def __eq__(self, other):
        """Equality comparison"""
        if not isinstance(other, PairWritable):
            return False
        return self.left == other.left and self.right == other.right
    
    def __lt__(self, other):
        """Less than comparison for sorting"""
        return self.compare_to(other) < 0
    
    def __le__(self, other):
        """Less than or equal comparison"""
        return self.compare_to(other) <= 0
    
    def __gt__(self, other):
        """Greater than comparison"""
        return self.compare_to(other) > 0
    
    def __ge__(self, other):
        """Greater than or equal comparison"""
        return self.compare_to(other) >= 0