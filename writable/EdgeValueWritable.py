"""
This class is used to save output of Each DynamicDistance Looping!!!

@author ben
"""

class EdgeValueWritable:
    """
    Python equivalent of the Java EdgeValueWritable class that implements Writable interface.
    Assumes that Writable functionality and serialization methods are implemented elsewhere.
    """
    
    ####################################
    # Class constants
    ####################################
    PRIME = 1000003
    
    def __init__(self):
        """Initialize EdgeValueWritable object"""
        ####################################
        # Public fields
        ####################################
        # This does not mean edge weight!!!!
        self.edge_value = 0.0
        self.additional = ''
    
    def set(self, edge_value, additional):
        """
        Set fields
        
        Args:
            edge_value (float): The edge value
            additional (str): Additional character information
        """
        self.edge_value = edge_value
        self.additional = additional
    
    def read_fields(self, data_input):
        """
        Read fields from DataInput stream
        Equivalent to Java's readFields method
        
        Args:
            data_input: Input stream object with read_double() and read_char() methods
        """
        self.edge_value = data_input.read_double()
        self.additional = data_input.read_char()
    
    def write(self, data_output):
        """
        Write fields to DataOutput stream
        Equivalent to Java's write method
        
        Args:
            data_output: Output stream object with write_double() and write_char() methods
        """
        data_output.write_double(self.edge_value)
        data_output.write_char(self.additional)
    
    def __hash__(self):
        """
        Override hash method
        
        Returns:
            int: Hash code of the string representation
        """
        return hash(str(self))
    
    def __str__(self):
        """
        String representation of the object
        
        Returns:
            str: String representation in format "edge_value additional"
        """
        return f"{self.edge_value} {self.additional}"
    
    def __repr__(self):
        """
        Developer-friendly string representation
        
        Returns:
            str: Detailed string representation
        """
        return f"EdgeValueWritable(edge_value={self.edge_value}, additional='{self.additional}')"
    
    def __eq__(self, other):
        """
        Equality comparison
        
        Args:
            other (EdgeValueWritable): Another EdgeValueWritable object
            
        Returns:
            bool: True if objects are equal
        """
        if not isinstance(other, EdgeValueWritable):
            return False
        return (self.edge_value == other.edge_value and 
                self.additional == other.additional)